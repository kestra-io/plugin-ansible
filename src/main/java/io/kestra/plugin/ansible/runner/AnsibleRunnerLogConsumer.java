package io.kestra.plugin.ansible.runner;

import io.kestra.core.models.tasks.runners.DefaultLogConsumer;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.ansible.runner.models.LogsMode;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.List;

/**
 * Intercepts process output line-by-line and forwards it to the Kestra execution logger
 * in a way that stays friendly to the UI and the log repository at very large volumes:
 * <ul>
 *   <li><b>Batching</b>: lines are grouped into a single log entry (up to {@code batchSize} lines or
 *       {@code flushIntervalMs}) so 100k lines become ~1k log rows instead of 100k.</li>
 *   <li><b>Cap</b>: at most {@code maxLogLines} lines are streamed; afterwards only critical lines
 *       (failures, unreachable, errors, recap) pass through, up to {@code CRITICAL_OVERFLOW_LIMIT}.</li>
 *   <li><b>Disk spooling</b>: the complete output is always written to a file on local disk (never kept in
 *       memory), so it can be uploaded as {@code outputLogFile} without any loss.</li>
 *   <li>Raw event JSON dumps are always filtered out (Tri-Layer architecture).</li>
 * </ul>
 */
public class AnsibleRunnerLogConsumer extends DefaultLogConsumer {

    private static final int CRITICAL_OVERFLOW_LIMIT = 500;

    private final RunContext runContext;
    private final boolean streamLogs;
    private final LogsMode logsMode;
    private final long maxLogLines;
    private final int batchSize;
    private final Path spoolFile;
    private final List<String> secrets;
    private BufferedWriter spoolWriter;
    private final ScheduledExecutorService flusher;

    private final StringBuilder batch = new StringBuilder();
    private int batchLines = 0;
    private boolean batchIsStdErr = false;

    private long streamedLines = 0;
    private long totalLines = 0;
    private long suppressedLines = 0;
    private long criticalOverflow = 0;
    private boolean truncationWarned = false;
    private boolean closed = false;

    public AnsibleRunnerLogConsumer(RunContext runContext, boolean streamLogs, LogsMode logsMode, Path spoolFile,
                                    long maxLogLines, int batchSize, java.util.List<String> secrets) {
        super(runContext);
        this.runContext = runContext;
        this.streamLogs = streamLogs;
        this.logsMode = logsMode != null ? logsMode : LogsMode.FULL;
        this.maxLogLines = maxLogLines;
        this.batchSize = Math.max(1, batchSize);
        this.spoolFile = spoolFile;
        this.secrets = secrets != null ? secrets : java.util.Collections.emptyList();

        try {
            this.spoolWriter = spoolFile != null
                ? Files.newBufferedWriter(spoolFile, StandardCharsets.UTF_8)
                : null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (streamLogs && this.batchSize > 1) {
            // Make sure slow/idle output is still delivered live (every second).
            this.flusher = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "ansible-runner-log-flusher");
                t.setDaemon(true);
                return t;
            });
            this.flusher.scheduleWithFixedDelay(this::flushQuietly, 1, 1, TimeUnit.SECONDS);
        } else {
            this.flusher = null;
        }
    }

    @Override
    public synchronized void accept(String line, Boolean isStdErr, Instant instant) {
        try { super.accept(line, isStdErr, instant); } catch (Exception ignored) {}
        if (line == null || line.isBlank()) {
            return;
        }

        for (String secret : secrets) {
            if (secret != null && !secret.isBlank()) {
                line = line.replace(secret, "***");
            }
        }

        // Always spool the complete raw output to disk (bounded memory)
        writeToSpool(line);
        totalLines++;

        // Filter out raw event JSON telemetry dumps to satisfy the Tri-Layer Architecture requirement
        String trimmed = line.trim();
        if ((trimmed.startsWith("{") && trimmed.endsWith("}")) &&
            (trimmed.contains("\"event\"") || trimmed.contains("\"uuid\"") || trimmed.contains("\"counter\""))) {
            return;
        }

        if (!streamLogs) {
            return;
        }

        boolean stderr = Boolean.TRUE.equals(isStdErr);
        boolean critical = isCriticalLine(trimmed, stderr);

        if (logsMode == LogsMode.SUMMARY && !isSummaryLine(trimmed, stderr)) {
            suppressedLines++;
            return;
        }

        if (maxLogLines > 0 && streamedLines >= maxLogLines) {
            if (!critical || criticalOverflow >= CRITICAL_OVERFLOW_LIMIT) {
                suppressedLines++;
                warnTruncatedOnce();
                return;
            }
            criticalOverflow++;
        }

        streamedLines++;
        append(line, stderr);
    }

    private void append(String line, boolean stderr) {
        // Keep a single level per entry: flush when stdout/stderr switches
        if (batchLines > 0 && batchIsStdErr != stderr) {
            flush();
        }
        batchIsStdErr = stderr;
        if (batchLines > 0) {
            batch.append('\n');
        }
        // Strip Windows-style carriage returns emitted by ansible-runner
        batch.append(line.endsWith("\r") ? line.substring(0, line.length() - 1) : line);
        batchLines++;

        if (batchLines >= batchSize) {
            flush();
        }
    }

    private void warnTruncatedOnce() {
        if (truncationWarned) {
            return;
        }
        truncationWarned = true;
        flush();
        runContext.logger().warn(
            "Log streaming limit reached ({} lines): further output is not streamed to the Kestra UI " +
            "to keep it responsive (failures, unreachable hosts and the recap are still shown). " +
            "The complete log is stored in 'outputLogFile'. Raise 'maxLogLines' (0 = unlimited) or use logsMode: SUMMARY.",
            maxLogLines
        );
    }

    /** Flush any buffered lines as a single log entry. */
    public synchronized void flush() {
        if (batchLines == 0) {
            return;
        }
        String message = batch.toString();
        batch.setLength(0);
        batchLines = 0;
        if (batchIsStdErr) {
            runContext.logger().warn(message);
        } else {
            runContext.logger().info(message);
        }
    }

    private synchronized void flushQuietly() {
        try {
            flush();
        } catch (Exception ignored) {
            // never let the background flusher die
        }
    }

    /** Flush pending lines, log a final truncation note and release resources. Safe to call multiple times. */
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (flusher != null) {
            flusher.shutdownNow();
        }
        flush();
        if (suppressedLines > 0 && streamLogs) {
            runContext.logger().info(
                "Log streaming summary: {} total lines, {} streamed, {} not streamed to the UI.",
                totalLines, streamedLines, suppressedLines
            );
        }
        try {
            if (spoolWriter != null) {
                spoolWriter.close();
            }
        } catch (IOException ignored) {
            // best effort
        }
    }

    public boolean wasTruncated() {
        return truncationWarned;
    }

    public long getTotalLines() {
        return totalLines;
    }

    private void writeToSpool(String line) {
        if (spoolWriter == null) {
            return;
        }
        try {
            spoolWriter.write(line.endsWith("\r") ? line.substring(0, line.length() - 1) : line);
            spoolWriter.newLine();
        } catch (IOException e) {
            runContext.logger().warn("Failed to write to spool log, disabling spooling: {}", e.getMessage());
            try { spoolWriter.close(); } catch (Exception ignored) {}
            spoolWriter = null;
        }
    }

    private boolean isCriticalLine(String trimmed, boolean stderr) {
        return stderr ||
               trimmed.startsWith("fatal:") ||
               trimmed.startsWith("failed:") ||
               trimmed.startsWith("unreachable:") ||
               trimmed.startsWith("ERROR!") ||
               trimmed.startsWith("PLAY RECAP") ||
               trimmed.contains(" : ok=");
    }

    private boolean isSummaryLine(String trimmed, boolean stderr) {
        if (stderr) {
            return true;
        }
        // 'ok:' and 'skipping:' are per-host-per-item lines that dominate large runs; they are
        // intentionally excluded here (counts are available in the recap and in the stats output).
        return trimmed.startsWith("PLAY [") ||
               trimmed.startsWith("TASK [") ||
               trimmed.startsWith("PLAY RECAP") ||
               trimmed.startsWith("changed:") ||
               trimmed.startsWith("failed:") ||
               trimmed.startsWith("fatal:") ||
               trimmed.startsWith("unreachable:") ||
               trimmed.startsWith("ERROR!") ||
               trimmed.startsWith("WARNING:") ||
               trimmed.contains(" : ok=") ||
               trimmed.startsWith("ansible-playbook");
    }

    public Path getSpoolFile() {
        return spoolFile;
    }

    public InputStream getLogInputStream() {
        if (spoolFile == null || !Files.exists(spoolFile)) {
            return null;
        }
        try {
            return Files.newInputStream(spoolFile);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
