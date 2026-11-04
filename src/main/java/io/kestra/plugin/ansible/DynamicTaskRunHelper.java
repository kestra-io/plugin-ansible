package io.kestra.plugin.ansible;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.event.Level;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.executions.TaskRunAttempt;
import io.kestra.core.models.flows.State;
import io.kestra.core.runners.DynamicTaskRunLog;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.WorkerTaskResult;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;

/**
 * Shared helper for emitting per-task dynamic {@link TaskRun}s and bounded per-host
 * {@link DynamicTaskRunLog}s across {@code AnsibleCLI} and {@code AnsibleRunner}.
 */
public final class DynamicTaskRunHelper {
    public static final int MAX_LOG_LINE_LENGTH = 4_000;
    public static final int STDERR_TAIL_LENGTH = 1_024;
    public static final int MAX_MSG_LOG_LENGTH = 2_000;
    public static final int MAX_ITEM_LABEL_LENGTH = 256;
    public static final int MAX_FAILED_ITEM_LOGS = 50;

    private DynamicTaskRunHelper() {
    }

    public record TaskEntry(
        String uid,
        Instant startedAt,
        Instant endedAt,
        boolean failed,
        List<DynamicTaskRunLog> logs) {
    }

    /**
     * Create one dynamic {@link TaskRun} per Ansible task and emit that task's host-result lines as logs
     * tagged with the taskrun's id, so logs are attributed per Ansible task instead of all landing
     * on the parent task's root taskrun.
     */
    public static void emit(RunContext runContext, List<TaskEntry> tasks) throws IllegalVariableEvaluationException {
        if (tasks == null || tasks.isEmpty()) {
            return;
        }

        var validTasks = tasks.stream()
            .filter(t -> t.uid() != null && t.startedAt() != null && t.endedAt() != null)
            .toList();
        if (validTasks.isEmpty()) {
            return;
        }

        var tenantId = runContext.flowInfo().tenantId();
        var namespace = runContext.render("{{ flow.namespace }}");
        var flowId = runContext.render("{{ flow.id }}");
        var executionId = runContext.render("{{ execution.id }}");
        var parentTaskRunId = runContext.render("{{ taskrun.id }}");

        for (var task : validTasks) {
            var finalType = task.failed() ? State.Type.FAILED : State.Type.SUCCESS;
            var state = State.of(
                finalType, new ArrayList<>(
                    List.of(
                        new State.History(State.Type.CREATED, task.startedAt()),
                        new State.History(State.Type.RUNNING, task.startedAt()),
                        new State.History(finalType, task.endedAt())
                    )
                )
            );

            var subTaskRun = TaskRun.builder()
                .id(IdUtils.create())
                .tenantId(tenantId)
                .namespace(namespace)
                .flowId(flowId)
                .taskId(task.uid())
                .executionId(executionId)
                .parentTaskRunId(parentTaskRunId)
                .state(state)
                .attempts(
                    List.of(
                        TaskRunAttempt.builder()
                            .state(state)
                            .build()
                    )
                )
                .build();

            runContext.dynamicWorkerResult(
                WorkerTaskResult.builder().taskRun(subTaskRun).build(),
                task.logs() != null ? task.logs() : List.of()
            );
        }
    }

    public static Instant parseInstant(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        var s = value.trim();
        try {
            return Instant.parse(s);
        } catch (Exception ignored) {
        }
        try {
            if (!s.endsWith("Z") && !s.contains("+") && !s.matches(".*-\\d{2}:\\d{2}$")) {
                if (s.contains(" ")) {
                    s = s.replace(' ', 'T');
                }
                return LocalDateTime.parse(s).toInstant(ZoneOffset.UTC);
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    public static boolean isFailure(String status) {
        return "failed".equalsIgnoreCase(status) || "unreachable".equalsIgnoreCase(status);
    }

    public static boolean isIgnored(String status) {
        return "ignored".equalsIgnoreCase(status);
    }

    public static List<DynamicTaskRunLog> hostLogs(String host, String status, Object result, boolean fullLogs, boolean storedResults) {
        var logs = new ArrayList<DynamicTaskRunLog>();
        var hasFailureReason = isFailure(status) || isIgnored(status);

        String message;
        if (fullLogs) {
            message = "[" + host + "] " + status + " => " + truncateForLog(stringifyResult(result), storedResults);
        } else if (hasFailureReason) {
            message = "[" + host + "] " + status + " => " + truncateForLog(failureReason(result, storedResults), storedResults);
        } else {
            message = "[" + host + "] " + status;
        }

        logs.add(new DynamicTaskRunLog(logLevel(status), message));

        if (!fullLogs) {
            logs.addAll(failedItemLogs(host, status, result, storedResults));
        }

        return logs;
    }

    public static Level logLevel(String status) {
        if (isIgnored(status)) {
            return Level.WARN;
        }
        return isFailure(status) ? Level.ERROR : Level.INFO;
    }

    private static List<DynamicTaskRunLog> failedItemLogs(String host, String hostStatus, Object rawResult, boolean storedResults) {
        if (!(rawResult instanceof Map<?, ?> result) || !(result.get("results") instanceof List<?> items)) {
            return List.of();
        }

        var failedItems = items.stream()
            .filter(item -> item instanceof Map<?, ?> m && (Boolean.TRUE.equals(m.get("failed")) || Boolean.TRUE.equals(m.get("unreachable"))))
            .map(item -> (Map<?, ?>) item)
            .toList();

        var level = logLevel(hostStatus);
        var logs = new ArrayList<DynamicTaskRunLog>();
        for (var item : failedItems.stream().limit(MAX_FAILED_ITEM_LOGS).toList()) {
            var status = Boolean.TRUE.equals(item.get("unreachable")) ? "unreachable" : "failed";
            var label = itemLabel(item);
            logs.add(
                new DynamicTaskRunLog(
                    level,
                    "[" + host + "] " + status + label + " => " + truncateForLog(failureReason(item, storedResults), storedResults)
                )
            );
        }

        if (failedItems.size() > MAX_FAILED_ITEM_LOGS) {
            var fullResults = storedResults ? "`resultsUri`" : "`playbooks`";
            logs.add(
                new DynamicTaskRunLog(
                    level,
                    "[" + host + "] ... and " + (failedItems.size() - MAX_FAILED_ITEM_LOGS) + " more failed items, see " + fullResults
                )
            );
        }
        return logs;
    }

    /** Prefers loop_control.label over the raw item, like the callback does for the light result; no_log items get none. */
    private static String itemLabel(Map<?, ?> item) {
        if (Boolean.TRUE.equals(item.get("_ansible_no_log"))) {
            return "";
        }
        var raw = item.get("_ansible_item_label") != null ? item.get("_ansible_item_label") : item.get("item");
        if (raw == null) {
            return "";
        }
        return " (item=" + truncate(raw instanceof String text ? text : stringifyResult(raw), MAX_ITEM_LABEL_LENGTH) + ")";
    }

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max);
    }

    public static String failureReason(Object result, boolean storedResults) {
        if (result instanceof Map<?, ?> m && m.get("msg") != null) {
            // msg is cut on its own so rc and the stderr tail still fit under the line limit
            var reason = new StringBuilder(truncateForLog(String.valueOf(m.get("msg")), MAX_MSG_LOG_LENGTH, storedResults));
            if (m.get("rc") != null) {
                reason.append(" (rc=").append(m.get("rc")).append(")");
            }
            if (!Boolean.TRUE.equals(m.get("_ansible_no_log")) && m.get("stderr") instanceof String stderr && !stderr.isEmpty()) {
                var tail = stderr.length() <= STDERR_TAIL_LENGTH ? stderr : stderr.substring(stderr.length() - STDERR_TAIL_LENGTH);
                reason.append(" | stderr: ").append(tail);
            }
            return reason.toString();
        }
        return stringifyResult(result);
    }

    public static String truncateForLog(String s, boolean storedResults) {
        return truncateForLog(s, MAX_LOG_LINE_LENGTH, storedResults);
    }

    private static String truncateForLog(String s, int max, boolean storedResults) {
        if (s == null || s.length() <= max) {
            return s;
        }
        var fullResults = storedResults ? "`resultsUri`" : "`playbooks`";
        return s.substring(0, max) + "... (truncated, see " + fullResults + ")";
    }

    public static String stringifyResult(Object result) {
        if (result == null) {
            return "{}";
        }

        try {
            return JacksonMapper.ofJson().writeValueAsString(result);
        } catch (Exception e) {
            return String.valueOf(result);
        }
    }
}
