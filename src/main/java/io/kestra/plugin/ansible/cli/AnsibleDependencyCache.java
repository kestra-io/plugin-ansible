package io.kestra.plugin.ansible.cli;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.FileUtils;

import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.UnixModeToPosixFilePermissions;

/** Caches the working-dir tree {@link AnsibleCLI} installs Galaxy and Python dependencies into. */
final class AnsibleDependencyCache {
    // bump when the archive layout changes
    private static final int CACHE_FORMAT_VERSION = 0;
    static final String CACHE_ID = "ansible-dependencies-v" + CACHE_FORMAT_VERSION;

    static final String DEPENDENCY_ROOT = ".kestra_ansible";
    // written only when the whole install chain succeeded
    static final String COMPLETE_MARKER = DEPENDENCY_ROOT + "/.complete";

    private AnsibleDependencyCache() {
    }

    /** SHA-256 over every install input, each field length-prefixed so adjacent values cannot collide. */
    static String computeHash(
        String taskRunnerType,
        String containerImage,
        List<String> galaxyDependencies,
        List<String> pythonDependencies,
        Path requirementsYml,
        Path requirementsTxt) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            updateField(digest, String.valueOf(CACHE_FORMAT_VERSION));
            updateField(digest, taskRunnerType);
            updateField(digest, containerImage == null ? "" : containerImage);
            updateField(digest, galaxyDependencies);
            updateField(digest, pythonDependencies);
            updateField(digest, requirementsYml);
            updateField(digest, requirementsTxt);
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void updateField(MessageDigest digest, List<String> values) {
        updateField(digest, String.valueOf(values.size()));
        values.forEach(value -> updateField(digest, value));
    }

    private static void updateField(MessageDigest digest, String value) {
        updateField(digest, value.getBytes(StandardCharsets.UTF_8));
    }

    // streamed, so a large user-supplied file never lands whole in the worker heap
    private static void updateField(MessageDigest digest, Path file) throws IOException {
        if (file == null) {
            updateField(digest, new byte[0]);
            return;
        }
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(Files.size(file)).array());
        try (InputStream in = new DigestInputStream(Files.newInputStream(file), digest)) {
            in.transferTo(OutputStream.nullOutputStream());
        }
    }

    private static void updateField(MessageDigest digest, byte[] value) {
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value.length).array());
        digest.update(value);
    }

    /** Single-quotes a value for a shell command line, rejecting what single quotes cannot hold. */
    static String quote(String propertyName, String value) {
        if (value.indexOf('\0') >= 0 || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0) {
            throw new IllegalArgumentException(
                "Invalid entry in `" + propertyName + "`: '" + value + "' must not contain a NUL or newline character."
            );
        }
        return "'" + value.replace("'", "'\\''") + "'";
    }

    static String quoteAll(String propertyName, List<String> values) {
        return values.stream()
            .map(value -> quote(propertyName, value))
            .collect(Collectors.joining(" "));
    }

    /** Returns false on a miss or any failure, so a broken cache only means a normal install. */
    static boolean restore(RunContext runContext, Path workingDir, String hash, Duration ttl) {
        Path root = workingDir.resolve(DEPENDENCY_ROOT).normalize();
        try {
            Optional<InputStream> cacheFile = runContext.storage().getCacheFile(CACHE_ID, hash, ttl);
            if (cacheFile.isEmpty()) {
                return false;
            }

            try (
                InputStream is = cacheFile.get();
                GzipCompressorInputStream gzis = new GzipCompressorInputStream(is);
                TarArchiveInputStream tais = new TarArchiveInputStream(gzis)
            ) {
                TarArchiveEntry entry;
                while ((entry = tais.getNextEntry()) != null) {
                    extractEntry(tais, entry, root);
                }
            }
            return true;
        } catch (IOException e) {
            runContext.logger().warn("Unable to restore the Ansible dependency cache, falling back to a normal install: {}", e.getMessage());
            FileUtils.deleteQuietly(root.toFile());
            return false;
        }
    }

    private static void extractEntry(TarArchiveInputStream tais, TarArchiveEntry entry, Path root) throws IOException {
        Path outputPath = root.resolve(entry.getName()).normalize();
        // the restored tree is executable code, nothing may land outside the root
        if (!outputPath.startsWith(root)) {
            throw new IOException("cache entry escapes the cache root: " + entry.getName());
        }

        if (entry.isSymbolicLink()) {
            Path target = outputPath.getParent().resolve(entry.getLinkName()).normalize();
            if (!target.startsWith(root)) {
                throw new IOException("cache symlink escapes the cache root: " + entry.getName() + " -> " + entry.getLinkName());
            }
            Files.createDirectories(outputPath.getParent());
            Files.deleteIfExists(outputPath);
            Files.createSymbolicLink(outputPath, Path.of(entry.getLinkName()));
        } else if (entry.isLink()) {
            Path target = root.resolve(entry.getLinkName()).normalize();
            if (!target.startsWith(root)) {
                throw new IOException("cache hardlink escapes the cache root: " + entry.getName() + " -> " + entry.getLinkName());
            }
            Files.createDirectories(outputPath.getParent());
            Files.deleteIfExists(outputPath);
            Files.createLink(outputPath, target);
        } else if (entry.isDirectory()) {
            Files.createDirectories(outputPath);
            restoreMode(outputPath, entry);
        } else {
            Files.createDirectories(outputPath.getParent());
            try (OutputStream os = Files.newOutputStream(outputPath)) {
                tais.transferTo(os);
            }
            restoreMode(outputPath, entry);
        }
    }

    private static void restoreMode(Path path, TarArchiveEntry entry) {
        try {
            Files.setPosixFilePermissions(path, UnixModeToPosixFilePermissions.toPosixPermissions(entry.getMode()));
        } catch (UnsupportedOperationException | IOException ignored) {
            // no POSIX permissions on this file system
        }
    }

    static void upload(RunContext runContext, Path workingDir, String hash) throws IOException {
        Path root = workingDir.resolve(DEPENDENCY_ROOT).normalize();
        Path tempFile = runContext.workingDir().createTempFile(".tar.gz");

        try (
            OutputStream fos = Files.newOutputStream(tempFile);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            GzipCompressorOutputStream gzos = new GzipCompressorOutputStream(bos);
            TarArchiveOutputStream taos = new TarArchiveOutputStream(gzos)
        ) {
            taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
            Files.walkFileTree(root, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    if (!dir.equals(root)) {
                        addEntry(dir, root.relativize(dir) + "/");
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    addEntry(file, root.relativize(file).toString());
                    return FileVisitResult.CONTINUE;
                }

                private void addEntry(Path path, String rawEntryName) throws IOException {
                    String entryName = rawEntryName.replace("\\", "/");

                    if (Files.isSymbolicLink(path)) {
                        TarArchiveEntry entry = new TarArchiveEntry(entryName, TarConstants.LF_SYMLINK);
                        entry.setLinkName(Files.readSymbolicLink(path).toString());
                        taos.putArchiveEntry(entry);
                        taos.closeArchiveEntry();
                        return;
                    }

                    TarArchiveEntry entry = new TarArchiveEntry(path.toFile(), entryName);
                    try {
                        entry.setMode(UnixModeToPosixFilePermissions.fromPosixFilePermissions(Files.getPosixFilePermissions(path)));
                    } catch (UnsupportedOperationException | IOException ignored) {
                        // no POSIX permissions on this file system
                    }
                    taos.putArchiveEntry(entry);
                    if (!Files.isDirectory(path)) {
                        Files.copy(path, taos);
                    }
                    taos.closeArchiveEntry();
                }
            });
        }

        runContext.storage().putCacheFile(tempFile.toFile(), CACHE_ID, hash);
    }
}
