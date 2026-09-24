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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Comparator;
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

import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.UnixModeToPosixFilePermissions;

/**
 * Caches the {@code .kestra_ansible} working-directory subtree that {@link AnsibleCLI} installs
 * Galaxy collections/roles and Python packages into (issue #135): key computation, and tar/gzip
 * packing and unpacking against Kestra's storage cache API.
 */
final class AnsibleDependencyCache {
    // Bump when the layout of what gets tarred changes, to avoid restoring an incompatible cache.
    private static final int CACHE_FORMAT_VERSION = 0;
    static final String CACHE_ID = "ansible-dependencies-v" + CACHE_FORMAT_VERSION;

    // Working-directory subtree that Galaxy collections/roles and pip packages install into, kept
    // out of the image's default locations so it is shared by every command in a task and cacheable.
    static final String DEPENDENCY_ROOT = ".kestra_ansible";
    // Touched only once the whole install chain succeeds; absence tells a failed install from one that never ran.
    static final String COMPLETE_MARKER = DEPENDENCY_ROOT + "/.complete";

    private AnsibleDependencyCache() {
    }

    /**
     * SHA-256 hash of everything that determines what gets installed, so a change to any of it
     * invalidates the cache. Every field is length-prefixed so adjacent values (e.g. two dependency
     * entries, or the boundary between the two dependency lists) can never collide by concatenation.
     */
    static String computeHash(
        String taskRunnerType,
        String containerImage,
        List<String> galaxyDependencies,
        List<String> pythonDependencies,
        byte[] requirementsYml,
        byte[] requirementsTxt) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            updateField(digest, String.valueOf(CACHE_FORMAT_VERSION));
            updateField(digest, taskRunnerType);
            updateField(digest, containerImage == null ? "" : containerImage);
            updateField(digest, String.valueOf(galaxyDependencies.size()));
            for (String dependency : galaxyDependencies) {
                updateField(digest, dependency);
            }
            updateField(digest, String.valueOf(pythonDependencies.size()));
            for (String dependency : pythonDependencies) {
                updateField(digest, dependency);
            }
            updateField(digest, requirementsYml == null ? new byte[0] : requirementsYml);
            updateField(digest, requirementsTxt == null ? new byte[0] : requirementsTxt);
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is a JDK-mandatory algorithm
            throw new IllegalStateException(e);
        }
    }

    private static void updateField(MessageDigest digest, String value) {
        updateField(digest, value.getBytes(StandardCharsets.UTF_8));
    }

    private static void updateField(MessageDigest digest, byte[] value) {
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value.length).array());
        digest.update(value);
    }

    /**
     * Single-quotes a dependency string for safe use in a shell command line, rejecting characters
     * that cannot be escaped this way.
     */
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

    /**
     * Restores a previously cached {@code .kestra_ansible} tree into the working directory.
     * Returns {@code false} on a cache miss, and also on any I/O or validation failure (corrupt
     * archive, tar-slip attempt): callers fall back to a normal install either way, so a cache
     * problem never fails the task.
     */
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
            deleteRecursively(root);
            return false;
        }
    }

    private static void extractEntry(TarArchiveInputStream tais, TarArchiveEntry entry, Path root) throws IOException {
        Path outputPath = root.resolve(entry.getName()).normalize();
        // The restored tree ends up on ANSIBLE_COLLECTIONS_PATH/PYTHONPATH, i.e. it is executable
        // code: an entry name containing "../" must never be allowed to write outside the cache root.
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
        } else {
            Files.createDirectories(outputPath.getParent());
            try (OutputStream os = Files.newOutputStream(outputPath)) {
                tais.transferTo(os);
            }
            try {
                Files.setPosixFilePermissions(outputPath, UnixModeToPosixFilePermissions.toPosixPermissions(entry.getMode()));
            } catch (UnsupportedOperationException | IOException e) {
                // best effort: file system may not support POSIX permissions (e.g. Windows)
            }
        }
    }

    private static void deleteRecursively(Path root) {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path ->
            {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ignored) {
                    // best effort cleanup of a partially-restored, rejected cache
                }
            });
        } catch (IOException ignored) {
            // best effort cleanup
        }
    }

    /** Tars and gzips the {@code .kestra_ansible} tree and uploads it to Kestra's storage cache. */
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
                    } catch (UnsupportedOperationException | IOException e) {
                        // best effort: file system may not support POSIX permissions (e.g. Windows)
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
