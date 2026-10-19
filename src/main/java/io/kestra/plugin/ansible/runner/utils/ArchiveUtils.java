package io.kestra.plugin.ansible.runner.utils;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

public class ArchiveUtils {

    private static final int MAX_ENTRIES = 10_000;
    private static final long MAX_UNCOMPRESSED_BYTES = 100 * 1024 * 1024L; // 100MB

    /**
     * Recursively compresses a source directory into a zip archive.
     */
    public static Path zipDirectory(Path sourceDir, Path targetZipFile) throws IOException {
        if (!Files.exists(sourceDir)) {
            throw new FileNotFoundException("Source directory does not exist: " + sourceDir);
        }

        if (targetZipFile.getParent() != null) {
            Files.createDirectories(targetZipFile.getParent());
        }

        try (ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(Files.newOutputStream(targetZipFile)))) {
            Files.walkFileTree(sourceDir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (file.equals(targetZipFile)) {
                        return FileVisitResult.CONTINUE;
                    }
                    if (Files.isSymbolicLink(file)) {
                        System.err.println("Warning: Skipping symlink in archive: " + file);
                        return FileVisitResult.CONTINUE;
                    }
                    var relativePath = sourceDir.relativize(file).toString().replace("\\", "/");
                    var zipEntry = new ZipEntry(relativePath);
                    zos.putNextEntry(zipEntry);
                    Files.copy(file, zos);
                    zos.closeEntry();
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    if (!sourceDir.equals(dir)) {
                        var relativePath = sourceDir.relativize(dir).toString().replace("\\", "/") + "/";
                        zos.putNextEntry(new ZipEntry(relativePath));
                        zos.closeEntry();
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        }

        return targetZipFile;
    }

    /**
     * Extracts a zip stream into the target directory.
     */
    public static void unzip(InputStream inputStream, Path targetDir) throws IOException {
        Files.createDirectories(targetDir);
        try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(inputStream))) {
            ZipEntry entry;
            var entryCount = 0;
            var totalBytes = 0L;
            while ((entry = zis.getNextEntry()) != null) {
                entryCount++;
                if (entryCount > MAX_ENTRIES) {
                    throw new IOException("Archive contains too many entries (exceeds " + MAX_ENTRIES + ").");
                }
                var resolvedPath = targetDir.normalize().resolve(entry.getName()).normalize();
                if (!resolvedPath.startsWith(targetDir.normalize())) {
                    throw new IOException("Zip slip security exception: " + entry.getName());
                }

                if (entry.isDirectory()) {
                    Files.createDirectories(resolvedPath);
                } else {
                    if (resolvedPath.getParent() != null) {
                        Files.createDirectories(resolvedPath.getParent());
                    }
                    try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(resolvedPath))) {
                        byte[] buffer = new byte[8192];
                        int read;
                        while ((read = zis.read(buffer)) != -1) {
                            totalBytes += read;
                            if (totalBytes > MAX_UNCOMPRESSED_BYTES) {
                                throw new IOException("Archive uncompressed size exceeds limit of " + MAX_UNCOMPRESSED_BYTES + " bytes.");
                            }
                            os.write(buffer, 0, read);
                        }
                    }
                }
                zis.closeEntry();
            }
        }
    }

    /**
     * Extracts a tar.gz / tgz stream into the target directory.
     */
    public static void untarGz(InputStream inputStream, Path targetDir) throws IOException {
        Files.createDirectories(targetDir);
        try (
            var gzis = new GzipCompressorInputStream(new BufferedInputStream(inputStream));
            var tais = new TarArchiveInputStream(gzis)
        ) {
            TarArchiveEntry entry;
            var entryCount = 0;
            var totalBytes = 0L;
            while ((entry = tais.getNextTarEntry()) != null) {
                entryCount++;
                if (entryCount > MAX_ENTRIES) {
                    throw new IOException("Archive contains too many entries (exceeds " + MAX_ENTRIES + ").");
                }

                if (entry.isSymbolicLink() || entry.isLink()) {
                    System.err.println("Warning: Skipping symlink in archive: " + entry.getName());
                    continue;
                }

                var resolvedPath = targetDir.normalize().resolve(entry.getName()).normalize();
                if (!resolvedPath.startsWith(targetDir.normalize())) {
                    throw new IOException("Tar slip security exception: " + entry.getName());
                }

                if (entry.isDirectory()) {
                    Files.createDirectories(resolvedPath);
                } else {
                    if (resolvedPath.getParent() != null) {
                        Files.createDirectories(resolvedPath.getParent());
                    }
                    try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(resolvedPath))) {
                        byte[] buffer = new byte[8192];
                        int read;
                        while ((read = tais.read(buffer)) != -1) {
                            totalBytes += read;
                            if (totalBytes > MAX_UNCOMPRESSED_BYTES) {
                                throw new IOException("Archive uncompressed size exceeds limit of " + MAX_UNCOMPRESSED_BYTES + " bytes.");
                            }
                            os.write(buffer, 0, read);
                        }
                    }
                    if ((entry.getMode() & 0111) != 0) {
                        resolvedPath.toFile().setExecutable(true, false);
                    }
                }
            }
        }
    }
}
