package io.kestra.plugin.ansible.runner.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArchiveUtilsTest {

    @Test
    void testZipDirectoryAndUnzip(@TempDir Path tempDir) throws Exception {
        Path sourceDir = tempDir.resolve("source");
        Files.createDirectories(sourceDir.resolve("subdir"));
        Files.writeString(sourceDir.resolve("file1.txt"), "Hello World");
        Files.writeString(sourceDir.resolve("subdir").resolve("file2.txt"), "Subdir content");

        Path zipFile = tempDir.resolve("archive.zip");
        ArchiveUtils.zipDirectory(sourceDir, zipFile);

        assertTrue(Files.exists(zipFile));
        assertThat(Files.size(zipFile), greaterThan(0L));

        Path targetDir = tempDir.resolve("extracted");
        try (var is = Files.newInputStream(zipFile)) {
            ArchiveUtils.unzip(is, targetDir);
        }

        assertThat(Files.readString(targetDir.resolve("file1.txt")), is("Hello World"));
        assertThat(Files.readString(targetDir.resolve("subdir").resolve("file2.txt")), is("Subdir content"));
    }

    @Test
    void testUnzipFromMemory(@TempDir Path tempDir) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            zos.putNextEntry(new ZipEntry("nested/test.yml"));
            zos.write("- hosts: localhost\n".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        }

        Path targetDir = tempDir.resolve("mem_extracted");
        ArchiveUtils.unzip(new ByteArrayInputStream(baos.toByteArray()), targetDir);

        Path extractedFile = targetDir.resolve("nested/test.yml");
        assertTrue(Files.exists(extractedFile));
        assertThat(Files.readString(extractedFile), containsString("hosts: localhost"));
    }
}
