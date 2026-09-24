package io.kestra.plugin.ansible.cli;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class AnsibleDependencyCacheTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void computeHash_isStableForSameInputs() {
        String h1 = AnsibleDependencyCache.computeHash("Docker", "img", List.of("a", "b"), List.of("c"), "y".getBytes(StandardCharsets.UTF_8), "t".getBytes(StandardCharsets.UTF_8));
        String h2 = AnsibleDependencyCache.computeHash("Docker", "img", List.of("a", "b"), List.of("c"), "y".getBytes(StandardCharsets.UTF_8), "t".getBytes(StandardCharsets.UTF_8));

        assertThat(h1, is(h2));
    }

    @Test
    void computeHash_changesWithEachInput() {
        String base = AnsibleDependencyCache.computeHash("Docker", "img", List.of("a"), List.of(), null, null);

        assertThat(AnsibleDependencyCache.computeHash("Docker", "other-img", List.of("a"), List.of(), null, null), is(not(base)));
        assertThat(AnsibleDependencyCache.computeHash("Process", "img", List.of("a"), List.of(), null, null), is(not(base)));
        assertThat(AnsibleDependencyCache.computeHash("Docker", "img", List.of("b"), List.of(), null, null), is(not(base)));
        assertThat(AnsibleDependencyCache.computeHash("Docker", "img", List.of("a"), List.of("c"), null, null), is(not(base)));
        assertThat(AnsibleDependencyCache.computeHash("Docker", "img", List.of("a"), List.of(), "y".getBytes(StandardCharsets.UTF_8), null), is(not(base)));
        assertThat(AnsibleDependencyCache.computeHash("Docker", "img", List.of("a"), List.of(), null, "t".getBytes(StandardCharsets.UTF_8)), is(not(base)));
    }

    @Test
    void computeHash_doesNotCollideAcrossDependencyListBoundary() {
        // ["a","b"] galaxy / [] python must not hash the same as ["a"] galaxy / ["b"] python
        String h1 = AnsibleDependencyCache.computeHash("Docker", "img", List.of("a", "b"), List.of(), null, null);
        String h2 = AnsibleDependencyCache.computeHash("Docker", "img", List.of("a"), List.of("b"), null, null);

        assertThat(h1, is(not(h2)));
    }

    @Test
    void quote_wrapsAndEscapesEmbeddedSingleQuote() {
        assertThat(AnsibleDependencyCache.quote("galaxyDependencies", "community.general"), is("'community.general'"));
        assertThat(AnsibleDependencyCache.quote("galaxyDependencies", "it's"), is("'it'\\''s'"));
    }

    @Test
    void quote_rejectsNewline() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> AnsibleDependencyCache.quote("pythonDependencies", "evil\nrm -rf /")
        );

        assertThat(e.getMessage(), containsString("pythonDependencies"));
        assertThat(e.getMessage(), containsString("evil"));
    }

    @Test
    void quote_rejectsNulCharacter() {
        assertThrows(IllegalArgumentException.class, () -> AnsibleDependencyCache.quote("galaxyDependencies", "evil\0"));
    }

    @Test
    void quoteAll_joinsWithSpaces() {
        assertThat(
            AnsibleDependencyCache.quoteAll("galaxyDependencies", List.of("a", "b:>=1.0")),
            is("'a' 'b:>=1.0'")
        );
    }

    @Test
    void restore_returnsFalseOnCacheMiss() {
        RunContext runContext = newRunContext();

        boolean hit = AnsibleDependencyCache.restore(runContext, runContext.workingDir().path(), IdUtils.create(), null);

        assertThat(hit, is(false));
    }

    @Test
    void uploadThenRestore_roundTripsFilesDirsAndSymlink() throws Exception {
        RunContext runContext = newRunContext();
        Path workingDir = runContext.workingDir().path();
        Path root = workingDir.resolve(AnsibleDependencyCache.DEPENDENCY_ROOT);

        Files.createDirectories(root.resolve("collections/ansible_collections/community/general"));
        Files.writeString(root.resolve("collections/ansible_collections/community/general/MANIFEST.json"), "{}");
        Files.createDirectories(root.resolve("python/bin"));
        Files.createSymbolicLink(root.resolve("python/bin/python3"), Path.of("python3.12"));
        Files.createFile(root.resolve(".complete"));

        String hash = IdUtils.create();
        AnsibleDependencyCache.upload(runContext, workingDir, hash);

        // wipe the tree so a successful restore can only come from the uploaded cache, not disk
        try (var walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
        }
        assertThat(Files.exists(root), is(false));

        boolean hit = AnsibleDependencyCache.restore(runContext, workingDir, hash, null);

        assertThat(hit, is(true));
        assertThat(Files.readString(root.resolve("collections/ansible_collections/community/general/MANIFEST.json")), is("{}"));
        assertThat(Files.isSymbolicLink(root.resolve("python/bin/python3")), is(true));
        assertThat(Files.readSymbolicLink(root.resolve("python/bin/python3")), is(Path.of("python3.12")));
        assertThat(Files.exists(root.resolve(".complete")), is(true));
    }

    @Test
    void restore_rejectsTarEntryEscapingRoot() throws Exception {
        RunContext runContext = newRunContext();
        Path workingDir = runContext.workingDir().path();
        String hash = IdUtils.create();

        putMaliciousCache(runContext, hash, tar ->
        {
            TarArchiveEntry entry = new TarArchiveEntry("../escaped.txt");
            entry.setSize(4);
            tar.putArchiveEntry(entry);
            tar.write("evil".getBytes(StandardCharsets.UTF_8));
            tar.closeArchiveEntry();
        });

        boolean hit = AnsibleDependencyCache.restore(runContext, workingDir, hash, null);

        assertThat(hit, is(false));
        assertThat(Files.exists(workingDir.resolve("escaped.txt")), is(false));
    }

    @Test
    void restore_rejectsSymlinkEscapingRoot() throws Exception {
        RunContext runContext = newRunContext();
        Path workingDir = runContext.workingDir().path();
        String hash = IdUtils.create();

        putMaliciousCache(runContext, hash, tar ->
        {
            TarArchiveEntry entry = new TarArchiveEntry("evil-link", TarConstants.LF_SYMLINK);
            entry.setLinkName("../../../etc/passwd");
            tar.putArchiveEntry(entry);
            tar.closeArchiveEntry();
        });

        boolean hit = AnsibleDependencyCache.restore(runContext, workingDir, hash, null);

        assertThat(hit, is(false));
        assertThat(Files.exists(workingDir.resolve(AnsibleDependencyCache.DEPENDENCY_ROOT).resolve("evil-link")), is(false));
    }

    @Test
    void restore_rejectsHardlinkEscapingRoot() throws Exception {
        RunContext runContext = newRunContext();
        Path workingDir = runContext.workingDir().path();
        String hash = IdUtils.create();

        putMaliciousCache(runContext, hash, tar ->
        {
            TarArchiveEntry entry = new TarArchiveEntry("evil-hardlink", TarConstants.LF_LINK);
            entry.setLinkName("../outside.txt");
            tar.putArchiveEntry(entry);
            tar.closeArchiveEntry();
        });

        boolean hit = AnsibleDependencyCache.restore(runContext, workingDir, hash, null);

        assertThat(hit, is(false));
        assertThat(Files.exists(workingDir.resolve(AnsibleDependencyCache.DEPENDENCY_ROOT).resolve("evil-hardlink")), is(false));
    }

    private RunContext newRunContext() {
        AnsibleCLI task = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .commands(Property.ofValue(List.of("true")))
            .build();

        return TestsUtils.mockRunContext(runContextFactory, task, Map.of());
    }

    private interface TarWriter {
        void write(TarArchiveOutputStream tar) throws IOException;
    }

    private void putMaliciousCache(RunContext runContext, String hash, TarWriter writer) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (
            GzipCompressorOutputStream gzos = new GzipCompressorOutputStream(bytes);
            TarArchiveOutputStream taos = new TarArchiveOutputStream(gzos)
        ) {
            writer.write(taos);
        }

        Path tempFile = runContext.workingDir().createTempFile(".tar.gz");
        Files.write(tempFile, bytes.toByteArray());
        runContext.storage().putCacheFile(tempFile.toFile(), AnsibleDependencyCache.CACHE_ID, hash);
    }
}
