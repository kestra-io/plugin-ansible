package io.kestra.plugin.ansible.runner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.kestra.core.serializers.JacksonMapper;

// Replaces rendered secret values with *** in log lines, telemetry and artifact files.
final class SecretMasker {
    static final String MASK = "***";

    // Lines of a multi-line secret (an SSH key) shorter than this carry no usable key material
    // and would mask unrelated text.
    private static final int MIN_FRAGMENT_LENGTH = 4;

    private static final Pattern SENSITIVE_KEY = Pattern.compile(
        "(?i).*(pass|secret|token|credential|private|api[_-]?key|access[_-]?key|ssh[_-]?key|cert[_-]?pass|auth|vault|dsn|connection[_-]?string).*"
    );

    private final Set<String> secrets = new LinkedHashSet<>();
    private List<String> ordered = List.of();

    void add(String value) {
        if (value == null || value.isBlank()) {
            return;
        }
        addToken(value);
        addToken(value.strip());
        if (value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0) {
            for (String line : value.split("\\r?\\n|\\r")) {
                String fragment = line.strip();
                if (fragment.length() >= MIN_FRAGMENT_LENGTH) {
                    addToken(fragment);
                }
            }
        }
        // Longest first, so a secret containing another secret is masked whole
        List<String> sorted = new ArrayList<>(secrets);
        sorted.sort(Comparator.comparingInt(String::length).reversed());
        ordered = List.copyOf(sorted);
    }

    // Masks the values of keys that look sensitive (password, token, api_key...), at any depth.
    void addSensitiveValues(Map<?, ?> map) {
        if (map == null) {
            return;
        }
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (entry.getKey() != null && SENSITIVE_KEY.matcher(entry.getKey().toString()).matches()) {
                addAllLeaves(value);
            } else if (value instanceof Map<?, ?> nested) {
                addSensitiveValues(nested);
            } else if (value instanceof List<?> list) {
                for (Object item : list) {
                    if (item instanceof Map<?, ?> nestedItem) {
                        addSensitiveValues(nestedItem);
                    }
                }
            }
        }
    }

    boolean isEmpty() {
        return ordered.isEmpty();
    }

    String mask(String text) {
        if (text == null || text.isEmpty() || ordered.isEmpty()) {
            return text;
        }
        String masked = text;
        for (String secret : ordered) {
            masked = masked.replace(secret, MASK);
        }
        return masked;
    }

    // Rewrites every text file under the directory with secrets masked, before it is archived.
    void maskTree(Path dir, Logger logger) throws IOException {
        if (ordered.isEmpty() || dir == null || !Files.isDirectory(dir)) {
            return;
        }
        List<Path> files;
        try (Stream<Path> walk = Files.walk(dir)) {
            files = walk.filter(p -> Files.isRegularFile(p) && !Files.isSymbolicLink(p)).toList();
        }
        for (Path file : files) {
            maskFile(file, logger);
        }
    }

    private void maskFile(Path file, Logger logger) throws IOException {
        Path tmp = file.resolveSibling(file.getFileName() + ".masking");
        try (
            BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8);
            BufferedWriter writer = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8)
        ) {
            String line;
            boolean first = true;
            while ((line = reader.readLine()) != null) {
                if (!first) {
                    writer.newLine();
                }
                writer.write(mask(line));
                first = false;
            }
        } catch (CharacterCodingException e) {
            // Not a UTF-8 text file: Ansible never writes secrets into binary artifacts
            Files.deleteIfExists(tmp);
            logger.debug("Skipping secret masking of non-text artifact {}", file.getFileName());
            return;
        }
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
    }

    private void addAllLeaves(Object value) {
        if (value instanceof Map<?, ?> map) {
            map.values().forEach(this::addAllLeaves);
        } else if (value instanceof List<?> list) {
            list.forEach(this::addAllLeaves);
        } else if (value instanceof Boolean || value instanceof Number) {
            return;
        } else if (value != null) {
            String str = value.toString().strip();
            if ("true".equalsIgnoreCase(str) || "false".equalsIgnoreCase(str) || str.length() < MIN_FRAGMENT_LENGTH) {
                return;
            }
            add(str);
        }
    }

    private void addToken(String token) {
        if (token.isEmpty()) {
            return;
        }
        secrets.add(token);
        // Job events are JSON, where quotes, backslashes and newlines in a secret are escaped
        String escaped = jsonEscape(token);
        if (!escaped.equals(token)) {
            secrets.add(escaped);
        }
    }

    private static String jsonEscape(String token) {
        try {
            String quoted = JacksonMapper.ofJson().writeValueAsString(token);
            return quoted.substring(1, quoted.length() - 1);
        } catch (JsonProcessingException e) {
            return token;
        }
    }
}
