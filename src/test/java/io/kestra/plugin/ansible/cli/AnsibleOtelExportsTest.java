package io.kestra.plugin.ansible.cli;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class AnsibleOtelExportsTest {

    // Runs the OTel exports the task prepends to every command, then prints the variables they set.
    private static String run(Map<String, String> env) throws Exception {
        String script = String.join("\n", AnsibleCLI.OTEL_EXPORTS)
            + "\necho \"${ANSIBLE_CALLBACKS_ENABLED:-}|${OTEL_EXPORTER_OTLP_TRACES_PROTOCOL:-}\"";
        ProcessBuilder builder = new ProcessBuilder("/bin/sh", "-c", script);
        builder.environment().remove("ANSIBLE_CALLBACKS_ENABLED");
        builder.environment().remove("OTEL_EXPORTER_OTLP_ENDPOINT");
        builder.environment().remove("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL");
        builder.environment().putAll(env);
        Process process = builder.start();
        String out = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        process.waitFor();
        return out;
    }

    @Test
    void shouldNotEnableOpenTelemetryWithoutEndpoint() throws Exception {
        assertThat(run(Map.of()), is("|"));
    }

    @Test
    void shouldEnableOpenTelemetryAndSwitchKotlpHttpJsonToProtobuf() throws Exception {
        String out = run(Map.of(
            "OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318",
            "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/json"
        ));

        assertThat(out, is("kestra_logger,community.general.opentelemetry|http/protobuf"));
    }

    @Test
    void shouldDefaultTracesProtocolToProtobufWhenUnset() throws Exception {
        String out = run(Map.of("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318"));

        assertThat(out, is("kestra_logger,community.general.opentelemetry|http/protobuf"));
    }

    @Test
    void shouldKeepUserCallbacksAndExplicitProtocol() throws Exception {
        String out = run(Map.of(
            "OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317",
            "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "grpc",
            "ANSIBLE_CALLBACKS_ENABLED", "kestra_logger,ara_default"
        ));

        assertThat(out, is("kestra_logger,ara_default,community.general.opentelemetry|grpc"));
    }
}
