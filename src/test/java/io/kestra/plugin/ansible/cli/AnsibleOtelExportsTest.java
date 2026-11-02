package io.kestra.plugin.ansible.cli;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

// Runs the real task in the Docker task runner and reads back the environment the command sees.
@KestraTest
class AnsibleOtelExportsTest {
    private static final String KOTLP_ENDPOINT = "http://127.0.0.1:4318";

    @Inject
    private RunContextFactory runContextFactory;

    private String run(Map<String, String> env) throws Exception {
        String command = "echo \"::{\\\"outputs\\\":{\\\"seen\\\":\\\"cb=${ANSIBLE_CALLBACKS_ENABLED:-}"
            + "|proto=${OTEL_EXPORTER_OTLP_TRACES_PROTOCOL:-}"
            + "|hide=${ANSIBLE_OPENTELEMETRY_HIDE_TASK_ARGUMENTS:-}"
            + "|logs=${ANSIBLE_OPENTELEMETRY_DISABLE_LOGS:-}\\\"}}::\"";

        AnsibleCLI task = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .env(Property.ofValue(env))
            .commands(Property.ofValue(List.of(command)))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        ScriptOutput output = task.run(runContext);

        assertThat(output.getExitCode(), is(0));
        return String.valueOf(output.getVars().get("seen"));
    }

    @Test
    void shouldNotEnableOpenTelemetryWithoutEndpoint() throws Exception {
        assertThat(run(Map.of("UNRELATED", "1")), is("cb=|proto=|hide=|logs="));
    }

    @Test
    void shouldEnableOpenTelemetryForKotlpEndpointWithSafeDefaults() throws Exception {
        String seen = run(
            Map.of(
                "OTEL_EXPORTER_OTLP_ENDPOINT", KOTLP_ENDPOINT,
                "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/json"
            )
        );

        assertThat(seen, is("cb=kestra_logger,community.general.opentelemetry|proto=http/protobuf|hide=true|logs=true"));
    }

    @Test
    void shouldDefaultTracesProtocolToProtobufWhenUnset() throws Exception {
        String seen = run(Map.of("OTEL_EXPORTER_OTLP_ENDPOINT", KOTLP_ENDPOINT));

        assertThat(seen, is("cb=kestra_logger,community.general.opentelemetry|proto=http/protobuf|hide=true|logs=true"));
    }

    @Test
    void shouldKeepUserCallbacksProtocolAndExplicitOverrides() throws Exception {
        String seen = run(
            Map.of(
                "OTEL_EXPORTER_OTLP_ENDPOINT", KOTLP_ENDPOINT,
                "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "grpc",
                "ANSIBLE_CALLBACKS_ENABLED", "kestra_logger,ara_default",
                "ANSIBLE_OPENTELEMETRY_HIDE_TASK_ARGUMENTS", "false",
                "ANSIBLE_OPENTELEMETRY_DISABLE_LOGS", "false"
            )
        );

        assertThat(seen, is("cb=kestra_logger,ara_default,community.general.opentelemetry|proto=grpc|hide=false|logs=false"));
    }

    @Test
    void shouldLeaveCustomCollectorEndpointUntouched() throws Exception {
        String seen = run(
            Map.of(
                "OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector.example.com:4317",
                "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/json"
            )
        );

        assertThat(seen, is("cb=|proto=http/json|hide=|logs="));
    }
}
