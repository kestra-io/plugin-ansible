package io.kestra.plugin.ansible.runner.models;

import java.util.Map;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class RunnerEnv {

    @Schema(
        title = "Extra variables passed as JSON to /runner/env/extravars",
        description = "Arbitrary variables passed to Ansible as extra variables. Note: values under keys matching sensitive patterns (pass, token, secret, credential, private, key, auth, vault, dsn) are automatically masked in logs, telemetry, and artifacts; for sensitive credentials, passing them via `passwords` is strongly recommended."
    )
    private Property<Map<String, Object>> extraVars;

    @Schema(
        title = "Environment variables for /runner/env/envvars",
        description = "Environment variables exported inside the runner container execution. Note: values under keys matching sensitive patterns are automatically masked; for sensitive credentials, passing them via `passwords` is strongly recommended."
    )
    private Property<Map<String, String>> envVars;

    @Schema(
        title = "Interactive prompt passwords for /runner/env/passwords",
        description = "Mapping of prompt regex/strings to passwords for Vault, sudo, Cisco enable secrets, etc. All values in this map are guaranteed to be masked across all streamed logs, output log files, and packaged artifacts."
    )
    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    private Property<Map<String, String>> passwords;

    @Schema(
        title = "Private SSH key for device authentication",
        description = "Private SSH key securely written to /runner/env/ssh_key with 0600 file permissions and masked across all logs and artifacts."
    )
    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    private Property<String> sshKey;
}
