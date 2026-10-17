package io.kestra.plugin.ansible.runner.models;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class Project {

    @Schema(
        title = "Project playbook source directory, archive URI, or inline playbook",
        description = "URI to the Kestra storage file, zip archive, local directory, or inline playbook YAML string."
    )
    private Property<String> source;

    @Schema(
        title = "Inline playbook YAML content",
        description = "Inline YAML string representing the playbook. Automatically written to the entrypoint playbook file."
    )
    private Property<String> content;

    @Schema(
        title = "Inline playbook YAML content (alias for content)",
        description = "Inline YAML string representing the playbook."
    )
    private Property<String> inline;

    @Schema(
        title = "Entrypoint playbook filename",
        description = "The playbook file to execute within the project directory (e.g. site.yml)."
    )
    @Builder.Default
    private Property<String> playbook = Property.ofValue("site.yml");

    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public static Project fromString(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.contains("\n") || trimmed.startsWith("---") || trimmed.startsWith("- ")) {
            return Project.builder().content(Property.ofValue(value)).build();
        } else if (trimmed.endsWith(".zip") || trimmed.endsWith(".tar.gz") || trimmed.endsWith(".tgz") || trimmed.startsWith("kestra://")) {
            return Project.builder().source(Property.ofValue(value)).build();
        } else {
            return Project.builder().playbook(Property.ofValue(value)).build();
        }
    }
}
