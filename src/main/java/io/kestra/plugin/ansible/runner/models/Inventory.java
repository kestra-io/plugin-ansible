package io.kestra.plugin.ansible.runner.models;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class Inventory {

    @Schema(
        title = "Inventory file URI or path",
        description = "URI to a Kestra Internal Storage or Namespace file (e.g., static hosts file or cached JSON)."
    )
    private Property<String> file;

    @Schema(
        title = "Inline inventory content",
        description = "Raw INI or YAML inventory content defined directly in the flow."
    )
    private Property<String> content;

    @Schema(
        title = "Inline inventory content alias",
        description = "Alternative alias for inline inventory content."
    )
    private Property<String> inline;

    @Schema(
        title = "Multiple inventory sources",
        description = "List of inventory files, directories, or URIs to include in /runner/inventory/."
    )
    private Property<List<String>> sources;

    @Schema(
        title = "Structured host definitions",
        description = "Map of hostnames to their respective host variables, serialized as /runner/inventory/hosts.json."
    )
    private Property<Map<String, Object>> hosts;

    @Schema(
        title = "Structured inventory groups",
        description = "Map of Ansible group definitions containing hosts, child groups, and group vars."
    )
    private Property<Map<String, Object>> groups;

    @Schema(
        title = "Dynamic inventory script",
        description = "Dynamic inventory script content or path to an executable script."
    )
    private Property<String> script;

    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public static Inventory fromString(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.contains("\n") || trimmed.startsWith("[") || trimmed.startsWith("---") || trimmed.contains("=") || trimmed.contains(" ")) {
            return Inventory.builder().content(Property.ofValue(value)).build();
        }
        return Inventory.builder().file(Property.ofValue(value)).build();
    }
}
