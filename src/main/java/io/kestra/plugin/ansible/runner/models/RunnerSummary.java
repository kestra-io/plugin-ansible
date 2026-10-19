package io.kestra.plugin.ansible.runner.models;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class RunnerSummary {

    @Schema(title = "Total count of hosts that encountered task failures")
    @Builder.Default
    private Integer failures = 0;

    @Schema(title = "Total count of hosts that registered state changes")
    @Builder.Default
    private Integer changed = 0;

    @Schema(title = "Total count of successful hosts with no failures")
    @Builder.Default
    private Integer ok = 0;

    @Schema(title = "Total count of unreachable hosts")
    @Builder.Default
    private Integer unreachable = 0;

    @Schema(title = "Total count of skipped tasks across hosts")
    @Builder.Default
    private Integer skipped = 0;
}
