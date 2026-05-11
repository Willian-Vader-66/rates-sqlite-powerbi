package com.example.financedashboard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DashboardCard(
        String label,
        Double value,
        Double change,
        String unit,
        String status,
        String provider,
        @JsonProperty("data_mode") String dataMode,
        @JsonProperty("data_warning") String dataWarning
) {
}
