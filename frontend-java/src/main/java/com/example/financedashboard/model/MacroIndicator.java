package com.example.financedashboard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record MacroIndicator(
        String date,
        @JsonProperty("indicator_code") String indicatorCode,
        @JsonProperty("indicator_name") String indicatorName,
        Double value,
        String unit,
        String source,
        @JsonProperty("fetched_at") String fetchedAt
) {
}
