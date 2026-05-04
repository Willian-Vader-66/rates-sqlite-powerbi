package com.example.financedashboard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record MarketSignal(
        String symbol,
        @JsonProperty("asset_type") String assetType,
        String trend,
        String signal,
        @JsonProperty("generated_at") String generatedAt
) {
}
