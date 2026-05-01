package com.example.financedashboard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record AnalysisSnapshot(
        @JsonProperty("snapshot_id") Long snapshotId,
        String symbol,
        @JsonProperty("asset_type") String assetType,
        String exchange,
        @JsonProperty("generated_at") String generatedAt,
        @JsonProperty("last_price") Double lastPrice,
        @JsonProperty("last_close") Double lastClose,
        @JsonProperty("daily_return") Double dailyReturn,
        @JsonProperty("sma_20") Double sma20,
        @JsonProperty("sma_50") Double sma50,
        @JsonProperty("volatility_20") Double volatility20,
        @JsonProperty("min_30d") Double min30d,
        @JsonProperty("max_30d") Double max30d,
        String trend,
        String signal,
        String notes
) {
}
