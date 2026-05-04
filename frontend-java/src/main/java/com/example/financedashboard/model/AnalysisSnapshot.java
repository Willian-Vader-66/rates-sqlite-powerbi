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
        @JsonProperty("change_30d") Double change30d,
        @JsonProperty("change_90d") Double change90d,
        @JsonProperty("change_1y") Double change1y,
        @JsonProperty("sma_20") Double sma20,
        @JsonProperty("sma_50") Double sma50,
        @JsonProperty("volatility_20") Double volatility20,
        @JsonProperty("min_30d") Double min30d,
        @JsonProperty("max_30d") Double max30d,
        String trend,
        String signal,
        String notes,
        @JsonProperty("display_name") String displayName,
        @JsonProperty("base_currency") String baseCurrency,
        @JsonProperty("quote_currency") String quoteCurrency,
        @JsonProperty("display_pair") String displayPair,
        @JsonProperty("display_unit") String displayUnit,
        @JsonProperty("value_format") String valueFormat,
        @JsonProperty("chart_title") String chartTitle,
        @JsonProperty("chart_subtitle") String chartSubtitle,
        @JsonProperty("axis_label") String axisLabel,
        @JsonProperty("tooltip_label") String tooltipLabel,
        @JsonProperty("technical_label") String technicalLabel,
        @JsonProperty("technical_score") Integer technicalScore,
        @JsonProperty("technical_tone") String technicalTone,
        @JsonProperty("technical_summary") String technicalSummary
) {
}
