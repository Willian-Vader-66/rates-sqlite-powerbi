package com.example.financedashboard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Quote(
        String symbol,
        @JsonProperty("asset_type") String assetType,
        String exchange,
        Double price,
        Double bid,
        Double ask,
        Double open,
        Double high,
        Double low,
        @JsonProperty("previous_close") Double previousClose,
        Double change,
        @JsonProperty("percent_change") Double percentChange,
        Long volume,
        @JsonProperty("quote_time") String quoteTime,
        String provider,
        @JsonProperty("fetched_at") String fetchedAt,
        @JsonProperty("display_name") String displayName,
        @JsonProperty("base_currency") String baseCurrency,
        @JsonProperty("quote_currency") String quoteCurrency,
        @JsonProperty("display_pair") String displayPair,
        @JsonProperty("display_unit") String displayUnit,
        @JsonProperty("value_format") String valueFormat,
        @JsonProperty("chart_title") String chartTitle,
        @JsonProperty("chart_subtitle") String chartSubtitle,
        @JsonProperty("axis_label") String axisLabel,
        @JsonProperty("tooltip_label") String tooltipLabel
) {
}
