package com.example.financedashboard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record FixedChart(
        String id,
        String title,
        @JsonProperty("asset_type") String assetType,
        String base,
        String symbol,
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
        String period,
        @JsonProperty("start_date") String startDate,
        @JsonProperty("end_date") String endDate,
        @JsonProperty("point_count") Integer pointCount,
        List<ChartPoint> points,
        String message
) {
}
