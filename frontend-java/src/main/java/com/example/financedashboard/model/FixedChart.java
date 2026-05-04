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
        List<ChartPoint> points,
        String message
) {
}
