package com.example.financedashboard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record TopStockPerformance(
        String symbol,
        String name,
        @JsonProperty("latest_price") Double latestPrice,
        @JsonProperty("start_price") Double startPrice,
        @JsonProperty("change_30d") Double change30d,
        String trend,
        String signal,
        List<ChartPoint> points
) {
}
