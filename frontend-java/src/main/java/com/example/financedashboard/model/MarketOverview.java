package com.example.financedashboard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record MarketOverview(
        @JsonProperty("generated_at") String generatedAt,
        List<DashboardCard> cards,
        List<MarketSignal> signals,
        String message
) {
}
