package com.example.financedashboard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PricePoint(
        String date,
        String symbol,
        String base,
        String exchange,
        Double rate,
        Double open,
        Double high,
        Double low,
        Double close,
        @JsonProperty("adjusted_close") Double adjustedClose,
        Long volume,
        String currency,
        String provider,
        String source,
        @JsonProperty("fetched_at") String fetchedAt
) {
    public Double displayValue() {
        return close != null ? close : rate;
    }
}
