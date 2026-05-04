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
        Double value,
        @JsonProperty("price_usd") Double priceUsd,
        Double open,
        Double high,
        Double low,
        Double close,
        @JsonProperty("adjusted_close") Double adjustedClose,
        Long volume,
        String currency,
        String provider,
        String source,
        @JsonProperty("fetched_at") String fetchedAt,
        @JsonProperty("display_name") String displayName,
        @JsonProperty("asset_type") String assetType,
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
    public Double displayValue() {
        if (close != null) {
            return close;
        }
        if (rate != null) {
            return rate;
        }
        if (priceUsd != null) {
            return priceUsd;
        }
        return value;
    }
}
