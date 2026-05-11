package com.example.financedashboard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Instrument(
        @JsonProperty("instrument_id") Long instrumentId,
        String symbol,
        String name,
        @JsonProperty("asset_type") String assetType,
        String exchange,
        String currency,
        String sector,
        String provider,
        @JsonProperty("provider_symbol") String providerSymbol,
        @JsonProperty("data_mode") String dataMode,
        @JsonProperty("data_warning") String dataWarning,
        @JsonProperty("is_demo") Boolean demo,
        @JsonProperty("is_live") Boolean live,
        @JsonProperty("is_active") Integer isActive,
        Integer priority,
        @JsonProperty("created_at") String createdAt,
        @JsonProperty("updated_at") String updatedAt,
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
    public boolean active() {
        return isActive == null || isActive == 1;
    }
}
