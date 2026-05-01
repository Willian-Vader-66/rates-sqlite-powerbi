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
        @JsonProperty("is_active") Integer isActive,
        Integer priority,
        @JsonProperty("created_at") String createdAt,
        @JsonProperty("updated_at") String updatedAt
) {
    public boolean active() {
        return isActive == null || isActive == 1;
    }
}
