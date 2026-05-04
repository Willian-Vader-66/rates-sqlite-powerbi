package com.example.financedashboard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record SystemStatus(
        @JsonProperty("db_path") String dbPath,
        @JsonProperty("db_exists") Boolean dbExists,
        @JsonProperty("db_size_bytes") Long dbSizeBytes,
        @JsonProperty("total_instruments") Integer totalInstruments,
        @JsonProperty("active_stocks") Integer activeStocks,
        @JsonProperty("active_currencies") Integer activeCurrencies,
        @JsonProperty("active_crypto") Integer activeCrypto,
        @JsonProperty("active_macro") Integer activeMacro,
        @JsonProperty("latest_quote_count") Integer latestQuoteCount,
        @JsonProperty("latest_analysis_count") Integer latestAnalysisCount,
        @JsonProperty("historical_row_count") Integer historicalRowCount,
        @JsonProperty("date_min") String dateMin,
        @JsonProperty("date_max") String dateMax,
        @JsonProperty("is_empty") Boolean empty,
        @JsonProperty("recommended_prepare_command") String recommendedPrepareCommand,
        String message
) {
}
