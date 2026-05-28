package com.example.financedashboard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

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
        @JsonProperty("data_mode") String dataMode,
        @JsonProperty("requested_days") Integer requestedDays,
        @JsonProperty("history_mode") String historyMode,
        @JsonProperty("advanced_history_available") Boolean advancedHistoryAvailable,
        @JsonProperty("advanced_history_enabled") Boolean advancedHistoryEnabled,
        @JsonProperty("advanced_history_max_years") Integer advancedHistoryMaxYears,
        List<String> providers,
        @JsonProperty("provider_summary") List<ProviderSummary> providerSummary,
        Coverage coverage,
        @JsonProperty("data_mode_counts") DataModeCounts dataModeCounts,
        @JsonProperty("data_health") DataHealth dataHealth,
        @JsonProperty("live_provider_status") LiveProviderStatus liveProviderStatus,
        @JsonProperty("data_generated_at") String dataGeneratedAt,
        @JsonProperty("data_warning") String dataWarning,
        String message
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ProviderSummary(
            @JsonProperty("asset_type") String assetType,
            List<String> providers
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Coverage(
            @JsonProperty("date_min") String dateMin,
            @JsonProperty("date_max") String dateMax,
            @JsonProperty("historical_rows") Integer historicalRows
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record DataModeCounts(
            Integer demo,
            Integer live,
            Integer mixed,
            Integer unknown
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record LiveProviderStatus(
            List<ProviderItem> providers,
            @JsonProperty("all_configured") Boolean allConfigured,
            String recommendation
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record DataHealth(
            String status,
            @JsonProperty("missing_important_symbols") List<String> missingImportantSymbols,
            @JsonProperty("symbols_without_history") List<String> symbolsWithoutHistory,
            @JsonProperty("symbols_without_quote") List<String> symbolsWithoutQuote,
            @JsonProperty("repair_command") String repairCommand
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ProviderItem(
            @JsonProperty("asset_type") String assetType,
            String provider,
            Boolean configured,
            Boolean available,
            @JsonProperty("missing_env") List<String> missingEnv,
            @JsonProperty("supported_symbols") List<String> supportedSymbols,
            String message
    ) {
    }
}
