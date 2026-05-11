package com.example.financedashboard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DashboardSummary(
        @JsonProperty("total_instruments") Integer totalInstruments,
        @JsonProperty("active_stocks") Integer activeStocks,
        @JsonProperty("active_currencies") Integer activeCurrencies,
        @JsonProperty("active_crypto") Integer activeCrypto,
        @JsonProperty("active_macro") Integer activeMacro,
        @JsonProperty("latest_quote_count") Integer latestQuoteCount,
        @JsonProperty("latest_analysis_count") Integer latestAnalysisCount,
        @JsonProperty("instruments_without_analysis") Integer instrumentsWithoutAnalysis,
        @JsonProperty("instruments_without_quotes") Integer instrumentsWithoutQuotes,
        @JsonProperty("last_successful_ingest_run") IngestRun lastSuccessfulIngestRun,
        @JsonProperty("failed_runs_count") Integer failedRunsCount,
        @JsonProperty("data_mode") String dataMode,
        List<String> providers,
        @JsonProperty("generated_at") String generatedAt,
        String warning
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record IngestRun(
            @JsonProperty("run_id") Long runId,
            @JsonProperty("started_at") String startedAt,
            @JsonProperty("finished_at") String finishedAt,
            String mode,
            String base,
            String symbols,
            String start,
            String end,
            @JsonProperty("row_count") Integer rowCount,
            String status,
            String error
    ) {
    }
}
