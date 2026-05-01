package com.example.financedashboard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DashboardSummary(
        @JsonProperty("total_instruments") Integer totalInstruments,
        @JsonProperty("active_stocks") Integer activeStocks,
        @JsonProperty("active_currencies") Integer activeCurrencies,
        @JsonProperty("latest_quote_count") Integer latestQuoteCount,
        @JsonProperty("latest_analysis_count") Integer latestAnalysisCount,
        @JsonProperty("last_successful_ingest_run") IngestRun lastSuccessfulIngestRun,
        @JsonProperty("failed_runs_count") Integer failedRunsCount
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
