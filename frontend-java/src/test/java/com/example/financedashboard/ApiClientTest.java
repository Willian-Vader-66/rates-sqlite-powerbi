package com.example.financedashboard;

import com.example.financedashboard.api.ApiClient;
import com.example.financedashboard.config.AppConfig;
import com.example.financedashboard.model.DashboardSummary;
import com.example.financedashboard.model.FixedCharts;
import com.example.financedashboard.model.Instrument;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.net.http.HttpClient;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiClientTest {
    @Test
    void buildUriCombinesBasePathAndEncodedQueryParams() {
        ApiClient client = new ApiClient("http://example.test/", HttpClient.newHttpClient(), Duration.ofSeconds(30));

        String uri = client.buildUri("/api/instruments", Map.of(
                "asset_type", "STOCK",
                "search", "Apple Inc"
        )).toString();

        assertTrue(uri.startsWith("http://example.test/api/instruments?"));
        assertTrue(uri.contains("asset_type=STOCK"));
        assertTrue(uri.contains("search=Apple%20Inc"));
    }

    @Test
    void parsesDashboardSummaryAndInstrumentJson() throws Exception {
        ApiClient client = new ApiClient("http://127.0.0.1:8000", HttpClient.newHttpClient(), Duration.ofSeconds(30));
        JsonNode summaryJson = client.getObjectMapper().readTree("""
                {
                  "total_instruments": 10,
                  "active_stocks": 8,
                  "active_currencies": 2,
                  "latest_quote_count": 3,
                  "latest_analysis_count": 4,
                  "instruments_without_analysis": 1,
                  "instruments_without_quotes": 2,
                  "failed_runs_count": 1,
                  "last_successful_ingest_run": {"run_id": 5, "mode": "stocks_backfill", "row_count": 20}
                }
                """);
        DashboardSummary summary = client.getObjectMapper().treeToValue(summaryJson, DashboardSummary.class);

        JsonNode instrumentJson = client.getObjectMapper().readTree("""
                {"instrument_id": 1, "symbol": "AAPL", "name": "Apple Inc", "asset_type": "STOCK", "is_active": 1}
                """);
        Instrument instrument = client.getObjectMapper().treeToValue(instrumentJson, Instrument.class);

        assertEquals(10, summary.totalInstruments());
        assertEquals(1, summary.instrumentsWithoutAnalysis());
        assertEquals(2, summary.instrumentsWithoutQuotes());
        assertEquals("stocks_backfill", summary.lastSuccessfulIngestRun().mode());
        assertEquals("AAPL", instrument.symbol());
        assertTrue(instrument.active());
    }

    @Test
    void parsesFixedChartsWithEmptyDataMessage() throws Exception {
        ApiClient client = new ApiClient("http://127.0.0.1:8000", HttpClient.newHttpClient(), Duration.ofSeconds(30));
        JsonNode fixedChartsJson = client.getObjectMapper().readTree("""
                {
                  "fx": [
                    {
                      "id": "usd_eur_30d",
                      "title": "USD/EUR - Last 30 Days",
                      "asset_type": "FX",
                      "base": "USD",
                      "symbol": "EUR",
                      "points": [],
                      "message": "No data loaded. Run: python -m fx_rates dashboard prepare-demo --years 4 --demo"
                    }
                  ],
                  "crypto": [],
                  "macro": []
                }
                """);

        FixedCharts fixedCharts = client.getObjectMapper().treeToValue(fixedChartsJson, FixedCharts.class);

        assertEquals("usd_eur_30d", fixedCharts.fx().get(0).id());
        assertTrue(fixedCharts.fx().get(0).message().startsWith("No data loaded."));
    }

    @Test
    void appConfigUsesEnvironmentStyleOverrideWhenProvided() {
        Properties properties = new Properties();
        properties.setProperty("api.base.url", "http://127.0.0.1:8000");
        properties.setProperty("refresh.interval.seconds", "30");

        AppConfig config = AppConfig.fromProperties(properties, "https://finance.example.com");

        assertEquals("https://finance.example.com", config.getApiBaseUrl());
        assertEquals(30, config.getRefreshIntervalSeconds());
    }

    @Test
    void apiClientKeepsConfiguredTimeout() {
        ApiClient client = new ApiClient("http://127.0.0.1:8000", HttpClient.newHttpClient(), Duration.ofSeconds(45));

        assertEquals(Duration.ofSeconds(45), client.getRequestTimeout());
    }
}
