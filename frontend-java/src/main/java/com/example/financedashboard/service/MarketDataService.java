package com.example.financedashboard.service;

import com.example.financedashboard.api.ApiClient;
import com.example.financedashboard.api.ApiException;
import com.example.financedashboard.model.AnalysisSnapshot;
import com.example.financedashboard.model.DashboardSummary;
import com.example.financedashboard.model.FixedCharts;
import com.example.financedashboard.model.Instrument;
import com.example.financedashboard.model.MarketOverview;
import com.example.financedashboard.model.PricePoint;
import com.example.financedashboard.model.Quote;
import com.example.financedashboard.model.SystemStatus;
import com.example.financedashboard.model.TopStockPerformance;
import com.fasterxml.jackson.core.type.TypeReference;

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MarketDataService {
    private final ApiClient apiClient;

    public MarketDataService(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    public DashboardSummary getDashboardSummary() throws ApiException {
        return apiClient.get("/api/dashboard/summary", Map.of(), DashboardSummary.class);
    }

    public SystemStatus getSystemStatus() throws ApiException {
        return apiClient.get("/api/system/status", Map.of(), SystemStatus.class);
    }

    public MarketOverview getMarketOverview() throws ApiException {
        return apiClient.get("/api/dashboard/market-overview", Map.of(), MarketOverview.class);
    }

    public MarketOverview getMarketOverview(HistoryRange range) throws ApiException {
        return apiClient.get("/api/dashboard/market-overview", Map.of("period", safeRange(range).label()), MarketOverview.class);
    }

    public FixedCharts getFixedCharts() throws ApiException {
        return apiClient.get("/api/dashboard/fixed-charts", Map.of(), FixedCharts.class);
    }

    public FixedCharts getFixedCharts(HistoryRange range) throws ApiException {
        return apiClient.get("/api/dashboard/fixed-charts", Map.of("period", safeRange(range).label()), FixedCharts.class);
    }

    public List<TopStockPerformance> getTopStocks30d() throws ApiException {
        return apiClient.getItems("/api/dashboard/top-stocks-30d", Map.of(), new TypeReference<>() {});
    }

    public List<TopStockPerformance> getTopStocks(HistoryRange range) throws ApiException {
        return apiClient.getItems(
                "/api/dashboard/top-stocks-30d",
                Map.of("days", Integer.toString(safeRange(range).days())),
                new TypeReference<>() {}
        );
    }

    public List<Instrument> getInstruments(String assetType, Boolean active, String search) throws ApiException {
        Map<String, String> params = new LinkedHashMap<>();
        if (assetType != null && !"ALL".equalsIgnoreCase(assetType)) {
            params.put("asset_type", assetType);
        }
        if (active != null) {
            params.put("active", active.toString());
        }
        if (search != null && !search.isBlank()) {
            params.put("search", search.trim());
        }
        return apiClient.getItems("/api/instruments", params, new TypeReference<>() {});
    }

    public List<Quote> getLatestQuotes() throws ApiException {
        return apiClient.getItems("/api/quotes/latest", Map.of(), new TypeReference<>() {});
    }

    public List<AnalysisSnapshot> getLatestAnalysis() throws ApiException {
        return apiClient.getItems("/api/analysis/latest", Map.of(), new TypeReference<>() {});
    }

    public List<PricePoint> getStockHistory(String symbol, LocalDate start, LocalDate end) throws ApiException {
        return apiClient.getItems(
                "/api/stocks/history",
                Map.of("symbol", symbol, "start", start.toString(), "end", end.toString()),
                new TypeReference<>() {}
        );
    }

    public List<PricePoint> getFxHistory(String base, String symbol, LocalDate start, LocalDate end) throws ApiException {
        return apiClient.getItems(
                "/api/fx/history",
                Map.of("base", base, "symbol", symbol, "start", start.toString(), "end", end.toString()),
                new TypeReference<>() {}
        );
    }

    public List<PricePoint> getCryptoHistory(String symbol, LocalDate start, LocalDate end) throws ApiException {
        return apiClient.getItems(
                "/api/crypto/history",
                Map.of("symbol", symbol, "start", start.toString(), "end", end.toString()),
                new TypeReference<>() {}
        );
    }

    public List<PricePoint> getMacroHistory(String indicatorCode, LocalDate start, LocalDate end) throws ApiException {
        return apiClient.getItems(
                "/api/macro/history",
                Map.of("indicator_code", indicatorCode, "start", start.toString(), "end", end.toString()),
                new TypeReference<>() {}
        );
    }

    public List<PricePoint> getHistory(String assetType, String symbol, String base, LocalDate end, HistoryRange range) throws ApiException {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("period", range.label());
        if (assetType != null && !assetType.isBlank()) {
            params.put("asset_type", assetType);
        }
        if (base != null && !base.isBlank()) {
            params.put("base", base);
        }
        return apiClient.getItems(
                "/api/history/" + symbol,
                params,
                new TypeReference<>() {}
        );
    }

    public enum HistoryRange {
        SEVEN_D("7D", 7, true),
        THIRTY_D("30D"),
        NINETY_D("90D"),
        ONE_EIGHTY_D("180D", 180, true),
        ONE_Y("365D", 365, true),
        THREE_Y("3Y", 1095, false),
        FIVE_Y("5Y", 1825, false),
        TEN_Y("10Y", 3650, false);

        private final String label;
        private final int days;
        private final boolean enabled;

        HistoryRange(String label) {
            this(label, switch (label) {
                case "30D" -> 30;
                case "90D" -> 90;
                default -> 365;
            }, true);
        }

        HistoryRange(String label, int days, boolean enabled) {
            this.label = label;
            this.days = days;
            this.enabled = enabled;
        }

        public String label() {
            return label;
        }

        public LocalDate startDate(LocalDate end) {
            return end.minusDays(days);
        }

        public int days() {
            return days;
        }

        public boolean enabled() {
            return enabled;
        }

        public String disabledTooltip() {
            return enabled ? "" : "Requires advanced history provider.";
        }
    }

    private static HistoryRange safeRange(HistoryRange range) {
        return range == null ? HistoryRange.NINETY_D : range;
    }
}
