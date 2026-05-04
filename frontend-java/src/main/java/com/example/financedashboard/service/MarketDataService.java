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
        LocalDate start = range.startDate(end);
        if ("FX".equalsIgnoreCase(assetType)) {
            return getFxHistory(base == null || base.isBlank() ? "USD" : base, symbol, start, end);
        }
        if ("CRYPTO".equalsIgnoreCase(assetType)) {
            return getCryptoHistory(symbol, start, end);
        }
        if ("MACRO".equalsIgnoreCase(assetType)) {
            return getMacroHistory(symbol, start, end);
        }
        return getStockHistory(symbol, start, end);
    }

    public enum HistoryRange {
        THIRTY_D("30D"),
        NINETY_D("90D"),
        SIX_M("6M"),
        ONE_Y("1Y"),
        FOUR_Y("4Y");

        private final String label;

        HistoryRange(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }

        public LocalDate startDate(LocalDate end) {
            return switch (this) {
                case THIRTY_D -> end.minusDays(30);
                case NINETY_D -> end.minusDays(90);
                case SIX_M -> end.minusMonths(6);
                case ONE_Y -> end.minusYears(1);
                case FOUR_Y -> end.minusYears(4);
            };
        }

        public int days() {
            return switch (this) {
                case THIRTY_D -> 30;
                case NINETY_D -> 90;
                case SIX_M -> 183;
                case ONE_Y -> 365;
                case FOUR_Y -> 1460;
            };
        }
    }

    private static HistoryRange safeRange(HistoryRange range) {
        return range == null ? HistoryRange.NINETY_D : range;
    }
}
