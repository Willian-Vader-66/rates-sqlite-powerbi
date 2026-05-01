package com.example.financedashboard.service;

import com.example.financedashboard.api.ApiClient;
import com.example.financedashboard.api.ApiException;
import com.example.financedashboard.model.AnalysisSnapshot;
import com.example.financedashboard.model.DashboardSummary;
import com.example.financedashboard.model.Instrument;
import com.example.financedashboard.model.PricePoint;
import com.example.financedashboard.model.Quote;
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
}
