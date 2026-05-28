package com.example.financedashboard;

import com.example.financedashboard.api.ApiClient;
import com.example.financedashboard.api.ApiException;
import com.example.financedashboard.service.MarketDataService;
import com.example.financedashboard.service.MarketDataService.HistoryRange;
import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MarketDataServiceTest {
    @Test
    void standardLiveRangesCalculateStartDate() {
        assertEquals(LocalDate.of(2026, 4, 26), HistoryRange.SEVEN_D.startDate(LocalDate.of(2026, 5, 3)));
        assertEquals(LocalDate.of(2026, 2, 2), HistoryRange.NINETY_D.startDate(LocalDate.of(2026, 5, 3)));
        assertEquals(LocalDate.of(2025, 11, 4), HistoryRange.ONE_EIGHTY_D.startDate(LocalDate.of(2026, 5, 3)));
        assertEquals(LocalDate.of(2025, 5, 3), HistoryRange.ONE_Y.startDate(LocalDate.of(2026, 5, 3)));
        assertEquals(false, HistoryRange.THREE_Y.enabled());
    }

    @Test
    void historyDispatchesToEndpointForAssetType() throws Exception {
        CapturingApiClient client = new CapturingApiClient();
        MarketDataService service = new MarketDataService(client);
        LocalDate end = LocalDate.of(2026, 5, 3);

        service.getHistory("STOCK", "AAPL", null, end, HistoryRange.ONE_Y);
        assertEquals("/api/history/AAPL", client.path);
        assertEquals("STOCK", client.params.get("asset_type"));
        assertEquals("365D", client.params.get("period"));

        service.getHistory("FX", "EUR", "USD", end, HistoryRange.ONE_Y);
        assertEquals("/api/history/EUR", client.path);
        assertEquals("FX", client.params.get("asset_type"));
        assertEquals("USD", client.params.get("base"));

        service.getHistory("CRYPTO", "BTC", null, end, HistoryRange.ONE_Y);
        assertEquals("/api/history/BTC", client.path);
        assertEquals("CRYPTO", client.params.get("asset_type"));

        service.getHistory("MACRO", "SELIC_DAILY", null, end, HistoryRange.ONE_Y);
        assertEquals("/api/history/SELIC_DAILY", client.path);
        assertEquals("MACRO", client.params.get("asset_type"));
    }

    private static final class CapturingApiClient extends ApiClient {
        private String path;
        private Map<String, String> params;

        CapturingApiClient() {
            super("http://127.0.0.1:8000", HttpClient.newHttpClient(), Duration.ofSeconds(5));
        }

        @Override
        public <T> List<T> getItems(String path, Map<String, String> queryParams, TypeReference<List<T>> itemListType) throws ApiException {
            this.path = path;
            this.params = queryParams;
            return List.of();
        }
    }
}
