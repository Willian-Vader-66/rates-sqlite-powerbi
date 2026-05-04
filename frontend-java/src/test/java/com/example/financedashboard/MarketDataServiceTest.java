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
    void fourYearRangeCalculatesStartDate() {
        assertEquals(LocalDate.of(2022, 5, 3), HistoryRange.FOUR_Y.startDate(LocalDate.of(2026, 5, 3)));
    }

    @Test
    void historyDispatchesToEndpointForAssetType() throws Exception {
        CapturingApiClient client = new CapturingApiClient();
        MarketDataService service = new MarketDataService(client);
        LocalDate end = LocalDate.of(2026, 5, 3);

        service.getHistory("STOCK", "AAPL", null, end, HistoryRange.FOUR_Y);
        assertEquals("/api/stocks/history", client.path);
        assertEquals("AAPL", client.params.get("symbol"));
        assertEquals("2022-05-03", client.params.get("start"));

        service.getHistory("FX", "EUR", "USD", end, HistoryRange.FOUR_Y);
        assertEquals("/api/fx/history", client.path);
        assertEquals("USD", client.params.get("base"));
        assertEquals("EUR", client.params.get("symbol"));

        service.getHistory("CRYPTO", "BTC", null, end, HistoryRange.FOUR_Y);
        assertEquals("/api/crypto/history", client.path);
        assertEquals("BTC", client.params.get("symbol"));

        service.getHistory("MACRO", "SELIC_DAILY", null, end, HistoryRange.FOUR_Y);
        assertEquals("/api/macro/history", client.path);
        assertEquals("SELIC_DAILY", client.params.get("indicator_code"));
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
