package com.example.financedashboard;

import com.example.financedashboard.config.AppConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AppConfigTest {
    @Test
    void readsDefaultValuesFromEmptyProperties() {
        AppConfig config = AppConfig.fromProperties(new Properties(), null);

        assertEquals("http://127.0.0.1:8000", config.getApiBaseUrl());
        assertEquals(30, config.getRefreshIntervalSeconds());
        assertEquals(Duration.ofSeconds(30), config.getHttpTimeout());
    }

    @Test
    void readsRefreshAndTimeoutOverrides() {
        Properties properties = new Properties();
        properties.setProperty("refresh.interval.seconds", "45");
        properties.setProperty("http.timeout.seconds", "60");

        AppConfig config = AppConfig.fromProperties(properties, null);

        assertEquals(45, config.getRefreshIntervalSeconds());
        assertEquals(60, config.getHttpTimeoutSeconds());
    }
}
