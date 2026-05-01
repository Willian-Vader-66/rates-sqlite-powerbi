package com.example.financedashboard.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;

public final class AppConfig {
    public static final String DEFAULT_API_BASE_URL = "http://127.0.0.1:8000";
    private static final String ENV_API_BASE_URL = "FINANCE_API_BASE_URL";

    private final String apiBaseUrl;
    private final int refreshIntervalSeconds;
    private final int httpTimeoutSeconds;
    private final int historyLookbackDays;

    private AppConfig(String apiBaseUrl, int refreshIntervalSeconds, int httpTimeoutSeconds, int historyLookbackDays) {
        this.apiBaseUrl = stripTrailingSlash(apiBaseUrl);
        this.refreshIntervalSeconds = Math.max(5, refreshIntervalSeconds);
        this.httpTimeoutSeconds = Math.max(5, httpTimeoutSeconds);
        this.historyLookbackDays = Math.max(7, historyLookbackDays);
    }

    public static AppConfig load() {
        Properties properties = new Properties();
        loadClasspathDefaults(properties);
        loadExternalOverrides(properties);

        String envUrl = System.getenv(ENV_API_BASE_URL);
        String configuredUrl = isBlank(envUrl) ? properties.getProperty("api.base.url", DEFAULT_API_BASE_URL) : envUrl;

        return new AppConfig(
                configuredUrl,
                parseInt(properties.getProperty("refresh.interval.seconds"), 30),
                parseInt(properties.getProperty("http.timeout.seconds"), 30),
                parseInt(properties.getProperty("history.lookback.days"), 120)
        );
    }

    public String getApiBaseUrl() {
        return apiBaseUrl;
    }

    public int getRefreshIntervalSeconds() {
        return refreshIntervalSeconds;
    }

    public Duration getRefreshInterval() {
        return Duration.ofSeconds(refreshIntervalSeconds);
    }

    public int getHttpTimeoutSeconds() {
        return httpTimeoutSeconds;
    }

    public Duration getHttpTimeout() {
        return Duration.ofSeconds(httpTimeoutSeconds);
    }

    public int getHistoryLookbackDays() {
        return historyLookbackDays;
    }

    public static AppConfig fromProperties(Properties properties, String envApiBaseUrl) {
        String configuredUrl = isBlank(envApiBaseUrl)
                ? properties.getProperty("api.base.url", DEFAULT_API_BASE_URL)
                : envApiBaseUrl;
        return new AppConfig(
                configuredUrl,
                parseInt(properties.getProperty("refresh.interval.seconds"), 30),
                parseInt(properties.getProperty("http.timeout.seconds"), 30),
                parseInt(properties.getProperty("history.lookback.days"), 120)
        );
    }

    private static void loadClasspathDefaults(Properties properties) {
        try (InputStream input = AppConfig.class.getResourceAsStream("/application.properties")) {
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException ex) {
            System.err.println("Could not load classpath application.properties: " + ex.getMessage());
        }
    }

    private static void loadExternalOverrides(Properties properties) {
        Path localConfig = Path.of("config", "application.properties");
        if (!Files.exists(localConfig)) {
            return;
        }
        try (InputStream input = Files.newInputStream(localConfig)) {
            properties.load(input);
        } catch (IOException ex) {
            System.err.println("Could not load config/application.properties: " + ex.getMessage());
        }
    }

    private static int parseInt(String raw, int fallback) {
        if (isBlank(raw)) {
            return fallback;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException ex) {
            return fallback;
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String stripTrailingSlash(String value) {
        String trimmed = isBlank(value) ? DEFAULT_API_BASE_URL : value.trim();
        while (trimmed.endsWith("/") && trimmed.length() > 1) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }
}
