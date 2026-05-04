package com.example.financedashboard.api;

import com.example.financedashboard.config.AppConfig;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ApiClient {
    private final String apiBaseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Duration requestTimeout;

    public ApiClient(AppConfig config) {
        this(config.getApiBaseUrl(), HttpClient.newBuilder()
                .connectTimeout(config.getHttpTimeout())
                .build(), config.getHttpTimeout());
    }

    public ApiClient(String apiBaseUrl, HttpClient httpClient) {
        this(apiBaseUrl, httpClient, Duration.ofSeconds(30));
    }

    public ApiClient(String apiBaseUrl, HttpClient httpClient, Duration requestTimeout) {
        this.apiBaseUrl = stripTrailingSlash(apiBaseUrl);
        this.httpClient = httpClient;
        this.requestTimeout = requestTimeout == null ? Duration.ofSeconds(30) : requestTimeout;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public JsonNode getJson(String path, Map<String, String> queryParams) throws ApiException {
        URI uri = buildUri(path, queryParams);
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(requestTimeout)
                .GET()
                .build();
        try {
            if (Boolean.getBoolean("finance.api.debug")) {
                System.out.println("API GET " + uri);
            }
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new ApiException(response.statusCode(), "Backend returned HTTP " + response.statusCode() + " for " + uri);
            }
            return objectMapper.readTree(response.body());
        } catch (IOException ex) {
            throw new ApiException("Backend API is not available for " + uri + " (" + ex.getClass().getSimpleName() + ": " + ex.getMessage() + ")", ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ApiException("Request interrupted for " + uri, ex);
        }
    }

    public <T> T get(String path, Map<String, String> queryParams, Class<T> responseType) throws ApiException {
        JsonNode json = getJson(path, queryParams);
        try {
            return objectMapper.treeToValue(json, responseType);
        } catch (IOException ex) {
            throw new ApiException("Could not parse backend response from " + path, ex);
        }
    }

    public <T> List<T> getItems(String path, Map<String, String> queryParams, TypeReference<List<T>> itemListType) throws ApiException {
        JsonNode json = getJson(path, queryParams);
        JsonNode items = json.path("items");
        if (!items.isArray()) {
            return List.of();
        }
        try {
            return objectMapper.convertValue(items, itemListType);
        } catch (IllegalArgumentException ex) {
            throw new ApiException("Could not parse backend items from " + path, ex);
        }
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public URI buildUri(String path, Map<String, String> queryParams) {
        String normalizedPath = path.startsWith("/") ? path : "/" + path;
        StringBuilder builder = new StringBuilder(apiBaseUrl).append(normalizedPath);
        List<String> parts = new ArrayList<>();
        queryParams.forEach((key, value) -> {
            if (value != null && !value.isBlank()) {
                parts.add(encode(key) + "=" + encode(value));
            }
        });
        if (!parts.isEmpty()) {
            builder.append("?").append(String.join("&", parts));
        }
        return URI.create(builder.toString());
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static String stripTrailingSlash(String value) {
        String result = value == null || value.isBlank() ? AppConfig.DEFAULT_API_BASE_URL : value.trim();
        while (result.endsWith("/") && result.length() > 1) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }
}
