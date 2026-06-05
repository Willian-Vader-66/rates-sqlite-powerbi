package com.example.financedashboard.ops;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

public final class SecretRedactor {
    private static final Pattern ENV_ASSIGNMENT = Pattern.compile(
            "(?i)(TWELVE_DATA_API_KEY|COINGECKO_DEMO_API_KEY|COINGECKO_PRO_API_KEY|FRED_API_KEY|API_KEY|TOKEN|SECRET)\\s*=\\s*([^\\s;&]+)"
    );
    private static final Pattern BEARER_TOKEN = Pattern.compile("(?i)Bearer\\s+([A-Za-z0-9._\\-]{12,})");
    private static final Pattern LONG_TOKEN = Pattern.compile("\\b(?=[A-Za-z0-9_\\-]*\\d)[A-Za-z0-9_\\-]{24,}\\b");

    private final List<String> exactSecrets;

    public SecretRedactor(Collection<String> exactSecrets) {
        Set<String> unique = new LinkedHashSet<>();
        if (exactSecrets != null) {
            for (String secret : exactSecrets) {
                if (secret != null && !secret.isBlank()) {
                    unique.add(secret.trim());
                }
            }
        }
        this.exactSecrets = List.copyOf(unique);
    }

    public static SecretRedactor empty() {
        return new SecretRedactor(List.of());
    }

    public static SecretRedactor fromSessionSecrets(Map<String, String> sessionSecrets) {
        if (sessionSecrets == null || sessionSecrets.isEmpty()) {
            return empty();
        }
        return new SecretRedactor(sessionSecrets.values());
    }

    public String redact(String text) {
        if (text == null || text.isEmpty()) {
            return "";
        }
        String redacted = text;
        redacted = ENV_ASSIGNMENT.matcher(redacted).replaceAll("$1=<redacted>");
        redacted = BEARER_TOKEN.matcher(redacted).replaceAll("Bearer <redacted>");
        for (String secret : exactSecrets) {
            redacted = redacted.replace(secret, maskExact(secret));
        }
        redacted = LONG_TOKEN.matcher(redacted).replaceAll("<redacted-token>");
        return redacted;
    }

    public String redactLines(Collection<String> lines) {
        List<String> redacted = new ArrayList<>();
        if (lines != null) {
            for (String line : lines) {
                redacted.add(redact(line));
            }
        }
        return String.join(System.lineSeparator(), redacted);
    }

    public static String maskedPreview(String value) {
        if (value == null || value.isBlank()) {
            return "-";
        }
        String trimmed = value.trim();
        int prefix = Math.min(4, trimmed.length());
        return trimmed.substring(0, prefix) + "****";
    }

    private static String maskExact(String secret) {
        Objects.requireNonNull(secret, "secret");
        return SecretRedactor.maskedPreview(secret);
    }
}
