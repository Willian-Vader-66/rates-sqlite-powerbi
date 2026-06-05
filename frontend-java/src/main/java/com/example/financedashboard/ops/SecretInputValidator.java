package com.example.financedashboard.ops;

import java.util.List;

public final class SecretInputValidator {
    private static final String COMMAND_MESSAGE =
            "Cole somente a chave da Twelve Data, sem aspas, sem comando e sem caminho.";
    private static final List<String> COMMAND_MARKERS = List.of(
            "$env:",
            "python",
            "powershell",
            "pwsh",
            " fx_rates",
            "fx_rates",
            "run_live_pipeline",
            "run_finance_monitor",
            "twleve_data_api_key=",
            "twelve_data_api_key=",
            "c:\\",
            " c:",
            "cd ",
            " cd",
            ";",
            "|",
            "&",
            "\"",
            "'",
            "`"
    );
    private static final List<String> PLACEHOLDERS = List.of(
            "none",
            "null",
            "sua_chave_aqui",
            "your_key",
            "your_api_key",
            "your_twelve_data_api_key",
            "change_me",
            "changeme",
            "todo",
            "test",
            "fake",
            "demo",
            "placeholder"
    );

    private SecretInputValidator() {
    }

    public static ValidationResult validateTwelveKey(String raw) {
        String value = raw == null ? "" : raw.trim();
        String lower = value.toLowerCase();
        if (value.isBlank()) {
            return ValidationResult.invalid("TWELVE_DATA_API_KEY is empty.");
        }
        if (value.contains("\n") || value.contains("\r")) {
            return ValidationResult.invalid(COMMAND_MESSAGE);
        }
        for (String marker : COMMAND_MARKERS) {
            if (lower.contains(marker)) {
                return ValidationResult.invalid(COMMAND_MESSAGE);
            }
        }
        if (value.split("\\s+").length > 1) {
            return ValidationResult.invalid(COMMAND_MESSAGE);
        }
        if (value.length() > 128) {
            return ValidationResult.invalid("TWELVE_DATA_API_KEY is too long. " + COMMAND_MESSAGE);
        }
        if (!value.matches("[A-Za-z0-9._-]+")) {
            return ValidationResult.invalid(COMMAND_MESSAGE);
        }
        for (String placeholder : PLACEHOLDERS) {
            if (lower.equals(placeholder) || lower.contains(placeholder)) {
                return ValidationResult.invalid("Paste a real Twelve Data key, not a placeholder.");
            }
        }
        if (value.length() < 12) {
            return ValidationResult.invalid("TWELVE_DATA_API_KEY is too short or placeholder-like.");
        }
        return ValidationResult.valid(value);
    }

    public record ValidationResult(boolean valid, String value, String message) {
        private static ValidationResult valid(String value) {
            return new ValidationResult(true, value, "Key accepted for this JavaFX session.");
        }

        private static ValidationResult invalid(String message) {
            return new ValidationResult(false, "", message);
        }
    }
}
