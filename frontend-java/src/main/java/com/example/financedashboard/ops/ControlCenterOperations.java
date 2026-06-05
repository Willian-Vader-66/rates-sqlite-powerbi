package com.example.financedashboard.ops;

import java.time.Duration;
import java.util.List;

public final class ControlCenterOperations {
    public static final String CANDIDATE_DB = ".tmp/live-main-candidate.sqlite";
    public static final String MAIN_DB = "data/fx.sqlite";

    private ControlCenterOperations() {
    }

    public static List<PipelineStepDefinition> live365PipelineSteps() {
        return List.of(
                new PipelineStepDefinition(
                        1,
                        "Provider Validation",
                        List.of("providers", "status", "--external-test"),
                        false,
                        false,
                        false,
                        Duration.ofMinutes(5)
                ),
                new PipelineStepDefinition(
                        2,
                        "Crypto History 365D",
                        List.of("crypto", "test-history", "--symbols", "BTC,ETH,BNB,SOL,XRP", "--days", "365"),
                        false,
                        false,
                        false,
                        Duration.ofMinutes(5)
                ),
                new PipelineStepDefinition(
                        3,
                        "Build Live Candidate DB",
                        List.of("dashboard", "build-live-db", "--days", "365", "--db-path", CANDIDATE_DB, "--external-test"),
                        true,
                        false,
                        false,
                        Duration.ofMinutes(45)
                ),
                new PipelineStepDefinition(
                        4,
                        "Validate Samples",
                        List.of("dashboard", "validate-samples", "--db-path", CANDIDATE_DB, "--samples-per-symbol", "5", "--external-test"),
                        true,
                        false,
                        false,
                        Duration.ofMinutes(15)
                ),
                new PipelineStepDefinition(
                        5,
                        "Audit Live",
                        List.of("dashboard", "audit-live", "--db-path", CANDIDATE_DB),
                        false,
                        false,
                        false,
                        Duration.ofMinutes(5)
                ),
                new PipelineStepDefinition(
                        6,
                        "API Smoke Live",
                        List.of("api", "smoke-live", "--db-path", CANDIDATE_DB, "--port", "8001"),
                        false,
                        false,
                        false,
                        Duration.ofMinutes(10)
                ),
                new PipelineStepDefinition(
                        7,
                        "Promote Dry Run",
                        List.of("dashboard", "promote-live", "--candidate-db", CANDIDATE_DB, "--dry-run"),
                        true,
                        false,
                        false,
                        Duration.ofMinutes(15)
                ),
                new PipelineStepDefinition(
                        8,
                        "Promote to Main Database",
                        List.of("dashboard", "promote-live", "--candidate-db", CANDIDATE_DB, "--backup"),
                        true,
                        true,
                        true,
                        Duration.ofMinutes(20)
                )
        );
    }

    public static List<PipelineStepDefinition> validationOnlySteps() {
        return live365PipelineSteps().stream()
                .filter(step -> !step.promoteStep())
                .toList();
    }

    public static List<String> backendServeArguments(String host, int port) {
        return List.of("serve", "--host", host, "--port", Integer.toString(port));
    }

    public static List<String> prepareDemoArguments() {
        return List.of("dashboard", "prepare-demo", "--days", "365", "--demo");
    }

    public static List<String> auditMainArguments() {
        return List.of("dashboard", "audit-live", "--db-path", MAIN_DB);
    }

    public static List<String> auditCandidateArguments() {
        return List.of("dashboard", "audit-live", "--db-path", CANDIDATE_DB);
    }

    public static List<String> providerValidationArguments() {
        return List.of("providers", "status", "--external-test");
    }

    public static String dataModeLabel(String mode) {
        if (mode == null || mode.isBlank()) {
            return "UNKNOWN";
        }
        return switch (mode.trim().toLowerCase()) {
            case "demo" -> "DEMO";
            case "live" -> "LIVE 365D";
            case "mixed" -> "MIXED";
            default -> "UNKNOWN";
        };
    }

    public static String advancedHistoryLabel() {
        return "Advanced History 3Y / 5Y / 10Y is future-only and requires paid provider support.";
    }

    public static String twelveEnvironmentSummary(String value) {
        SecretInputValidator.ValidationResult validation = SecretInputValidator.validateTwelveKey(value);
        boolean present = value != null && !value.isBlank();
        String preview = validation.valid() ? SecretRedactor.maskedPreview(value) : "-";
        int length = value == null ? 0 : value.trim().length();
        return "key_present=%s source=session environment key_valid_format=%s key_length=%s masked_preview=%s".formatted(
                Boolean.toString(present),
                Boolean.toString(validation.valid()),
                length,
                preview
        );
    }

    public static boolean providerOutputShowsStockPass(String output) {
        for (String rawLine : safeLines(output)) {
            String line = rawLine.trim().toLowerCase();
            if (line.startsWith("stock:")) {
                return line.contains("provider=twelvedata")
                        && line.contains("status=configured")
                        && line.contains("available=true")
                        && line.contains("external_test=pass");
            }
        }
        return false;
    }

    public static boolean providerOutputShowsMissingTwelveKey(String output) {
        String lower = output == null ? "" : output.toLowerCase();
        return lower.contains("missing_env=twelve_data_api_key")
                || lower.contains("twelve_data_api_key not set")
                || lower.contains("provider_key_missing")
                || lower.contains("twelve_data_api_key missing");
    }

    public static boolean providerOutputShowsExternalPass(String output) {
        boolean sawProvider = false;
        for (String rawLine : safeLines(output)) {
            String line = rawLine.trim().toLowerCase();
            if (!line.matches("^(fx|crypto|stock|macro):.*")) {
                continue;
            }
            sawProvider = true;
            if (line.contains("external_test=fail")
                    || line.contains("status=not_configured")
                    || line.contains("available=false")
                    || line.contains("missing_env=twelve_data_api_key")) {
                return false;
            }
        }
        return sawProvider && providerOutputShowsStockPass(output);
    }

    public static String sampleValidationSummary(String output) {
        String text = output == null ? "" : output;
        String status = firstMatchingLine(text, "LIVE SAMPLE VALIDATION STATUS:");
        String samples = firstMatchingLine(text, "Samples:");
        String providerFailures = firstMatchingLine(text, "Provider failures:");
        String rateLimit = firstMatchingLine(text, "Rate limit detected:");
        String report = firstMatchingLine(text, "Report:");
        String reason = firstMatchingLine(text, "Reason:");
        String internalFail = firstLineAfterSection(text, "INTERNAL SAMPLE VALIDATION:", "FAIL:");
        String externalFail = firstLineAfterSection(text, "EXTERNAL PROVIDER SAMPLE VALIDATION:", "FAIL:");
        String releaseGate = firstMatchingLine(text, "release_gate:");
        String promotionAllowed = firstMatchingLine(text, "promotion_allowed:");
        String reasonCodes = firstMatchingLine(text, "reason_codes:");
        StringBuilder builder = new StringBuilder("Step 4 summary");
        if (!status.isBlank()) {
            builder.append(System.lineSeparator()).append(status);
        }
        builder.append(System.lineSeparator()).append(samples.isBlank() ? "Samples OK/WARN/FAIL: unavailable" : samples);
        builder.append(System.lineSeparator()).append(providerFailures.isBlank() ? "Provider failures: unavailable" : providerFailures);
        builder.append(System.lineSeparator()).append(rateLimit.isBlank() ? "Rate limit detected: false" : rateLimit);
        if (text.toLowerCase().contains("rate limit detected: true") || text.toLowerCase().contains("external_rate_limit")) {
            builder.append(System.lineSeparator()).append("Validação externa limitada pelo provider. O banco candidato passou nas auditorias internas, mas a confirmação por amostragem externa ficou parcial.");
        }
        builder.append(System.lineSeparator()).append("Internal validation: ").append(isZeroCountLine(internalFail) ? "Passed" : "Failed");
        builder.append(System.lineSeparator()).append("External validation: ").append(externalValidationLabel(text, externalFail));
        builder.append(System.lineSeparator()).append("Promotion gate: ").append(promotionGateLabel(releaseGate, promotionAllowed));
        if (!reason.isBlank()) {
            builder.append(System.lineSeparator()).append(reason);
        }
        if (!reasonCodes.isBlank()) {
            builder.append(System.lineSeparator()).append(reasonCodes);
        }
        builder.append(System.lineSeparator()).append(report.isBlank() ? "Report: docs/LIVE_SAMPLE_VALIDATION_REPORT.md" : report);
        return builder.toString();
    }

    public static String auditLiveSummary(String output) {
        String text = output == null ? "" : output;
        String status = firstMatchingLine(text, "LIVE AUDIT STATUS:");
        String critical = firstMatchingLine(text, "Critical failures:");
        String warnings = firstMatchingLine(text, "Warnings:");
        String monthlyNote = firstMatchingLine(text, "note: IPCA is a monthly macro series");
        StringBuilder builder = new StringBuilder("Step 5 summary");
        if (!status.isBlank()) {
            builder.append(System.lineSeparator()).append(status);
        }
        builder.append(System.lineSeparator()).append(critical.isBlank() ? "Critical failures: unavailable" : critical);
        builder.append(System.lineSeparator()).append(warnings.isBlank() ? "Warnings: unavailable" : warnings);
        if (!monthlyNote.isBlank()) {
            builder.append(System.lineSeparator()).append("IPCA monthly policy: ")
                    .append(monthlyNote.replaceFirst("^note:\\s*", ""));
        }
        return builder.toString();
    }

    private static String firstMatchingLine(String text, String prefix) {
        for (String line : safeLines(text)) {
            String trimmed = line.trim();
            if (trimmed.startsWith(prefix)) {
                return trimmed;
            }
        }
        return "";
    }

    private static String firstLineAfterSection(String text, String section, String prefix) {
        boolean inSection = false;
        for (String line : safeLines(text)) {
            String trimmed = line.trim();
            if (trimmed.equals(section)) {
                inSection = true;
                continue;
            }
            if (inSection && trimmed.endsWith(":") && !trimmed.equals(section)) {
                return "";
            }
            if (!inSection) {
                continue;
            }
            if (trimmed.startsWith(prefix)) {
                return trimmed;
            }
        }
        return "";
    }

    private static String[] safeLines(String text) {
        return (text == null ? "" : text).split("\\R");
    }

    private static boolean isZeroCountLine(String line) {
        return line == null || line.isBlank() || line.matches("^[A-Z_ ]+:\\s*0$");
    }

    private static String externalValidationLabel(String text, String externalFail) {
        String lower = text.toLowerCase();
        if (lower.contains("provider_key_missing") || lower.contains("twelve_data_api_key missing")) {
            return "Blocked by Missing Secret";
        }
        if (lower.contains("provider_tls_error") || lower.contains("tls/ca") || lower.contains("ssl_error")) {
            return "Blocked by Provider/TLS";
        }
        if (lower.contains("rate limit detected: true") || lower.contains("external_rate_limit")) {
            return "Rate Limited";
        }
        if (!isZeroCountLine(externalFail)) {
            return "Failed";
        }
        if (lower.contains("provider calls attempted: 0")) {
            return "Skipped";
        }
        return "Passed";
    }

    private static String promotionGateLabel(String releaseGate, String promotionAllowed) {
        String gate = releaseGate == null ? "" : releaseGate.toUpperCase();
        String allowed = promotionAllowed == null ? "" : promotionAllowed.toLowerCase();
        if (gate.contains("PASS_WITH_WARNINGS") || allowed.contains("true") && gate.contains("WARN")) {
            return "Allowed with warning";
        }
        if (gate.contains("PASS") || allowed.contains("true")) {
            return "Allowed";
        }
        return "Blocked";
    }

    public static List<String> reportPaths() {
        return List.of(
                "docs/LIVE_BUILD_REPORT.md",
                "docs/LIVE_SAMPLE_VALIDATION_REPORT.md",
                "docs/LIVE_AUDIT_REPORT.md",
                "docs/API_LIVE_SMOKE_REPORT.md",
                "docs/LIVE_365D_RELEASE_GATE_REPORT.md",
                "docs/LIVE_DATA_SCOPE.md",
                "docs/LIVE_PROMOTION_GUIDE.md",
                "docs/WORKSPACE_MIGRATION_AUDIT.md"
        );
    }

    public static List<String> logPaths() {
        return List.of(
                "logs/finance-monitor-startup.log",
                "logs/backend-visual-test.log",
                "logs/frontend-visual-test.log",
                "logs/app.log"
        );
    }
}
