package com.example.financedashboard;

import com.example.financedashboard.ops.CommandResult;
import com.example.financedashboard.ops.CommandSpec;
import com.example.financedashboard.ops.ControlCenterOperations;
import com.example.financedashboard.ops.LocalCommandRunner;
import com.example.financedashboard.ops.PipelineStepDefinition;
import com.example.financedashboard.ops.SecretInputValidator;
import com.example.financedashboard.ops.SecretRedactor;
import com.example.financedashboard.service.MarketDataService.HistoryRange;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlCenterOperationsTest {
    private static final String SAMPLE_SECRET = "REDACTION" + "SAMPLE" + "123456789";

    @Test
    void secretRedactorMasksExactKeysAndEnvAssignments() {
        SecretRedactor redactor = new SecretRedactor(List.of(SAMPLE_SECRET));

        String output = redactor.redact("TWELVE_DATA_API_KEY=" + SAMPLE_SECRET + " Bearer " + SAMPLE_SECRET);

        assertFalse(output.contains(SAMPLE_SECRET));
        assertTrue(output.contains("TWELVE_DATA_API_KEY=<redacted>"));
        assertTrue(output.contains("Bearer <redacted>"));
    }

    @Test
    void secretInputRejectsPastedCommands() {
        SecretInputValidator.ValidationResult result = SecretInputValidator.validateTwelveKey(
                "$env:TWELVE_DATA_API_KEY='" + SAMPLE_SECRET + "'; python -m fx_rates providers status"
        );

        assertFalse(result.valid());
        assertEquals("Cole somente a chave da Twelve Data, sem aspas, sem comando e sem caminho.", result.message());
    }

    @Test
    void secretInputRejectsPathsAndLongText() {
        assertFalse(SecretInputValidator.validateTwelveKey("C:\\Projetos_Local\\key.txt").valid());
        assertFalse(SecretInputValidator.validateTwelveKey("abc def ghi").valid());
        assertFalse(SecretInputValidator.validateTwelveKey("A".repeat(129)).valid());
    }

    @Test
    void localCommandRunnerBuildsArgumentListWithoutShellString() {
        try (LocalCommandRunner runner = new LocalCommandRunner(LocalCommandRunner.detectProjectRoot())) {
            CommandSpec spec = runner.fxRatesCommand(
                    "Validate Providers",
                    ControlCenterOperations.providerValidationArguments(),
                    Map.of("TWELVE_DATA_API_KEY", SAMPLE_SECRET),
                    Duration.ofMinutes(5)
            );

            assertEquals("-m", spec.arguments().get(0));
            assertEquals("fx_rates", spec.arguments().get(1));
            assertEquals("providers", spec.arguments().get(2));
            assertTrue(spec.commandLine().contains("--external-test"));
        }
    }

    @Test
    void pipelineRequiresTwelveKeyBeforeStockLiveValidation() {
        List<PipelineStepDefinition> steps = ControlCenterOperations.live365PipelineSteps();

        assertFalse(steps.get(0).requiresTwelveKey());
        assertTrue(steps.get(2).requiresTwelveKey());
        assertTrue(steps.get(3).requiresTwelveKey());
        assertTrue(steps.get(6).requiresTwelveKey());
    }

    @Test
    void environmentKeySummaryDoesNotExposeSecretValue() {
        String summary = ControlCenterOperations.twelveEnvironmentSummary(SAMPLE_SECRET);

        assertFalse(summary.contains(SAMPLE_SECRET));
        assertTrue(summary.contains("key_present=true"));
        assertTrue(summary.contains("source=session environment"));
        assertTrue(summary.contains("key_valid_format=true"));
        assertTrue(summary.contains("masked_preview=REDA****"));
    }

    @Test
    void providerOutputShowsStockPassWhenTwelveDataExternalTestPassed() {
        String output = "STOCK: provider=twelvedata status=configured available=true requires_api_key=true missing_env=- key_present=yes key_valid_format=true external_test=pass";

        assertTrue(ControlCenterOperations.providerOutputShowsStockPass(output));
        assertFalse(ControlCenterOperations.providerOutputShowsMissingTwelveKey(output));
    }

    @Test
    void providerOutputShowsExternalPassWhenAllProvidersPassed() {
        String output = """
                FX: provider=frankfurter status=configured available=true missing_env=- external_test=pass
                CRYPTO: provider=coingecko status=configured available=true missing_env=- external_test=pass
                STOCK: provider=twelvedata status=configured available=true missing_env=- external_test=pass
                MACRO: provider=bcb_sgs status=configured available=true missing_env=- external_test=pass
                """;

        assertTrue(ControlCenterOperations.providerOutputShowsExternalPass(output));
    }

    @Test
    void providerOutputShowsMissingTwelveKeyWhenEnvIsMissing() {
        String output = """
                STOCK: provider=twelvedata status=not_configured available=false requires_api_key=true missing_env=TWELVE_DATA_API_KEY key_present=no key_valid_format=false external_test=fail
                  TWELVE_DATA_API_KEY not set; live stock ingestion is unavailable.
                """;

        assertFalse(ControlCenterOperations.providerOutputShowsStockPass(output));
        assertTrue(ControlCenterOperations.providerOutputShowsMissingTwelveKey(output));
    }

    @Test
    void promoteStepRequiresManualConfirmation() {
        PipelineStepDefinition promote = ControlCenterOperations.live365PipelineSteps().get(7);

        assertTrue(promote.promoteStep());
        assertTrue(promote.requiresConfirmation());
        assertTrue(promote.fxRatesArguments().contains("--backup"));
    }

    @Test
    void advancedHistoryRangesRemainFutureOnly() {
        assertFalse(HistoryRange.THREE_Y.enabled());
        assertFalse(HistoryRange.FIVE_Y.enabled());
        assertFalse(HistoryRange.TEN_Y.enabled());
        assertTrue(ControlCenterOperations.advancedHistoryLabel().contains("future-only"));
    }

    @Test
    void dataModeLabelsShowLive365d() {
        assertEquals("LIVE 365D", ControlCenterOperations.dataModeLabel("live"));
        assertEquals("DEMO", ControlCenterOperations.dataModeLabel("demo"));
        assertEquals("MIXED", ControlCenterOperations.dataModeLabel("mixed"));
        assertEquals("UNKNOWN", ControlCenterOperations.dataModeLabel(""));
    }

    @Test
    void backendCommandUsesPythonModuleServe() {
        try (LocalCommandRunner runner = new LocalCommandRunner(LocalCommandRunner.detectProjectRoot())) {
            CommandSpec spec = runner.fxRatesCommand(
                    "Start Backend",
                    ControlCenterOperations.backendServeArguments("127.0.0.1", 8000),
                    Map.of(),
                    Duration.ofMinutes(1)
            );

            assertEquals("-m", spec.arguments().get(0));
            assertEquals("fx_rates", spec.arguments().get(1));
            assertEquals("serve", spec.arguments().get(2));
            assertTrue(spec.arguments().contains("--host"));
            assertTrue(spec.arguments().contains("--port"));
        }
    }

    @Test
    void localCommandRunnerRedactsSecretOutput() {
        try (LocalCommandRunner runner = new LocalCommandRunner(LocalCommandRunner.detectProjectRoot())) {
            CommandSpec spec = new CommandSpec(
                    "Print Secret",
                    runner.projectRoot(),
                    runner.pythonExecutable().toString(),
                    List.of("-c", "import os; print(os.environ.get('TWELVE_DATA_API_KEY'))"),
                    Map.of("TWELVE_DATA_API_KEY", SAMPLE_SECRET),
                    Duration.ofSeconds(10)
            );

            CommandResult result = runner.runForTests(spec);

            assertTrue(result.successful());
            assertFalse(result.stdout().contains(SAMPLE_SECRET));
            assertTrue(result.stdout().contains("REDA****"));
        }
    }

    @Test
    void localCommandRunnerClassifiesProviderExternalFailureOutput() {
        try (LocalCommandRunner runner = new LocalCommandRunner(LocalCommandRunner.detectProjectRoot())) {
            CommandSpec spec = new CommandSpec(
                    "Provider Failure",
                    runner.projectRoot(),
                    runner.pythonExecutable().toString(),
                    List.of("-c", "print('STOCK: provider=twelvedata status=not_configured external_test=fail')"),
                    Map.of(),
                    Duration.ofSeconds(10)
            );

            CommandResult result = runner.runForTests(spec);

            assertEquals(CommandResult.Status.FAILED, result.status());
        }
    }

    @Test
    void localCommandRunnerClassifiesProviderExternalPassOutput() {
        try (LocalCommandRunner runner = new LocalCommandRunner(LocalCommandRunner.detectProjectRoot())) {
            CommandSpec spec = new CommandSpec(
                    "Provider Pass",
                    runner.projectRoot(),
                    runner.pythonExecutable().toString(),
                    List.of("-c", "print('STOCK: provider=twelvedata status=configured available=true missing_env=- external_test=pass')"),
                    Map.of(),
                    Duration.ofSeconds(10)
            );

            CommandResult result = runner.runForTests(spec);

            assertEquals(CommandResult.Status.PASSED, result.status());
        }
    }

    @Test
    void sampleValidationSummaryHighlightsRateLimitAndReport() {
        String output = """
                LIVE SAMPLE VALIDATION STATUS: READY_WITH_WARNINGS
                Samples: OK=40 WARN=2 FAIL=0
                Provider failures: 1
                Rate limit detected: true
                INTERNAL SAMPLE VALIDATION:
                total samples: 38
                OK: 38
                WARN: 0
                FAIL: 0
                EXTERNAL PROVIDER SAMPLE VALIDATION:
                provider calls attempted: 2
                OK: 0
                WARN: 2
                FAIL: 0
                RATE_LIMIT: 2
                DATA DECISION:
                release_gate: PASS_WITH_WARNINGS
                promotion_allowed: true
                reason_codes: EXTERNAL_RATE_LIMIT
                Reason: EXTERNAL_RATE_LIMIT
                Report: docs\\LIVE_SAMPLE_VALIDATION_REPORT.md
                """;

        String summary = ControlCenterOperations.sampleValidationSummary(output);

        assertTrue(summary.contains("Samples: OK=40 WARN=2 FAIL=0"));
        assertTrue(summary.contains("Provider failures: 1"));
        assertTrue(summary.contains("Validação externa limitada pelo provider."));
        assertTrue(summary.contains("Internal validation: Passed"));
        assertTrue(summary.contains("External validation: Rate Limited"));
        assertTrue(summary.contains("Promotion gate: Allowed with warning"));
        assertTrue(summary.contains("Reason: EXTERNAL_RATE_LIMIT"));
        assertTrue(summary.contains("docs\\LIVE_SAMPLE_VALIDATION_REPORT.md"));
    }

    @Test
    void sampleValidationSummaryHighlightsMissingSecretBlocker() {
        String output = """
                LIVE SAMPLE VALIDATION STATUS: NOT_READY
                Samples: OK=120 WARN=0 FAIL=0
                Provider failures: 2
                Rate limit detected: false
                INTERNAL SAMPLE VALIDATION:
                total samples: 120
                OK: 120
                WARN: 0
                FAIL: 0
                EXTERNAL PROVIDER SAMPLE VALIDATION:
                provider calls attempted: 0
                OK: 0
                WARN: 0
                FAIL: 0
                SKIPPED: 1
                DATA DECISION:
                release_gate: BLOCKED
                promotion_allowed: false
                reason_codes: PROVIDER_KEY_MISSING
                Reason: TWELVE_DATA_API_KEY missing for stock sample validation.
                Report: docs\\LIVE_SAMPLE_VALIDATION_REPORT.md
                """;

        String summary = ControlCenterOperations.sampleValidationSummary(output);

        assertTrue(summary.contains("Internal validation: Passed"));
        assertTrue(summary.contains("External validation: Blocked by Missing Secret"));
        assertTrue(summary.contains("Promotion gate: Blocked"));
        assertTrue(summary.contains("PROVIDER_KEY_MISSING"));
    }

    @Test
    void auditLiveSummaryHighlightsIpcaMonthlyPolicy() {
        String output = """
                LIVE AUDIT STATUS: OK
                Critical failures: 0
                Warnings: 0
                MACRO IPCA_MONTHLY:
                  frequency: monthly
                  latest_date: 2026-04-01
                  stale_days: 63
                  allowed_stale_days: 75
                  status: OK
                  note: IPCA is a monthly macro series and may lag daily market data. Latest value is within allowed monthly freshness window.
                """;

        String summary = ControlCenterOperations.auditLiveSummary(output);

        assertTrue(summary.contains("LIVE AUDIT STATUS: OK"));
        assertTrue(summary.contains("Critical failures: 0"));
        assertTrue(summary.contains("Warnings: 0"));
        assertTrue(summary.contains("IPCA monthly policy: IPCA is a monthly macro series"));
    }
}
