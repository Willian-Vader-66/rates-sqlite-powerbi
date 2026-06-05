package com.example.financedashboard.ui;

import com.example.financedashboard.api.ApiException;
import com.example.financedashboard.config.AppConfig;
import com.example.financedashboard.model.SystemStatus;
import com.example.financedashboard.ops.CommandResult;
import com.example.financedashboard.ops.CommandSpec;
import com.example.financedashboard.ops.ControlCenterOperations;
import com.example.financedashboard.ops.LocalCommandRunner;
import com.example.financedashboard.ops.PipelineStepDefinition;
import com.example.financedashboard.ops.SecretInputValidator;
import com.example.financedashboard.ops.SecretRedactor;
import com.example.financedashboard.service.MarketDataService;
import com.example.financedashboard.util.FormatUtils;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Parent;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.TitledPane;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public class ControlCenterController {
    private static final DateTimeFormatter CHECKED_AT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String TWELVE_DATA_API_KEY = "TWELVE_DATA_API_KEY";
    private static final String BACKEND_OFFLINE_MODE_MESSAGE =
            "Backend offline. Start the local API server to read current data mode.";

    private final AppConfig config;
    private final MarketDataService marketDataService;
    private final LocalCommandRunner commandRunner;
    private final HttpClient httpClient;
    private final Map<String, String> sessionSecrets = new LinkedHashMap<>();
    private final List<PipelineStepDefinition> pipelineSteps = ControlCenterOperations.live365PipelineSteps();
    private final Map<Integer, StepStatus> stepStatuses = new LinkedHashMap<>();
    private final Map<Integer, Label> stepStatusLabels = new LinkedHashMap<>();
    private final Map<Integer, TextArea> stepLogs = new LinkedHashMap<>();
    private final ObservableList<ProviderRow> providerRows = FXCollections.observableArrayList();

    private final Label systemStatus = new Label("Waiting for status refresh.");
    private final Label dataModeStatus = new Label(BACKEND_OFFLINE_MODE_MESSAGE);
    private final Label dataModeDetails = new Label("-");
    private final Label secretStatus = new Label("Missing");
    private final Label secretFutureStatus = new Label("Optional/Future: COINGECKO_DEMO_API_KEY, COINGECKO_PRO_API_KEY, FRED_API_KEY");
    private final PasswordField twelvePassword = new PasswordField();
    private final TextField twelveVisible = new TextField();
    private final CheckBox showTwelveKey = new CheckBox("Show");
    private final Label providerAlert = new Label("Twelve Data key required for stock live data.");
    private final Label backendStatus = new Label("Not started by UI.");
    private final TextArea backendLog = new TextArea();
    private final Label mainDbStatus = new Label("-");
    private final Label candidateDbStatus = new Label("-");
    private final TextArea commandLog = new TextArea();
    private final ComboBox<String> reportSelector = new ComboBox<>();
    private final TextArea reportViewer = new TextArea();
    private final Label reportsStatus = new Label("-");

    private LocalCommandRunner.RunningCommand activeCommand;
    private LocalCommandRunner.RunningCommand backendCommand;
    private boolean backendStartedByUi;
    private boolean promoteDryRunPassed;
    private boolean syncingSecretFields;
    private String twelveKeySource = "missing";

    public ControlCenterController(AppConfig config, MarketDataService marketDataService) {
        this.config = config;
        this.marketDataService = marketDataService;
        this.commandRunner = new LocalCommandRunner(LocalCommandRunner.detectProjectRoot());
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
        for (PipelineStepDefinition step : pipelineSteps) {
            stepStatuses.put(step.number(), StepStatus.PENDING);
        }
        initializeProviderRows();
        activateEnvironmentTwelveKey(false);
    }

    public Parent createView() {
        Label title = new Label("Finance Monitor Control Center");
        title.getStyleClass().add("page-title");
        Label subtitle = new Label("Local operations for LIVE 365D, backend control, providers, secrets, reports, and database promotion.");
        subtitle.getStyleClass().add("muted");
        subtitle.setWrapText(true);
        VBox header = new VBox(4, title, subtitle);

        VBox content = new VBox(14,
                buildSystemStatusSection(),
                buildDataModeSection(),
                buildSecretKeysSection(),
                buildProvidersSection(),
                buildPipelineSection(),
                buildBackendSection(),
                buildDatabaseSection(),
                buildReportsSection(),
                buildAdvancedHistorySection()
        );
        content.getStyleClass().add("overview-page");

        ScrollPane scrollPane = new ScrollPane(content);
        scrollPane.setFitToWidth(true);
        scrollPane.getStyleClass().add("dashboard-scroll");

        BorderPane root = new BorderPane(scrollPane);
        root.setTop(header);
        root.getStyleClass().add("control-center-root");
        BorderPane.setMargin(header, new Insets(0, 0, 12, 0));

        Platform.runLater(() -> {
            refreshReports();
            refreshStatus();
            applyInitialVisualMode();
        });
        return root;
    }

    public void stop() {
        if (activeCommand != null && !activeCommand.isDone()) {
            activeCommand.cancel();
        }
        if (backendStartedByUi && backendCommand != null && !backendCommand.isDone()) {
            backendCommand.cancel();
        }
        commandRunner.close();
    }

    private VBox buildSystemStatusSection() {
        Button refresh = secondaryButton("Refresh Status");
        refresh.setOnAction(event -> refreshStatus());
        Button health = secondaryButton("Check Health");
        health.setOnAction(event -> checkBackendHealth());
        Button start = primaryButton("Start Backend");
        start.setOnAction(event -> startBackend());
        systemStatus.setWrapText(true);
        HBox actions = new HBox(8, refresh, health, start);
        actions.setAlignment(Pos.CENTER_LEFT);
        return section("System Status", new VBox(10, systemStatus, actions));
    }

    private VBox buildDataModeSection() {
        dataModeStatus.getStyleClass().add("data-mode-badge");
        dataModeDetails.setWrapText(true);
        Button refresh = secondaryButton("Refresh Status");
        refresh.setOnAction(event -> refreshStatus());
        Button start = secondaryButton("Start Backend");
        start.setOnAction(event -> startBackend());
        Button audit = secondaryButton("Run Audit");
        audit.setOnAction(event -> runOneOffCommand("Audit Main DB", ControlCenterOperations.auditMainArguments(), false, Duration.ofMinutes(5)));
        Button openSettings = secondaryButton("Open Settings");
        openSettings.setOnAction(event -> appendCommandLog("Control Center settings are available in Secret Keys, Providers, Backend Server, and Database."));
        Button prepareDemo = secondaryButton("Prepare DEMO");
        prepareDemo.setOnAction(event -> prepareDemoWithConfirmation());
        Button runLive = primaryButton("Run LIVE 365D");
        runLive.setOnAction(event -> runFullValidation());
        FlowPane actions = new FlowPane(8, 8, refresh, start, audit, openSettings, prepareDemo, runLive);
        return section("Data Mode", new VBox(10, dataModeStatus, dataModeDetails, actions));
    }

    private VBox buildSecretKeysSection() {
        twelvePassword.setPromptText("TWELVE_DATA_API_KEY");
        twelveVisible.setPromptText("TWELVE_DATA_API_KEY");
        twelveVisible.setVisible(false);
        twelveVisible.setManaged(false);
        twelvePassword.textProperty().addListener((obs, oldValue, newValue) -> syncSecretText(twelvePassword, twelveVisible, newValue));
        twelveVisible.textProperty().addListener((obs, oldValue, newValue) -> syncSecretText(twelveVisible, twelvePassword, newValue));
        showTwelveKey.selectedProperty().addListener((obs, oldValue, show) -> {
            twelveVisible.setVisible(show);
            twelveVisible.setManaged(show);
            twelvePassword.setVisible(!show);
            twelvePassword.setManaged(!show);
        });

        Button validate = primaryButton("Validate Key");
        validate.setOnAction(event -> validateTwelveKey());
        Button clear = secondaryButton("Clear Key");
        clear.setOnAction(event -> clearTwelveKey());
        Button useSession = secondaryButton("Use for this session");
        useSession.setOnAction(event -> useTwelveKeyForSession());
        Button useEnvironment = secondaryButton("Use environment key");
        useEnvironment.setOnAction(event -> useEnvironmentTwelveKey());

        HBox inputRow = new HBox(8, twelvePassword, twelveVisible, showTwelveKey);
        HBox.setHgrow(twelvePassword, Priority.ALWAYS);
        HBox.setHgrow(twelveVisible, Priority.ALWAYS);
        inputRow.setAlignment(Pos.CENTER_LEFT);
        HBox actions = new HBox(8, validate, clear, useSession, useEnvironment);
        secretStatus.setWrapText(true);
        secretFutureStatus.setWrapText(true);
        return section("Secret Keys", new VBox(10,
                labeledValue("TWELVE_DATA_API_KEY", secretStatus),
                inputRow,
                actions,
                secretFutureStatus
        ));
    }

    private VBox buildProvidersSection() {
        TableView<ProviderRow> table = new TableView<>(providerRows);
        table.getStyleClass().add("provider-table");
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);
        table.setPrefHeight(190);
        table.getColumns().add(column("Provider", ProviderRow::provider));
        table.getColumns().add(column("Asset Type", ProviderRow::assetType));
        table.getColumns().add(column("Configured", ProviderRow::configured));
        table.getColumns().add(column("Available", ProviderRow::available));
        table.getColumns().add(column("Missing Env", ProviderRow::missingEnv));
        table.getColumns().add(column("Last Checked", ProviderRow::lastChecked));
        table.getColumns().add(column("Message", ProviderRow::message));
        providerAlert.getStyleClass().add("negative");
        providerAlert.setWrapText(true);
        Button validate = primaryButton("Validate Providers");
        validate.setOnAction(event -> validateProviders());
        return section("Providers", new VBox(10, providerAlert, table, validate));
    }

    private VBox buildPipelineSection() {
        GridPane stepGrid = new GridPane();
        stepGrid.setHgap(10);
        stepGrid.setVgap(8);
        stepGrid.getStyleClass().add("control-step-grid");
        stepGrid.add(headerLabel("Step"), 0, 0);
        stepGrid.add(headerLabel("Command"), 1, 0);
        stepGrid.add(headerLabel("Status"), 2, 0);

        int row = 1;
        for (PipelineStepDefinition step : pipelineSteps) {
            Label number = new Label("Step " + step.number());
            Label command = new Label(step.name());
            command.setWrapText(true);
            Label status = stepStatusLabel(StepStatus.PENDING);
            stepStatusLabels.put(step.number(), status);
            stepGrid.add(number, 0, row);
            stepGrid.add(command, 1, row);
            stepGrid.add(status, 2, row);

            TextArea log = readOnlyTextArea(120);
            stepLogs.put(step.number(), log);
            TitledPane pane = new TitledPane(step.label() + " Logs", log);
            pane.setExpanded(false);
            stepGrid.add(pane, 1, row + 1);
            GridPane.setColumnSpan(pane, 2);
            row += 2;
        }

        Button full = primaryButton("Run Full Validation");
        full.setOnAction(event -> runFullValidation());
        Button next = secondaryButton("Run Next Step");
        next.setOnAction(event -> runNextStep());
        Button stop = secondaryButton("Stop");
        stop.setOnAction(event -> stopActiveCommand());
        Button reset = secondaryButton("Reset Status");
        reset.setOnAction(event -> resetPipelineStatus());
        Button promote = primaryButton("Promote Candidate");
        promote.setOnAction(event -> promoteCandidate());
        FlowPane actions = new FlowPane(8, 8, full, next, stop, reset, promote);

        commandLog.setPrefRowCount(9);
        commandLog.setEditable(false);
        commandLog.setWrapText(true);
        commandLog.getStyleClass().add("command-log");
        return section("Live 365D Pipeline", new VBox(10, actions, stepGrid, commandLog));
    }

    private VBox buildBackendSection() {
        backendLog.setPrefRowCount(8);
        backendLog.setEditable(false);
        backendLog.setWrapText(true);
        Button start = primaryButton("Start Backend");
        start.setOnAction(event -> startBackend());
        Button stop = secondaryButton("Stop Backend");
        stop.setOnAction(event -> stopBackend());
        Button restart = secondaryButton("Restart Backend");
        restart.setOnAction(event -> restartBackend());
        Button health = secondaryButton("Check Health");
        health.setOnAction(event -> checkBackendHealth());
        FlowPane actions = new FlowPane(8, 8, start, stop, restart, health);
        backendStatus.setWrapText(true);
        return section("Local API Server", new VBox(10, backendStatus, actions, backendLog));
    }

    private VBox buildDatabaseSection() {
        mainDbStatus.setWrapText(true);
        candidateDbStatus.setWrapText(true);
        Button auditMain = secondaryButton("Audit Main DB");
        auditMain.setOnAction(event -> runOneOffCommand("Audit Main DB", ControlCenterOperations.auditMainArguments(), false, Duration.ofMinutes(5)));
        Button auditCandidate = secondaryButton("Audit Candidate DB");
        auditCandidate.setOnAction(event -> runOneOffCommand("Audit Candidate DB", ControlCenterOperations.auditCandidateArguments(), false, Duration.ofMinutes(5)));
        Button openReports = secondaryButton("Open Reports");
        openReports.setOnAction(event -> openSelectedReport());
        Button promote = primaryButton("Promote Candidate");
        promote.setOnAction(event -> promoteCandidate());
        FlowPane actions = new FlowPane(8, 8, auditMain, auditCandidate, openReports, promote);
        return section("Database", new VBox(10,
                labeledValue("Main DB", mainDbStatus),
                labeledValue("Candidate DB", candidateDbStatus),
                actions
        ));
    }

    private VBox buildReportsSection() {
        reportSelector.setItems(FXCollections.observableArrayList(ControlCenterOperations.reportPaths()));
        reportSelector.getSelectionModel().selectFirst();
        Button open = primaryButton("Open Report");
        open.setOnAction(event -> openSelectedReport());
        Button refresh = secondaryButton("Refresh Reports");
        refresh.setOnAction(event -> refreshReports());
        Button copy = secondaryButton("Copy Summary");
        copy.setOnAction(event -> copyReportSummary());
        reportViewer.setPrefRowCount(12);
        reportViewer.setEditable(false);
        reportViewer.setWrapText(true);
        reportsStatus.setWrapText(true);
        HBox controls = new HBox(8, reportSelector, open, refresh, copy);
        HBox.setHgrow(reportSelector, Priority.ALWAYS);
        return section("Reports & Logs", new VBox(10, reportsStatus, controls, reportViewer));
    }

    private VBox buildAdvancedHistorySection() {
        Label info = new Label(ControlCenterOperations.advancedHistoryLabel()
                + "\nCurrent product scope remains LIVE 365D standard. Do not use 3Y/5Y/10Y as active live mode yet.");
        info.setWrapText(true);
        info.getStyleClass().add("muted");
        return section("Advanced History", info);
    }

    private void refreshStatus() {
        systemStatus.setText("Reading /api/system/status...");
        Task<SystemStatus> task = new Task<>() {
            @Override
            protected SystemStatus call() throws ApiException {
                return marketDataService.getSystemStatus();
            }
        };
        task.setOnSucceeded(event -> applySystemStatus(task.getValue()));
        task.setOnFailed(event -> {
            Throwable error = task.getException();
            String message = error == null ? "Backend unavailable." : error.getMessage();
            systemStatus.setText("Backend offline: " + message);
            dataModeStatus.setText(BACKEND_OFFLINE_MODE_MESSAGE);
            dataModeStatus.getStyleClass().removeAll("data-mode-demo", "data-mode-live", "data-mode-mixed", "data-mode-unknown");
            dataModeStatus.getStyleClass().add("data-mode-unknown");
            dataModeDetails.setText("-");
            backendStatus.setText("Backend offline or unreachable at " + config.getApiBaseUrl());
            updateDatabaseInfo(null);
        });
        Thread worker = new Thread(task, "control-center-status");
        worker.setDaemon(true);
        worker.start();
    }

    private void applySystemStatus(SystemStatus status) {
        String mode = ControlCenterOperations.dataModeLabel(status.dataMode());
        systemStatus.setText("""
                Backend online: %s
                DB: %s
                Data mode: %s
                Data health: %s
                """.formatted(
                config.getApiBaseUrl(),
                FormatUtils.text(status.dbPath()),
                mode,
                dataHealth(status)
        ));
        dataModeStatus.setText(mode);
        dataModeStatus.getStyleClass().removeAll("data-mode-demo", "data-mode-live", "data-mode-mixed", "data-mode-unknown");
        dataModeStatus.getStyleClass().add(dataModeStyle(status.dataMode()));
        String providers = status.providers() == null || status.providers().isEmpty() ? "-" : String.join(", ", status.providers());
        String warnings = status.dataWarning() == null || status.dataWarning().isBlank() ? "-" : status.dataWarning();
        String failures = status.dataHealth() == null || status.dataHealth().symbolsWithoutHistory() == null
                ? "-"
                : String.join(", ", status.dataHealth().symbolsWithoutHistory());
        dataModeDetails.setText("""
                data_health: %s
                history_mode: %s
                requested_days: %s
                date_min: %s
                date_max: %s
                total instruments: %s
                historical rows: %s
                providers active: %s
                warnings: %s
                failures: %s
                """.formatted(
                dataHealth(status),
                FormatUtils.text(status.historyMode()),
                FormatUtils.integer(status.requestedDays()),
                FormatUtils.text(status.dateMin()),
                FormatUtils.text(status.dateMax()),
                FormatUtils.integer(status.totalInstruments()),
                FormatUtils.integer(status.historicalRowCount()),
                providers,
                warnings,
                failures == null || failures.isBlank() ? "-" : failures
        ));
        updateProviderRowsFromStatus(status.liveProviderStatus());
        updateDatabaseInfo(status);
    }

    private void validateTwelveKey() {
        SecretInputValidator.ValidationResult validation = validateCurrentTwelveInput();
        if (!validation.valid()) {
            secretStatus.setText("Invalid: " + validation.message());
            return;
        }
        storeTwelveKey(validation.value(), "Validating");
        CommandSpec spec = commandRunner.fxRatesCommand(
                "Validate Twelve Data key",
                ControlCenterOperations.providerValidationArguments(),
                sessionSecrets,
                Duration.ofMinutes(5)
        );
        runCommand(spec, null, result -> {
            String output = result.stdout() + "\n" + result.stderr();
            boolean valid = result.successful() && stockProviderValid(output);
            if (!valid) {
                sessionSecrets.remove(TWELVE_DATA_API_KEY);
                twelveKeySource = "missing";
            }
            secretStatus.setText(valid
                    ? keySummary("Valid")
                    : "Invalid: Twelve Data provider did not pass external validation.");
            updateProviderRowsFromOutput(output);
        });
    }

    private void useTwelveKeyForSession() {
        SecretInputValidator.ValidationResult validation = validateCurrentTwelveInput();
        if (!validation.valid()) {
            secretStatus.setText("Invalid: " + validation.message());
            return;
        }
        storeTwelveKey(validation.value(), "Present");
    }

    private void clearTwelveKey() {
        sessionSecrets.remove(TWELVE_DATA_API_KEY);
        twelveKeySource = "missing";
        twelvePassword.clear();
        twelveVisible.clear();
        if (!activateEnvironmentTwelveKey(false)) {
            secretStatus.setText("Missing: Add TWELVE_DATA_API_KEY in Secret Keys or start the app from a PowerShell session where the variable is set.");
            providerAlert.setText("Twelve Data key required for stock live data.");
        }
    }

    private SecretInputValidator.ValidationResult validateCurrentTwelveInput() {
        String raw = showTwelveKey.isSelected() ? twelveVisible.getText() : twelvePassword.getText();
        return SecretInputValidator.validateTwelveKey(raw);
    }

    private void storeTwelveKey(String key, String status) {
        sessionSecrets.put(TWELVE_DATA_API_KEY, key);
        twelveKeySource = "Secret Keys UI";
        twelvePassword.clear();
        twelveVisible.clear();
        showTwelveKey.setSelected(false);
        secretStatus.setText(keySummary(status));
        providerAlert.setText("TWELVE_DATA_API_KEY present for this JavaFX session only.");
    }

    private String keySummary(String state) {
        String key = sessionSecrets.get(TWELVE_DATA_API_KEY);
        int length = key == null ? 0 : key.length();
        return "%s: key_present=true, source=%s, key_valid_format=true, key_length=%s, masked_preview=%s".formatted(
                state,
                twelveKeySource,
                length,
                SecretRedactor.maskedPreview(key)
        );
    }

    private void useEnvironmentTwelveKey() {
        if (!activateEnvironmentTwelveKey(true)) {
            secretStatus.setText("Missing: Add TWELVE_DATA_API_KEY in Secret Keys or start the app from a PowerShell session where the variable is set.");
        }
    }

    private boolean activateEnvironmentTwelveKey(boolean showFailure) {
        String raw = System.getenv(TWELVE_DATA_API_KEY);
        if (raw == null || raw.isBlank()) {
            if (showFailure) {
                secretStatus.setText("Missing: Add TWELVE_DATA_API_KEY in Secret Keys or start the app from a PowerShell session where the variable is set.");
            }
            return false;
        }
        SecretInputValidator.ValidationResult validation = SecretInputValidator.validateTwelveKey(raw);
        if (!validation.valid()) {
            if (showFailure || sessionSecrets.isEmpty()) {
                secretStatus.setText("Invalid environment key: " + ControlCenterOperations.twelveEnvironmentSummary(raw));
                providerAlert.setText("Cole somente a chave da Twelve Data, sem aspas, sem comando e sem caminho.");
            }
            return false;
        }
        sessionSecrets.put(TWELVE_DATA_API_KEY, validation.value());
        twelveKeySource = "session environment";
        twelvePassword.clear();
        twelveVisible.clear();
        showTwelveKey.setSelected(false);
        secretStatus.setText("Twelve Data key detected from session environment. Value is not displayed or stored. "
                + ControlCenterOperations.twelveEnvironmentSummary(validation.value()));
        providerAlert.setText("Twelve Data key detected from session environment. Value is not displayed or stored.");
        return true;
    }

    private void validateProviders() {
        if (!hasTwelveKeyForSession()) {
            providerAlert.setText("Add TWELVE_DATA_API_KEY in Secret Keys or start the app from a PowerShell session where the variable is set.");
        }
        CommandSpec spec = commandRunner.fxRatesCommand(
                "Validate Providers",
                ControlCenterOperations.providerValidationArguments(),
                sessionSecrets,
                Duration.ofMinutes(5)
        );
        runCommand(spec, null, result -> updateProviderRowsFromOutput(result.stdout() + "\n" + result.stderr()));
    }

    private void prepareDemoWithConfirmation() {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Prepare DEMO data");
        alert.setHeaderText("Prepare deterministic DEMO dashboard data?");
        alert.setContentText("This is for development, visual tests, and demos. It does not represent real market data.");
        Optional<ButtonType> result = alert.showAndWait();
        if (result.isPresent() && result.get() == ButtonType.OK) {
            runOneOffCommand("Prepare DEMO 365D", ControlCenterOperations.prepareDemoArguments(), false, Duration.ofMinutes(10));
        }
    }

    private void runFullValidation() {
        if (activeCommand != null && !activeCommand.isDone()) {
            appendCommandLog("A command is already running.");
            return;
        }
        runValidationStepAt(0);
    }

    private void runValidationStepAt(int index) {
        List<PipelineStepDefinition> validationSteps = ControlCenterOperations.validationOnlySteps();
        if (index >= validationSteps.size()) {
            appendCommandLog("LIVE 365D validation completed. Promotion remains manual.");
            return;
        }
        PipelineStepDefinition step = validationSteps.get(index);
        runPipelineStep(step, success -> {
            if (success) {
                runValidationStepAt(index + 1);
            } else {
                appendCommandLog("Pipeline stopped at " + step.label() + ".");
            }
        });
    }

    private void runNextStep() {
        for (PipelineStepDefinition step : pipelineSteps) {
            StepStatus status = stepStatuses.get(step.number());
            if (!isStepComplete(status)) {
                if (step.promoteStep() && !promoteDryRunPassed) {
                    appendCommandLog("Promote Candidate is locked until Step 7 passes.");
                    return;
                }
                runPipelineStep(step, ignored -> {
                });
                return;
            }
        }
        appendCommandLog("All LIVE 365D pipeline steps are already complete.");
    }

    private void promoteCandidate() {
        PipelineStepDefinition promote = pipelineSteps.stream()
                .filter(PipelineStepDefinition::promoteStep)
                .findFirst()
                .orElseThrow();
        if (!promoteDryRunPassed) {
            appendCommandLog("Promote Candidate is locked until Step 7 - Promote Dry Run passes.");
            return;
        }
        runPipelineStep(promote, ignored -> {
        });
    }

    private void runPipelineStep(PipelineStepDefinition step, java.util.function.Consumer<Boolean> afterCompletion) {
        if (step.requiresTwelveKey() && !hasTwelveKeyForSession()) {
            setStepStatus(step, StepStatus.BLOCKED_SECRET);
            appendStepLog(step, "Add TWELVE_DATA_API_KEY in Secret Keys or start the app from a PowerShell session where the variable is set.");
            appendCommandLog("TWELVE_DATA_API_KEY is required before " + step.label() + ".");
            if (afterCompletion != null) {
                afterCompletion.accept(false);
            }
            return;
        }
        if (step.requiresConfirmation() && !confirmPromotion()) {
            setStepStatus(step, StepStatus.SKIPPED);
            appendStepLog(step, "Promotion cancelled by user.");
            if (afterCompletion != null) {
                afterCompletion.accept(false);
            }
            return;
        }
        CommandSpec spec = commandRunner.fxRatesCommand(step.label(), step.fxRatesArguments(), sessionSecrets, step.timeout());
        setStepStatus(step, StepStatus.RUNNING);
        appendStepLog(step, "$ " + spec.displayCommand(SecretRedactor.fromSessionSecrets(sessionSecrets)));
        runCommand(spec, line -> appendStepLog(step, line.text()), result -> {
            StepStatus status = statusForResult(step, result);
            setStepStatus(step, status);
            if (step.number() == 7 && isStepComplete(status)) {
                promoteDryRunPassed = true;
            }
            if (step.number() == 4) {
                String summary = ControlCenterOperations.sampleValidationSummary(result.stdout() + "\n" + result.stderr());
                prependStepLog(step, summary);
                appendCommandLog(summary);
            }
            if (step.number() == 5) {
                String summary = ControlCenterOperations.auditLiveSummary(result.stdout() + "\n" + result.stderr());
                prependStepLog(step, summary);
                appendCommandLog(summary);
            }
            if (afterCompletion != null) {
                afterCompletion.accept(isStepComplete(status));
            }
        });
    }

    private StepStatus statusForResult(PipelineStepDefinition step, CommandResult result) {
        String output = ((result.stdout() == null ? "" : result.stdout()) + "\n" + (result.stderr() == null ? "" : result.stderr())).toLowerCase(Locale.ROOT);
        if (result.status() == CommandResult.Status.WARNING) {
            return StepStatus.WARNING;
        }
        if (result.status() == CommandResult.Status.PASSED) {
            if (step.number() == 6) {
                return StepStatus.READY_DRY_RUN;
            }
            if (step.number() == 7) {
                return StepStatus.READY_PROMOTION;
            }
            return StepStatus.PASSED;
        }
        if (ControlCenterOperations.providerOutputShowsMissingTwelveKey(output)
                || output.contains("missing secret")
                || output.contains("provider_key_missing")) {
            return StepStatus.BLOCKED_SECRET;
        }
        if (output.contains("tls/ca") || output.contains("ssl_error") || output.contains("provider_tls_error")) {
            return StepStatus.BLOCKED_PROVIDER;
        }
        return StepStatus.FAILED;
    }

    private boolean isStepComplete(StepStatus status) {
        return status == StepStatus.PASSED
                || status == StepStatus.WARNING
                || status == StepStatus.READY_DRY_RUN
                || status == StepStatus.READY_PROMOTION;
    }

    private boolean confirmPromotion() {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Promote candidate database");
        alert.setHeaderText("Promote candidate LIVE database to main database?");
        alert.setContentText("""
                A backup will be created.
                data/fx.sqlite will be replaced.
                This is not reversible without restoring a backup.
                """);
        Optional<ButtonType> result = alert.showAndWait();
        return result.isPresent() && result.get() == ButtonType.OK;
    }

    private void resetPipelineStatus() {
        promoteDryRunPassed = false;
        for (PipelineStepDefinition step : pipelineSteps) {
            setStepStatus(step, StepStatus.PENDING);
            TextArea log = stepLogs.get(step.number());
            if (log != null) {
                log.clear();
            }
        }
        appendCommandLog("Pipeline status reset.");
    }

    private void stopActiveCommand() {
        if (activeCommand == null || activeCommand.isDone()) {
            appendCommandLog("No active command to stop.");
            return;
        }
        activeCommand.cancel();
        appendCommandLog("Stop requested for active command.");
    }

    private void startBackend() {
        if (backendCommand != null && !backendCommand.isDone()) {
            backendStatus.setText("Backend already started by this UI.");
            return;
        }
        Endpoint endpoint = endpoint();
        if (isPortOpen(endpoint.host(), endpoint.port())) {
            backendStatus.setText("Port " + endpoint.port() + " is already in use.");
            appendBackendLog("Port " + endpoint.port() + " is already in use.");
            return;
        }
        CommandSpec spec = commandRunner.fxRatesCommand(
                "Start Backend",
                ControlCenterOperations.backendServeArguments(endpoint.host(), endpoint.port()),
                Map.of(),
                Duration.ofDays(1)
        );
        backendStartedByUi = true;
        backendStatus.setText("Starting backend on " + config.getApiBaseUrl());
        backendCommand = commandRunner.runAsync(spec, line -> Platform.runLater(() -> appendBackendLog(line.text())), result -> Platform.runLater(() -> {
            if (result.status() == CommandResult.Status.CANCELLED) {
                backendStatus.setText("Backend stopped by UI.");
            } else {
                backendStatus.setText("Backend process ended: " + result.status() + " exit=" + result.exitCode());
            }
        }));
        backendCommand.pid().ifPresent(pid -> backendStatus.setText("Backend starting. PID: " + pid));
    }

    private void stopBackend() {
        if (!backendStartedByUi || backendCommand == null || backendCommand.isDone()) {
            backendStatus.setText("No backend process started by this UI to stop.");
            return;
        }
        backendCommand.cancel();
        backendStartedByUi = false;
        backendStatus.setText("Backend stop requested.");
    }

    private void restartBackend() {
        stopBackend();
        Platform.runLater(this::startBackend);
    }

    private void checkBackendHealth() {
        backendStatus.setText("Checking /health...");
        Task<String> task = new Task<>() {
            @Override
            protected String call() throws Exception {
                HttpRequest request = HttpRequest.newBuilder(URI.create(config.getApiBaseUrl() + "/health"))
                        .timeout(Duration.ofSeconds(5))
                        .GET()
                        .build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                return "HTTP " + response.statusCode() + "\n" + response.body();
            }
        };
        task.setOnSucceeded(event -> {
            String text = SecretRedactor.fromSessionSecrets(sessionSecrets).redact(task.getValue());
            backendStatus.setText("Health OK at " + config.getApiBaseUrl());
            appendBackendLog(text);
            refreshStatus();
        });
        task.setOnFailed(event -> {
            Throwable error = task.getException();
            backendStatus.setText("Health failed: " + (error == null ? "unknown" : error.getMessage()));
        });
        Thread worker = new Thread(task, "control-center-health");
        worker.setDaemon(true);
        worker.start();
    }

    private void runOneOffCommand(String label, List<String> args, boolean requiresTwelve, Duration timeout) {
        if (requiresTwelve && !hasTwelveKeyForSession()) {
            appendCommandLog("Add TWELVE_DATA_API_KEY in Secret Keys or start the app from a PowerShell session where the variable is set before " + label + ".");
            return;
        }
        CommandSpec spec = commandRunner.fxRatesCommand(label, args, sessionSecrets, timeout);
        runCommand(spec, null, result -> {
            appendCommandLog(label + " finished with " + result.status() + " exit=" + result.exitCode());
            refreshReports();
            refreshStatus();
        });
    }

    private void runCommand(
            CommandSpec spec,
            java.util.function.Consumer<LocalCommandRunner.StreamLine> lineConsumer,
            java.util.function.Consumer<CommandResult> completion
    ) {
        if (activeCommand != null && !activeCommand.isDone()) {
            appendCommandLog("A command is already running.");
            return;
        }
        appendCommandLog("$ " + spec.displayCommand(SecretRedactor.fromSessionSecrets(sessionSecrets)));
        activeCommand = commandRunner.runAsync(spec, line -> Platform.runLater(() -> {
            appendCommandLog(line.text());
            if (lineConsumer != null) {
                lineConsumer.accept(line);
            }
        }), result -> Platform.runLater(() -> {
            activeCommand = null;
            appendCommandLog(spec.label() + " -> " + result.status() + " exit=" + result.exitCode());
            if (completion != null) {
                completion.accept(result);
            }
        }));
    }

    private void updateProviderRowsFromStatus(SystemStatus.LiveProviderStatus status) {
        if (status == null || status.providers() == null) {
            return;
        }
        String checked = LocalDateTime.now().format(CHECKED_AT);
        for (SystemStatus.ProviderItem item : status.providers()) {
            ProviderRow row = providerRow(item.assetType());
            if (row != null) {
                row.provider.set(FormatUtils.text(item.provider()));
                row.configured.set(Boolean.TRUE.equals(item.configured()) ? "yes" : "no");
                row.available.set(Boolean.TRUE.equals(item.available()) ? "yes" : "no");
                row.missingEnv.set(item.missingEnv() == null || item.missingEnv().isEmpty() ? "-" : String.join(",", item.missingEnv()));
                row.lastChecked.set(checked);
                row.message.set(FormatUtils.text(item.message()));
            }
        }
        updateProviderAlert();
    }

    private void updateProviderRowsFromOutput(String output) {
        String checked = LocalDateTime.now().format(CHECKED_AT);
        for (String rawLine : output.split("\\R")) {
            String line = rawLine.trim();
            if (!line.matches("^(FX|CRYPTO|STOCK|MACRO):.*")) {
                continue;
            }
            String asset = line.substring(0, line.indexOf(':'));
            ProviderRow row = providerRow(asset);
            if (row == null) {
                continue;
            }
            row.provider.set(tokenValue(line, "provider", row.provider.get()));
            row.configured.set(line.contains("status=configured") ? "yes" : "no");
            row.available.set("true".equalsIgnoreCase(tokenValue(line, "available", "false")) ? "yes" : "no");
            row.missingEnv.set(tokenValue(line, "missing_env", "-"));
            row.lastChecked.set(checked);
            row.message.set(tokenValue(line, "external_test", "-"));
        }
        if (ControlCenterOperations.providerOutputShowsStockPass(output)) {
            activateEnvironmentTwelveKey(false);
            ProviderRow stock = providerRow("STOCK");
            if (stock != null && "yes".equalsIgnoreCase(stock.available.get())) {
                providerAlert.setText("Twelve Data provider validated for this session. Secret value is not displayed or stored.");
                if (!sessionSecrets.containsKey(TWELVE_DATA_API_KEY)) {
                    secretStatus.setText("Twelve Data key validated by provider status. Value is not displayed or stored.");
                }
            }
        }
        if (ControlCenterOperations.providerOutputShowsExternalPass(output)) {
            PipelineStepDefinition providerStep = pipelineSteps.stream()
                    .filter(step -> step.number() == 1)
                    .findFirst()
                    .orElse(null);
            if (providerStep != null && stepStatuses.get(providerStep.number()) == StepStatus.BLOCKED_SECRET) {
                setStepStatus(providerStep, StepStatus.PASSED);
            }
        }
        updateProviderAlert();
    }

    private void updateProviderAlert() {
        ProviderRow stock = providerRow("STOCK");
        if (stock != null && !"yes".equalsIgnoreCase(stock.configured.get())) {
            providerAlert.setText("Twelve Data key required for stock live data.");
        } else if (stock != null && "yes".equalsIgnoreCase(stock.available.get())) {
            providerAlert.setText("Stock provider configured for this session.");
        }
    }

    private boolean hasTwelveKeyForSession() {
        if (sessionSecrets.containsKey(TWELVE_DATA_API_KEY)) {
            return true;
        }
        if (activateEnvironmentTwelveKey(false)) {
            return true;
        }
        ProviderRow stock = providerRow("STOCK");
        return stock != null
                && "yes".equalsIgnoreCase(stock.configured.get())
                && "yes".equalsIgnoreCase(stock.available.get())
                && !stock.missingEnv.get().toUpperCase(Locale.ROOT).contains(TWELVE_DATA_API_KEY);
    }

    private boolean stockProviderValid(String output) {
        return ControlCenterOperations.providerOutputShowsStockPass(output);
    }

    private String tokenValue(String line, String key, String fallback) {
        String prefix = key + "=";
        for (String token : line.split("\\s+")) {
            if (token.startsWith(prefix)) {
                String value = token.substring(prefix.length()).trim();
                return value.isBlank() ? fallback : value;
            }
        }
        return fallback;
    }

    private void updateDatabaseInfo(SystemStatus status) {
        Path root = commandRunner.projectRoot();
        Path main = root.resolve(ControlCenterOperations.MAIN_DB);
        Path candidate = root.resolve(ControlCenterOperations.CANDIDATE_DB);
        if (status == null) {
            mainDbStatus.setText(fileSummary(main) + "\nBackend status unavailable.");
        } else {
            mainDbStatus.setText("""
                    path: %s
                    exists: %s
                    size: %s
                    data_mode: %s
                    data_health: %s
                    date_min: %s
                    date_max: %s
                    historical rows: %s
                    """.formatted(
                    ControlCenterOperations.MAIN_DB,
                    Boolean.TRUE.equals(status.dbExists()) ? "true" : "false",
                    FormatUtils.bytes(status.dbSizeBytes()),
                    ControlCenterOperations.dataModeLabel(status.dataMode()),
                    dataHealth(status),
                    FormatUtils.text(status.dateMin()),
                    FormatUtils.text(status.dateMax()),
                    FormatUtils.integer(status.historicalRowCount())
            ));
        }
        candidateDbStatus.setText("""
                path: %s
                exists: %s
                size: %s
                last build status: %s
                last validation status: %s
                """.formatted(
                ControlCenterOperations.CANDIDATE_DB,
                Files.exists(candidate) ? "true" : "false",
                fileSize(candidate),
                stepStatuses.getOrDefault(3, StepStatus.PENDING).label,
                stepStatuses.getOrDefault(4, StepStatus.PENDING).label
        ));
    }

    private void refreshReports() {
        SecretRedactor redactor = SecretRedactor.fromSessionSecrets(sessionSecrets);
        StringBuilder summary = new StringBuilder("Reports:\n");
        Path root = commandRunner.projectRoot();
        for (String report : ControlCenterOperations.reportPaths()) {
            summary.append(Files.exists(root.resolve(report)) ? "yes  " : "no   ").append(report).append("\n");
        }
        summary.append("\nLogs:\n");
        for (String log : ControlCenterOperations.logPaths()) {
            summary.append(Files.exists(root.resolve(log)) ? "yes  " : "no   ").append(log).append("\n");
        }
        reportsStatus.setText(summary.toString());
        if (reportViewer.getText().isBlank()) {
            openSelectedReport();
        } else {
            reportViewer.setText(redactor.redact(reportViewer.getText()));
        }
    }

    private void openSelectedReport() {
        String selected = reportSelector.getSelectionModel().getSelectedItem();
        if (selected == null || selected.isBlank()) {
            return;
        }
        Path path = commandRunner.projectRoot().resolve(selected);
        if (!Files.exists(path)) {
            reportViewer.setText(selected + " does not exist yet.");
            return;
        }
        try {
            String text = Files.readString(path);
            reportViewer.setText(SecretRedactor.fromSessionSecrets(sessionSecrets).redact(text));
        } catch (IOException ex) {
            reportViewer.setText("Could not read " + selected + ": " + ex.getMessage());
        }
    }

    private void copyReportSummary() {
        ClipboardContent content = new ClipboardContent();
        content.putString(SecretRedactor.fromSessionSecrets(sessionSecrets).redact(reportsStatus.getText()));
        Clipboard.getSystemClipboard().setContent(content);
        appendCommandLog("Report summary copied to clipboard.");
    }

    private void applyInitialVisualMode() {
        String initialMode = System.getenv("FINANCE_INITIAL_DATA_MODE");
        if (initialMode != null && !initialMode.isBlank()) {
            appendCommandLog("Initial visual mode requested: " + initialMode.trim().toUpperCase(Locale.ROOT) + ". No data command was run automatically.");
        }
    }

    private void setStepStatus(PipelineStepDefinition step, StepStatus status) {
        stepStatuses.put(step.number(), status);
        Label label = stepStatusLabels.get(step.number());
        if (label != null) {
            label.setText(status.label);
            label.getStyleClass().removeAll("step-pending", "step-running", "step-passed", "step-warning", "step-failed", "step-blocked", "step-provider-blocked", "step-ready", "step-skipped");
            label.getStyleClass().add(status.styleClass);
        }
        updateDatabaseInfo(null);
    }

    private void appendStepLog(PipelineStepDefinition step, String text) {
        TextArea log = stepLogs.get(step.number());
        if (log != null) {
            log.appendText(SecretRedactor.fromSessionSecrets(sessionSecrets).redact(text) + System.lineSeparator());
        }
    }

    private void prependStepLog(PipelineStepDefinition step, String text) {
        TextArea log = stepLogs.get(step.number());
        if (log != null) {
            String redacted = SecretRedactor.fromSessionSecrets(sessionSecrets).redact(text);
            log.setText(redacted + System.lineSeparator() + System.lineSeparator() + log.getText());
        }
    }

    private void appendCommandLog(String text) {
        commandLog.appendText(SecretRedactor.fromSessionSecrets(sessionSecrets).redact(text) + System.lineSeparator());
    }

    private void appendBackendLog(String text) {
        backendLog.appendText(SecretRedactor.fromSessionSecrets(sessionSecrets).redact(text) + System.lineSeparator());
    }

    private boolean isPortOpen(String host, int port) {
        try (java.net.Socket socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress(host, port), 500);
            return true;
        } catch (ConnectException ex) {
            return false;
        } catch (IOException ex) {
            return false;
        }
    }

    private Endpoint endpoint() {
        try {
            URI uri = URI.create(config.getApiBaseUrl());
            String host = uri.getHost() == null ? "127.0.0.1" : uri.getHost();
            int port = uri.getPort() > 0 ? uri.getPort() : 8000;
            return new Endpoint(host, port);
        } catch (IllegalArgumentException ex) {
            return new Endpoint("127.0.0.1", 8000);
        }
    }

    private String dataHealth(SystemStatus status) {
        if (status == null || status.dataHealth() == null || status.dataHealth().status() == null) {
            return "UNKNOWN";
        }
        return status.dataHealth().status().toUpperCase(Locale.ROOT);
    }

    private String dataModeStyle(String mode) {
        if (mode == null || mode.isBlank()) {
            return "data-mode-unknown";
        }
        return switch (mode.trim().toLowerCase(Locale.ROOT)) {
            case "demo" -> "data-mode-demo";
            case "live" -> "data-mode-live";
            case "mixed" -> "data-mode-mixed";
            default -> "data-mode-unknown";
        };
    }

    private String fileSummary(Path path) {
        return "path: " + path + "\nexists: " + Files.exists(path) + "\nsize: " + fileSize(path);
    }

    private String fileSize(Path path) {
        try {
            return Files.exists(path) ? FormatUtils.bytes(Files.size(path)) : "-";
        } catch (IOException ex) {
            return "unavailable";
        }
    }

    private void syncSecretText(TextField source, TextField target, String value) {
        if (syncingSecretFields) {
            return;
        }
        syncingSecretFields = true;
        try {
            if (!target.getText().equals(value)) {
                target.setText(value);
            }
        } finally {
            syncingSecretFields = false;
        }
    }

    private void initializeProviderRows() {
        providerRows.setAll(
                new ProviderRow("Frankfurter", "FX", "yes", "unknown", "-", "-", "No key required."),
                new ProviderRow("CoinGecko", "CRYPTO", "yes", "unknown", "-", "-", "Public/demo/pro, standard 365D."),
                new ProviderRow("Twelve Data", "STOCK", "no", "unknown", "TWELVE_DATA_API_KEY", "-", "Twelve Data key required for stock live data."),
                new ProviderRow("BCB SGS", "MACRO", "yes", "unknown", "-", "-", "No key required.")
        );
    }

    private ProviderRow providerRow(String assetType) {
        if (assetType == null) {
            return null;
        }
        return providerRows.stream()
                .filter(row -> row.assetType.get().equalsIgnoreCase(assetType))
                .findFirst()
                .orElse(null);
    }

    private Button primaryButton(String text) {
        Button button = new Button(text);
        button.getStyleClass().add("neon-button");
        return button;
    }

    private Button secondaryButton(String text) {
        Button button = new Button(text);
        button.getStyleClass().add("secondary-button");
        return button;
    }

    private VBox section(String title, javafx.scene.Node body) {
        Label titleLabel = new Label(title);
        titleLabel.getStyleClass().add("panel-title");
        VBox section = new VBox(10, titleLabel, body);
        section.getStyleClass().add("panel");
        return section;
    }

    private VBox labeledValue(String title, Label value) {
        Label titleLabel = new Label(title);
        titleLabel.getStyleClass().add("metric-title");
        value.setWrapText(true);
        VBox box = new VBox(5, titleLabel, value);
        box.getStyleClass().add("control-subcard");
        return box;
    }

    private Label headerLabel(String text) {
        Label label = new Label(text);
        label.getStyleClass().add("table-header");
        return label;
    }

    private Label stepStatusLabel(StepStatus status) {
        Label label = new Label(status.label);
        label.getStyleClass().addAll("step-status", status.styleClass);
        return label;
    }

    private TextArea readOnlyTextArea(int height) {
        TextArea area = new TextArea();
        area.setEditable(false);
        area.setWrapText(true);
        area.setPrefHeight(height);
        return area;
    }

    private TableColumn<ProviderRow, String> column(String title, java.util.function.Function<ProviderRow, SimpleStringProperty> property) {
        TableColumn<ProviderRow, String> column = new TableColumn<>(title);
        column.setCellValueFactory(data -> property.apply(data.getValue()));
        return column;
    }

    private enum StepStatus {
        PENDING("Pending", "step-pending"),
        RUNNING("Running", "step-running"),
        PASSED("Passed", "step-passed"),
        WARNING("Passed with Warnings", "step-warning"),
        FAILED("Failed", "step-failed"),
        BLOCKED_SECRET("Blocked by Missing Secret", "step-blocked"),
        BLOCKED_PROVIDER("Blocked by Provider/TLS", "step-provider-blocked"),
        READY_DRY_RUN("Ready for Dry Run", "step-ready"),
        READY_PROMOTION("Ready for Promotion", "step-ready"),
        SKIPPED("Skipped", "step-skipped");

        private final String label;
        private final String styleClass;

        StepStatus(String label, String styleClass) {
            this.label = label;
            this.styleClass = styleClass;
        }
    }

    private record Endpoint(String host, int port) {
    }

    private static final class ProviderRow {
        private final SimpleStringProperty provider;
        private final SimpleStringProperty assetType;
        private final SimpleStringProperty configured;
        private final SimpleStringProperty available;
        private final SimpleStringProperty missingEnv;
        private final SimpleStringProperty lastChecked;
        private final SimpleStringProperty message;

        private ProviderRow(
                String provider,
                String assetType,
                String configured,
                String available,
                String missingEnv,
                String lastChecked,
                String message
        ) {
            this.provider = new SimpleStringProperty(provider);
            this.assetType = new SimpleStringProperty(assetType);
            this.configured = new SimpleStringProperty(configured);
            this.available = new SimpleStringProperty(available);
            this.missingEnv = new SimpleStringProperty(missingEnv);
            this.lastChecked = new SimpleStringProperty(lastChecked);
            this.message = new SimpleStringProperty(message);
        }

        private SimpleStringProperty provider() {
            return provider;
        }

        private SimpleStringProperty assetType() {
            return assetType;
        }

        private SimpleStringProperty configured() {
            return configured;
        }

        private SimpleStringProperty available() {
            return available;
        }

        private SimpleStringProperty missingEnv() {
            return missingEnv;
        }

        private SimpleStringProperty lastChecked() {
            return lastChecked;
        }

        private SimpleStringProperty message() {
            return message;
        }
    }
}
