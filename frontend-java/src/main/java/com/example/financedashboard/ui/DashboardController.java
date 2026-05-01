package com.example.financedashboard.ui;

import com.example.financedashboard.api.ApiException;
import com.example.financedashboard.config.AppConfig;
import com.example.financedashboard.model.AnalysisSnapshot;
import com.example.financedashboard.model.DashboardSummary;
import com.example.financedashboard.model.Instrument;
import com.example.financedashboard.model.PricePoint;
import com.example.financedashboard.model.Quote;
import com.example.financedashboard.service.MarketDataService;
import com.example.financedashboard.ui.InstrumentTableController.WatchRow;
import com.example.financedashboard.ui.components.ErrorBanner;
import com.example.financedashboard.ui.components.LoadingOverlay;
import com.example.financedashboard.ui.components.MetricCard;
import com.example.financedashboard.util.DateUtils;
import com.example.financedashboard.util.FormatUtils;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Parent;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.SplitPane;
import javafx.scene.control.ToggleButton;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.util.Duration;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class DashboardController {
    private static final String BACKEND_OFFLINE_MESSAGE =
            "Backend API unavailable. Start backend: python -m fx_rates serve --host 127.0.0.1 --port 8000";

    private final AppConfig config;
    private final MarketDataService marketDataService;
    private final StatusBarController statusBarController = new StatusBarController();
    private final InstrumentTableController tableController = new InstrumentTableController();
    private final ChartController chartController = new ChartController();
    private final ErrorBanner errorBanner = new ErrorBanner();
    private final LoadingOverlay loadingOverlay = new LoadingOverlay();
    private final AtomicBoolean refreshing = new AtomicBoolean(false);

    private final MetricCard totalInstruments = new MetricCard("Total Instruments");
    private final MetricCard activeStocks = new MetricCard("Active Stocks");
    private final MetricCard activeCurrencies = new MetricCard("Active FX");
    private final MetricCard latestQuotes = new MetricCard("Latest Quotes");
    private final MetricCard failedRuns = new MetricCard("Failed Runs");
    private final MetricCard lastIngest = new MetricCard("Last Successful Ingest");

    private final Label quoteDetails = new Label("Select an instrument to inspect the latest quote.");
    private final Label analysisDetails = new Label("Analysis snapshot will appear here.");
    private final Button refreshButton = new Button("Refresh");
    private final ToggleButton pauseButton = new ToggleButton("Pause Auto");
    private Timeline pollingTimeline;
    private boolean hasLoadedData;

    public DashboardController(AppConfig config, MarketDataService marketDataService) {
        this.config = config;
        this.marketDataService = marketDataService;
        this.tableController.setOnSelection(this::loadInstrumentDetails);
    }

    public Parent createView() {
        BorderPane root = new BorderPane();
        VBox header = buildHeader();
        VBox content = buildContent();
        root.setTop(header);
        root.setCenter(content);

        StackPane stack = new StackPane(root, loadingOverlay);
        startPolling();
        Platform.runLater(this::refreshNow);
        return stack;
    }

    public void stop() {
        if (pollingTimeline != null) {
            pollingTimeline.stop();
        }
    }

    private VBox buildHeader() {
        Label title = new Label("Finance Monitor");
        Label subtitle = new Label("Local Market Intelligence Console");
        title.getStyleClass().add("app-title");
        subtitle.getStyleClass().add("app-subtitle");
        VBox header = new VBox(6, title, subtitle, statusBarController.getView());
        header.getStyleClass().add("app-header");
        statusBarController.setDisconnected("Waiting for first refresh");
        return header;
    }

    private VBox buildContent() {
        HBox metrics = new HBox(12, totalInstruments, activeStocks, activeCurrencies, latestQuotes, failedRuns, lastIngest);
        metrics.getStyleClass().add("metric-row");

        refreshButton.getStyleClass().add("neon-button");
        refreshButton.setOnAction(event -> refreshNow(true));
        pauseButton.getStyleClass().add("neon-button-secondary");
        pauseButton.setOnAction(event -> toggleAutoRefresh());

        HBox actions = new HBox(10, refreshButton, pauseButton);
        actions.setAlignment(Pos.CENTER_RIGHT);

        VBox tablePanel = new VBox(10, actions, tableController.getView());
        VBox.setVgrow(tableController.getView(), Priority.ALWAYS);

        VBox detailsPanel = buildDetailsPanel();
        SplitPane splitPane = new SplitPane(tablePanel, detailsPanel);
        splitPane.setDividerPositions(0.56);
        VBox.setVgrow(splitPane, Priority.ALWAYS);

        VBox content = new VBox(14, errorBanner, metrics, splitPane);
        content.getStyleClass().add("content");
        return content;
    }

    private VBox buildDetailsPanel() {
        Label title = new Label("Selected Instrument Details");
        title.getStyleClass().add("panel-title");

        GridPane detailGrid = new GridPane();
        detailGrid.setHgap(12);
        detailGrid.setVgap(12);
        detailGrid.add(detailCard("Latest Quote", quoteDetails), 0, 0);
        detailGrid.add(detailCard("Analysis", analysisDetails), 1, 0);

        VBox details = new VBox(12, title, detailGrid, chartController.getView());
        details.getStyleClass().add("panel");
        VBox.setVgrow(chartController.getView(), Priority.ALWAYS);
        return details;
    }

    private VBox detailCard(String title, Label body) {
        Label titleLabel = new Label(title);
        titleLabel.getStyleClass().add("metric-title");
        body.setWrapText(true);
        VBox card = new VBox(8, titleLabel, body);
        card.getStyleClass().add("metric-card");
        card.setPadding(new Insets(12));
        card.setMinWidth(220);
        return card;
    }

    private void startPolling() {
        pollingTimeline = new Timeline(new KeyFrame(Duration.seconds(config.getRefreshIntervalSeconds()), event -> refreshNow(false)));
        pollingTimeline.setCycleCount(Timeline.INDEFINITE);
        pollingTimeline.play();
    }

    private void refreshNow() {
        refreshNow(false);
    }

    private void refreshNow(boolean manual) {
        if (!refreshing.compareAndSet(false, true)) {
            return;
        }
        refreshButton.setDisable(true);
        refreshButton.setText("Refreshing...");
        statusBarController.setRefreshing(config.getApiBaseUrl());
        if (!hasLoadedData && manual) {
            loadingOverlay.show();
        }
        Task<DashboardData> task = new Task<>() {
            @Override
            protected DashboardData call() throws Exception {
                DashboardSummary summary = marketDataService.getDashboardSummary();
                List<Instrument> instruments = marketDataService.getInstruments("ALL", true, null);
                List<Quote> quotes = marketDataService.getLatestQuotes();
                List<AnalysisSnapshot> analysis = marketDataService.getLatestAnalysis();
                return new DashboardData(summary, instruments, quotes, analysis);
            }
        };
        task.setOnSucceeded(event -> {
            refreshing.set(false);
            hasLoadedData = true;
            loadingOverlay.hide();
            refreshButton.setDisable(false);
            refreshButton.setText("Refresh");
            applyDashboardData(task.getValue());
            errorBanner.hide();
            if (pauseButton.isSelected()) {
                statusBarController.setPaused(config.getApiBaseUrl());
            } else {
                statusBarController.setConnected(config.getApiBaseUrl());
            }
            statusBarController.setLastRefreshNow();
        });
        task.setOnFailed(event -> {
            refreshing.set(false);
            loadingOverlay.hide();
            refreshButton.setDisable(false);
            refreshButton.setText("Refresh");
            Throwable error = task.getException();
            System.err.println("Dashboard refresh failed: " + error.getMessage());
            errorBanner.showMessage(BACKEND_OFFLINE_MESSAGE + " | " + error.getMessage());
            statusBarController.setDisconnected("Last update failed");
            statusBarController.setLastRefreshFailed();
        });
        Thread worker = new Thread(task, "finance-dashboard-refresh");
        worker.setDaemon(true);
        worker.start();
    }

    private void toggleAutoRefresh() {
        if (pauseButton.isSelected()) {
            pauseButton.setText("Resume Auto");
            if (pollingTimeline != null) {
                pollingTimeline.pause();
            }
            statusBarController.setPaused(config.getApiBaseUrl());
        } else {
            pauseButton.setText("Pause Auto");
            if (pollingTimeline != null) {
                pollingTimeline.play();
            }
            statusBarController.setConnected(config.getApiBaseUrl());
        }
    }

    private void applyDashboardData(DashboardData data) {
        DashboardSummary summary = data.summary();
        totalInstruments.setValue(FormatUtils.integer(summary.totalInstruments()));
        activeStocks.setValue(FormatUtils.integer(summary.activeStocks()));
        activeCurrencies.setValue(FormatUtils.integer(summary.activeCurrencies()));
        latestQuotes.setValue(FormatUtils.integer(summary.latestQuoteCount()));
        failedRuns.setValue(FormatUtils.integer(summary.failedRunsCount()));
        lastIngest.setValue(summary.lastSuccessfulIngestRun() == null
                ? "-"
                : summary.lastSuccessfulIngestRun().mode() + " (" + FormatUtils.integer(summary.lastSuccessfulIngestRun().rowCount()) + " rows)");
        tableController.updateData(data.instruments(), data.quotes(), data.analysis());
    }

    private void loadInstrumentDetails(WatchRow row) {
        quoteDetails.setText("""
                Symbol: %s
                Price: %s
                Bid / Ask: %s / %s
                Change: %s
                Updated: %s
                """.formatted(
                row.symbol(),
                FormatUtils.price(row.price()),
                row.quote() == null ? "-" : FormatUtils.price(row.quote().bid()),
                row.quote() == null ? "-" : FormatUtils.price(row.quote().ask()),
                row.quote() == null ? "-" : FormatUtils.percent(row.quote().percentChange()),
                DateUtils.displayDateTime(row.lastUpdate())
        ));
        analysisDetails.setText("""
                Trend: %s
                Signal: %s
                Last close: %s
                SMA 20 / SMA 50: %s / %s
                Volatility 20: %s
                """.formatted(
                row.trend(),
                row.signal(),
                row.analysis() == null ? "-" : FormatUtils.price(row.analysis().lastClose()),
                row.analysis() == null ? "-" : FormatUtils.price(row.analysis().sma20()),
                row.analysis() == null ? "-" : FormatUtils.price(row.analysis().sma50()),
                row.analysis() == null ? "-" : FormatUtils.percent(row.analysis().volatility20())
        ));
        loadHistory(row);
    }

    private void loadHistory(WatchRow row) {
        LocalDate end = LocalDate.now();
        LocalDate start = end.minusDays(config.getHistoryLookbackDays());
        Task<List<PricePoint>> task = new Task<>() {
            @Override
            protected List<PricePoint> call() throws ApiException {
                if ("FX".equalsIgnoreCase(row.assetType())) {
                    return marketDataService.getFxHistory("USD", row.symbol(), start, end);
                }
                return marketDataService.getStockHistory(row.symbol(), start, end);
            }
        };
        task.setOnSucceeded(event -> chartController.showHistory(row.symbol(), task.getValue()));
        task.setOnFailed(event -> {
            System.err.println("History load failed: " + task.getException().getMessage());
            chartController.clear();
            errorBanner.showMessage("Could not load history for " + row.symbol() + ". It will retry on the next selection.");
        });
        Thread worker = new Thread(task, "finance-dashboard-history");
        worker.setDaemon(true);
        worker.start();
    }

    private record DashboardData(
            DashboardSummary summary,
            List<Instrument> instruments,
            List<Quote> quotes,
            List<AnalysisSnapshot> analysis
    ) {
    }
}
