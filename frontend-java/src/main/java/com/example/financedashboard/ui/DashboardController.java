package com.example.financedashboard.ui;

import com.example.financedashboard.api.ApiException;
import com.example.financedashboard.config.AppConfig;
import com.example.financedashboard.model.AnalysisSnapshot;
import com.example.financedashboard.model.ChartPoint;
import com.example.financedashboard.model.DashboardSummary;
import com.example.financedashboard.model.FixedChart;
import com.example.financedashboard.model.FixedCharts;
import com.example.financedashboard.model.Instrument;
import com.example.financedashboard.model.MarketOverview;
import com.example.financedashboard.model.PricePoint;
import com.example.financedashboard.model.Quote;
import com.example.financedashboard.model.TopStockPerformance;
import com.example.financedashboard.service.MarketDataService;
import com.example.financedashboard.service.MarketDataService.HistoryRange;
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
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.SplitPane;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.ToggleButton;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.util.Duration;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class DashboardController {
    private static final String BACKEND_OFFLINE_MESSAGE =
            "Backend API unavailable. Start backend: python -m fx_rates serve --host 127.0.0.1 --port 8000";
    private static final String PREPARE_DASHBOARD_MESSAGE =
            "No data loaded. Run: python -m fx_rates dashboard prepare-demo --years 4 --demo";

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
    private final MetricCard activeCrypto = new MetricCard("Active Crypto");
    private final MetricCard activeMacro = new MetricCard("Macro");
    private final MetricCard latestQuotes = new MetricCard("Latest Quotes");
    private final MetricCard failedRuns = new MetricCard("Failed Runs");
    private final MetricCard lastIngest = new MetricCard("Last Successful Ingest");

    private final HBox marketOverviewCards = new HBox(12);
    private final FlowPane fixedChartPane = new FlowPane(14, 14);
    private final VBox topStocksPanel = new VBox(10);
    private final Label quoteDetails = new Label("Select an instrument to inspect the latest quote.");
    private final Label analysisDetails = new Label("Analysis snapshot will appear here.");
    private final Button refreshButton = new Button("Refresh");
    private final ToggleButton pauseButton = new ToggleButton("Pause Auto");
    private final ComboBox<HistoryRange> historyRange = new ComboBox<>();
    private Timeline pollingTimeline;
    private boolean hasLoadedData;
    private final Map<String, List<PricePoint>> historyCache = new HashMap<>();
    private String lastHistoryKey;
    private String inFlightHistoryKey;
    private Task<List<PricePoint>> historyTask;

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
        refreshButton.getStyleClass().add("neon-button");
        refreshButton.setOnAction(event -> refreshNow(true));
        pauseButton.getStyleClass().add("neon-button-secondary");
        pauseButton.setOnAction(event -> toggleAutoRefresh());

        HBox actions = new HBox(10, refreshButton, pauseButton);
        actions.setAlignment(Pos.CENTER_RIGHT);

        VBox tablePanel = new VBox(10, tableController.getView());
        VBox.setVgrow(tableController.getView(), Priority.ALWAYS);

        VBox detailsPanel = buildDetailsPanel();
        SplitPane splitPane = new SplitPane(tablePanel, detailsPanel);
        splitPane.setDividerPositions(0.56);
        VBox.setVgrow(splitPane, Priority.ALWAYS);

        TabPane tabs = new TabPane();
        tabs.getStyleClass().add("dashboard-tabs");
        tabs.getTabs().add(tab("Overview", buildOverviewPanel()));
        tabs.getTabs().add(tab("Watchlist", splitPane));
        VBox.setVgrow(tabs, Priority.ALWAYS);

        VBox content = new VBox(14, errorBanner, actions, tabs);
        content.getStyleClass().add("content");
        return content;
    }

    private VBox buildOverviewPanel() {
        HBox metrics = new HBox(12, totalInstruments, activeStocks, activeCurrencies, activeCrypto, activeMacro, latestQuotes, failedRuns);
        metrics.getStyleClass().add("metric-row");

        Label overviewTitle = new Label("Market Overview");
        overviewTitle.getStyleClass().add("panel-title");
        marketOverviewCards.getStyleClass().add("metric-row");

        Label chartTitle = new Label("Fixed 30-Day Charts");
        chartTitle.getStyleClass().add("panel-title");
        fixedChartPane.getStyleClass().add("chart-grid");
        fixedChartPane.setPrefWrapLength(980);

        Label stockTitle = new Label("Top 10 Companies - 30 Day Performance");
        stockTitle.getStyleClass().add("panel-title");
        topStocksPanel.getStyleClass().add("panel");

        VBox overview = new VBox(14, metrics, section(overviewTitle, marketOverviewCards), section(chartTitle, fixedChartPane), topStocksPanel, lastIngest);
        overview.getStyleClass().add("overview-page");
        ScrollPane scrollPane = new ScrollPane(overview);
        scrollPane.setFitToWidth(true);
        scrollPane.getStyleClass().add("dashboard-scroll");
        VBox wrapper = new VBox(scrollPane);
        VBox.setVgrow(scrollPane, Priority.ALWAYS);
        return wrapper;
    }

    private Tab tab(String title, Parent content) {
        Tab tab = new Tab(title, content);
        tab.setClosable(false);
        return tab;
    }

    private VBox section(Label title, javafx.scene.Node body) {
        VBox section = new VBox(10, title, body);
        section.getStyleClass().add("panel");
        return section;
    }

    private VBox buildDetailsPanel() {
        Label title = new Label("Selected Instrument Details");
        title.getStyleClass().add("panel-title");
        historyRange.setItems(javafx.collections.FXCollections.observableArrayList(HistoryRange.THIRTY_D, HistoryRange.NINETY_D, HistoryRange.ONE_Y, HistoryRange.FOUR_Y));
        historyRange.setValue(HistoryRange.FOUR_Y);
        historyRange.setConverter(new javafx.util.StringConverter<>() {
            @Override
            public String toString(HistoryRange range) {
                return range == null ? "" : range.label();
            }

            @Override
            public HistoryRange fromString(String value) {
                return HistoryRange.FOUR_Y;
            }
        });
        historyRange.valueProperty().addListener((obs, oldValue, newValue) -> {
            lastHistoryKey = null;
            WatchRow selected = tableController.getSelected();
            if (selected != null) {
                loadHistory(selected);
            }
        });
        HBox titleRow = new HBox(10, title, historyRange);
        titleRow.setAlignment(Pos.CENTER_LEFT);

        GridPane detailGrid = new GridPane();
        detailGrid.setHgap(12);
        detailGrid.setVgap(12);
        detailGrid.add(detailCard("Latest Quote", quoteDetails), 0, 0);
        detailGrid.add(detailCard("Analysis", analysisDetails), 1, 0);

        VBox details = new VBox(12, titleRow, detailGrid, chartController.getView());
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
        if (manual) {
            historyCache.clear();
            lastHistoryKey = null;
            inFlightHistoryKey = null;
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
                MarketOverview overview = safeCall(() -> marketDataService.getMarketOverview(), new MarketOverview(null, List.of(), List.of(), PREPARE_DASHBOARD_MESSAGE));
                FixedCharts fixedCharts = safeCall(() -> marketDataService.getFixedCharts(), new FixedCharts(List.of(), List.of(), List.of()));
                List<TopStockPerformance> topStocks = safeCall(() -> marketDataService.getTopStocks30d(), List.of());
                return new DashboardData(summary, instruments, quotes, analysis, overview, fixedCharts, topStocks);
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
        activeCrypto.setValue(FormatUtils.integer(summary.activeCrypto()));
        activeMacro.setValue(FormatUtils.integer(summary.activeMacro()));
        latestQuotes.setValue(FormatUtils.integer(summary.latestQuoteCount()));
        failedRuns.setValue(FormatUtils.integer(summary.failedRunsCount()));
        lastIngest.setValue(summary.lastSuccessfulIngestRun() == null
                ? "-"
                : summary.lastSuccessfulIngestRun().mode() + " (" + FormatUtils.integer(summary.lastSuccessfulIngestRun().rowCount()) + " rows)");
        tableController.updateData(data.instruments(), data.quotes(), data.analysis());
        updateMarketOverview(data.marketOverview());
        updateFixedCharts(data.fixedCharts());
        updateTopStocks(data.topStocks());
    }

    private void updateMarketOverview(MarketOverview overview) {
        marketOverviewCards.getChildren().clear();
        if (overview == null || overview.cards() == null || overview.cards().isEmpty()) {
            Label empty = new Label("No market overview yet. Run backend ingestion or enable demo mode.");
            empty.setText(overview != null && overview.message() != null && !overview.message().isBlank()
                    ? overview.message()
                    : PREPARE_DASHBOARD_MESSAGE);
            empty.getStyleClass().add("empty-state");
            marketOverviewCards.getChildren().add(empty);
            return;
        }
        overview.cards().forEach(card -> {
            MetricCard metric = new MetricCard(card.label());
            String suffix = card.unit() == null || card.unit().isBlank() ? "" : " " + card.unit();
            metric.setValue(FormatUtils.price(card.value()) + suffix);
            metric.getStyleClass().add("overview-card");
            if ("up".equalsIgnoreCase(card.status())) {
                metric.getStyleClass().add("status-up");
            } else if ("down".equalsIgnoreCase(card.status())) {
                metric.getStyleClass().add("status-down");
            }
            marketOverviewCards.getChildren().add(metric);
        });
    }

    private void updateFixedCharts(FixedCharts fixedCharts) {
        fixedChartPane.getChildren().clear();
        if (fixedCharts == null) {
            fixedChartPane.getChildren().add(emptyState(PREPARE_DASHBOARD_MESSAGE));
            return;
        }
        List<FixedChart> charts = java.util.stream.Stream.of(fixedCharts.fx(), fixedCharts.crypto(), fixedCharts.macro())
                .filter(list -> list != null)
                .flatMap(List::stream)
                .toList();
        if (charts.isEmpty()) {
            fixedChartPane.getChildren().add(emptyState(PREPARE_DASHBOARD_MESSAGE));
            return;
        }
        charts.forEach(chart -> fixedChartPane.getChildren().add(miniChart(chart)));
    }

    private void updateTopStocks(List<TopStockPerformance> topStocks) {
        topStocksPanel.getChildren().clear();
        Label title = new Label("Top 10 Companies - 30 Day Performance");
        title.getStyleClass().add("panel-title");
        topStocksPanel.getChildren().add(title);
        if (topStocks == null || topStocks.isEmpty()) {
            topStocksPanel.getChildren().add(emptyState(PREPARE_DASHBOARD_MESSAGE));
            return;
        }
        for (TopStockPerformance stock : topStocks) {
            Label row = new Label("%s  %s  %s  trend=%s  signal=%s".formatted(
                    stock.symbol(),
                    FormatUtils.price(stock.latestPrice()),
                    FormatUtils.percent(stock.change30d()),
                    FormatUtils.text(stock.trend()),
                    FormatUtils.text(stock.signal())
            ));
            row.getStyleClass().add("stock-performance-row");
            topStocksPanel.getChildren().add(row);
        }
    }

    private VBox miniChart(FixedChart fixedChart) {
        Label title = new Label(fixedChart.title());
        title.getStyleClass().add("metric-title");
        CategoryAxis xAxis = new CategoryAxis();
        NumberAxis yAxis = new NumberAxis();
        LineChart<String, Number> mini = new LineChart<>(xAxis, yAxis);
        mini.setAnimated(false);
        mini.setLegendVisible(false);
        mini.setCreateSymbols(false);
        mini.setMinHeight(190);
        mini.setPrefWidth(320);
        xAxis.setTickLabelsVisible(false);
        xAxis.setTickMarkVisible(false);
        XYChart.Series<String, Number> series = new XYChart.Series<>();
        List<ChartPoint> points = fixedChart.points() == null ? List.of() : fixedChart.points();
        for (ChartPoint point : points) {
            if (point.value() != null) {
                series.getData().add(new XYChart.Data<>(point.date(), point.value()));
            }
        }
        if (!series.getData().isEmpty()) {
            mini.getData().add(series);
        }
        String emptyMessage = fixedChart.message() == null || fixedChart.message().isBlank()
                ? PREPARE_DASHBOARD_MESSAGE
                : fixedChart.message();
        VBox box = new VBox(8, title, series.getData().isEmpty() ? emptyState(emptyMessage) : mini);
        box.getStyleClass().add("mini-chart-card");
        return box;
    }

    private Label emptyState(String message) {
        Label label = new Label(message);
        label.getStyleClass().add("empty-state");
        label.setWrapText(true);
        return label;
    }

    private void loadInstrumentDetails(WatchRow row) {
        String trend = displayAnalysisValue(row.trend());
        String signal = displayAnalysisValue(row.signal());
        quoteDetails.setText("""
                Symbol: %s
                Name: %s
                Asset: %s
                Price: %s
                30D Change: %s
                Bid / Ask: %s / %s
                Quote Change: %s
                Updated: %s
                """.formatted(
                row.symbol(),
                FormatUtils.text(row.name()),
                FormatUtils.text(row.assetType()),
                FormatUtils.price(row.price()),
                row.analysis() == null ? "-" : FormatUtils.percent(row.analysis().change30d()),
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
                trend,
                signal,
                row.analysis() == null ? "-" : FormatUtils.price(row.analysis().lastClose()),
                row.analysis() == null ? "-" : FormatUtils.price(row.analysis().sma20()),
                row.analysis() == null ? "-" : FormatUtils.price(row.analysis().sma50()),
                row.analysis() == null ? "-" : FormatUtils.percent(row.analysis().volatility20())
        ));
        loadHistory(row);
    }

    private void loadHistory(WatchRow row) {
        LocalDate end = LocalDate.now();
        HistoryRange selectedRange = historyRange.getValue() == null ? HistoryRange.FOUR_Y : historyRange.getValue();
        LocalDate start = selectedRange.startDate(end);
        chartController.setPeriodLabel(selectedRange.label());
        String historyKey = "%s|%s|%s|%s|%s".formatted(row.assetType(), row.symbol(), selectedRange.label(), start, end);
        if (historyKey.equals(lastHistoryKey)) {
            return;
        }
        if (historyKey.equals(inFlightHistoryKey)) {
            return;
        }
        if (historyCache.containsKey(historyKey)) {
            chartController.showHistory(row.symbol(), historyCache.get(historyKey));
            lastHistoryKey = historyKey;
            return;
        }
        if (historyTask != null && historyTask.isRunning()) {
            historyTask.cancel();
        }

        Task<List<PricePoint>> task = new Task<>() {
            @Override
            protected List<PricePoint> call() throws ApiException {
                return marketDataService.getHistory(row.assetType(), row.symbol(), "USD", end, selectedRange);
            }
        };
        historyTask = task;
        inFlightHistoryKey = historyKey;
        task.setOnSucceeded(event -> {
            if (task.isCancelled() || !historyKey.equals(inFlightHistoryKey)) {
                return;
            }
            historyCache.put(historyKey, task.getValue());
            lastHistoryKey = historyKey;
            inFlightHistoryKey = null;
            chartController.showHistory(row.symbol(), task.getValue());
            updateHistoryCoverage(row, task.getValue());
        });
        task.setOnFailed(event -> {
            if (!historyKey.equals(inFlightHistoryKey)) {
                return;
            }
            inFlightHistoryKey = null;
            System.err.println("History load failed: " + task.getException().getMessage());
            chartController.clear();
            errorBanner.showMessage("Could not load history for " + row.symbol() + ". It will retry on the next selection.");
        });
        Thread worker = new Thread(task, "finance-dashboard-history");
        worker.setDaemon(true);
        worker.start();
    }

    private void updateHistoryCoverage(WatchRow row, List<PricePoint> points) {
        String baseText = analysisDetails.getText().replaceAll("(?s)\\nHistory:.*$", "");
        if (points == null || points.isEmpty()) {
            analysisDetails.setText(baseText + "\nHistory: Limited history available.");
            return;
        }
        String start = points.get(0).date();
        String end = points.get(points.size() - 1).date();
        analysisDetails.setText(baseText + "\nHistory: " + start + " to " + end);
    }

    private String displayAnalysisValue(String value) {
        if (value == null || value.isBlank()) {
            return "Analysis pending";
        }
        if ("UNKNOWN".equalsIgnoreCase(value)) {
            return "Not enough data";
        }
        return value;
    }

    private record DashboardData(
            DashboardSummary summary,
            List<Instrument> instruments,
            List<Quote> quotes,
            List<AnalysisSnapshot> analysis,
            MarketOverview marketOverview,
            FixedCharts fixedCharts,
            List<TopStockPerformance> topStocks
    ) {
    }

    private static <T> T safeCall(ThrowingSupplier<T> supplier, T fallback) {
        try {
            return supplier.get();
        } catch (Exception ex) {
            System.err.println("Optional dashboard endpoint failed: " + ex.getMessage());
            return fallback;
        }
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }
}
