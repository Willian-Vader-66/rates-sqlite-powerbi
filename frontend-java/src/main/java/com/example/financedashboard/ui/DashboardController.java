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
import com.example.financedashboard.model.SystemStatus;
import com.example.financedashboard.model.TopStockPerformance;
import com.example.financedashboard.service.MarketDataService;
import com.example.financedashboard.service.MarketDataService.HistoryRange;
import com.example.financedashboard.ui.InstrumentTableController.WatchRow;
import com.example.financedashboard.ui.components.ErrorBanner;
import com.example.financedashboard.ui.components.LoadingOverlay;
import com.example.financedashboard.ui.components.MetricCard;
import com.example.financedashboard.ui.components.PeriodSelector;
import com.example.financedashboard.ui.chart.InteractiveFinanceChart;
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
import javafx.scene.control.Tooltip;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.util.Duration;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
    private final FlowPane technicalHighlightsPane = new FlowPane(12, 12);
    private final VBox topStocksPanel = new VBox(10);
    private final VBox marketsPage = new VBox(14);
    private final VBox stocksPage = new VBox(14);
    private final VBox fxCryptoPage = new VBox(14);
    private final VBox macroPage = new VBox(14);
    private final Label overviewChartTitle = new Label("Market Snapshot - 90D");
    private final PeriodSelector overviewPeriod = new PeriodSelector(HistoryRange.NINETY_D);
    private final Label quoteDetails = new Label("Select an instrument to inspect the latest quote.");
    private final Label analysisDetails = new Label("Analysis snapshot will appear here.");
    private final Label dataModeBadge = new Label("DATA MODE: UNKNOWN");
    private final VBox settingsPanel = new VBox(12);
    private final Label settingsConnection = new Label("-");
    private final Label settingsDatabase = new Label("-");
    private final Label settingsCounts = new Label("-");
    private final Label settingsCoverage = new Label("-");
    private final Label settingsRuntime = new Label("-");
    private final Button testConnectionButton = new Button("Test Connection");
    private final Button refreshButton = new Button("Refresh");
    private final ToggleButton pauseButton = new ToggleButton("Pause Auto");
    private final PeriodSelector historyRange = new PeriodSelector(HistoryRange.ONE_Y);
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
        VBox brand = new VBox(3, title, subtitle);

        refreshButton.getStyleClass().add("neon-button");
        refreshButton.setOnAction(event -> refreshNow(true));
        pauseButton.getStyleClass().add("neon-button-secondary");
        pauseButton.setOnAction(event -> toggleAutoRefresh());
        dataModeBadge.getStyleClass().addAll("data-mode-badge", "data-mode-unknown");
        HBox actions = new HBox(10, dataModeBadge, refreshButton, pauseButton);
        actions.setAlignment(Pos.CENTER_RIGHT);

        HBox headerRow = new HBox(18, brand, statusBarController.getView(), actions);
        headerRow.setAlignment(Pos.CENTER_LEFT);
        HBox.setHgrow(statusBarController.getView(), Priority.ALWAYS);
        VBox header = new VBox(6, headerRow);
        header.getStyleClass().add("app-header");
        statusBarController.setDisconnected("Waiting for first refresh");
        return header;
    }

    private VBox buildContent() {
        VBox tablePanel = new VBox(10, tableController.getView());
        VBox.setVgrow(tableController.getView(), Priority.ALWAYS);

        VBox detailsPanel = buildDetailsPanel();
        SplitPane splitPane = new SplitPane(tablePanel, detailsPanel);
        splitPane.setDividerPositions(0.56);
        VBox.setVgrow(splitPane, Priority.ALWAYS);

        TabPane tabs = new TabPane();
        tabs.getStyleClass().add("dashboard-tabs");
        tabs.getTabs().add(tab("Overview", buildOverviewPanel()));
        tabs.getTabs().add(tab("Markets", buildDynamicPage(marketsPage)));
        tabs.getTabs().add(tab("Stocks", buildDynamicPage(stocksPage)));
        tabs.getTabs().add(tab("FX & Crypto", buildDynamicPage(fxCryptoPage)));
        tabs.getTabs().add(tab("Macro", buildDynamicPage(macroPage)));
        tabs.getTabs().add(tab("Watchlist", splitPane));
        tabs.getTabs().add(tab("Settings", buildSettingsPanel()));
        VBox.setVgrow(tabs, Priority.ALWAYS);

        VBox content = new VBox(14, errorBanner, tabs);
        content.getStyleClass().add("content");
        return content;
    }

    private VBox buildOverviewPanel() {
        HBox metrics = new HBox(12, totalInstruments, activeStocks, activeCurrencies, activeCrypto, activeMacro, latestQuotes, failedRuns);
        metrics.getStyleClass().add("metric-row");

        Label overviewTitle = new Label("Market Overview");
        overviewTitle.getStyleClass().add("panel-title");
        marketOverviewCards.getStyleClass().add("metric-row");

        overviewChartTitle.getStyleClass().add("panel-title");
        overviewPeriod.setOnChange(range -> {
            overviewChartTitle.setText("Market Snapshot - " + range.label());
            refreshNow(true);
        });
        HBox chartHeader = new HBox(12, overviewChartTitle, overviewPeriod);
        chartHeader.setAlignment(Pos.CENTER_LEFT);
        fixedChartPane.getStyleClass().add("chart-grid");
        fixedChartPane.setPrefWrapLength(980);

        Label stockTitle = new Label("Top 10 Companies - Selected Period Performance");
        stockTitle.getStyleClass().add("panel-title");
        topStocksPanel.getStyleClass().add("panel");
        Label technicalTitle = new Label("Technical Signals - 90D");
        technicalTitle.getStyleClass().add("panel-title");

        VBox overview = new VBox(14, metrics, section(overviewTitle, marketOverviewCards), section(chartHeader, fixedChartPane), section(technicalTitle, technicalHighlightsPane), topStocksPanel, lastIngest);
        overview.getStyleClass().add("overview-page");
        ScrollPane scrollPane = new ScrollPane(overview);
        scrollPane.setFitToWidth(true);
        scrollPane.getStyleClass().add("dashboard-scroll");
        VBox wrapper = new VBox(scrollPane);
        VBox.setVgrow(scrollPane, Priority.ALWAYS);
        return wrapper;
    }

    private VBox buildDynamicPage(VBox page) {
        page.getStyleClass().add("overview-page");
        page.getChildren().setAll(emptyState("Loading market data..."));
        ScrollPane scrollPane = new ScrollPane(page);
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

    private VBox section(javafx.scene.Node title, javafx.scene.Node body) {
        if (title instanceof Label label && !label.getStyleClass().contains("panel-title")) {
            label.getStyleClass().add("panel-title");
        }
        VBox section = new VBox(10, title, body);
        section.getStyleClass().add("panel");
        return section;
    }

    private VBox buildDetailsPanel() {
        Label title = new Label("Selected Instrument Details");
        title.getStyleClass().add("panel-title");
        historyRange.setOnChange(range -> {
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
        detailGrid.add(detailCard("Latest Value", quoteDetails), 0, 0);
        detailGrid.add(detailCard("Analysis", analysisDetails), 1, 0);

        VBox details = new VBox(12, titleRow, detailGrid, chartController.getView());
        details.getStyleClass().add("panel");
        VBox.setVgrow(chartController.getView(), Priority.ALWAYS);
        return details;
    }

    private VBox buildSettingsPanel() {
        Label title = new Label("Settings");
        title.getStyleClass().add("panel-title");
        testConnectionButton.getStyleClass().add("secondary-button");
        testConnectionButton.setOnAction(event -> refreshNow(true));
        settingsConnection.setWrapText(true);
        settingsDatabase.setWrapText(true);
        settingsCounts.setWrapText(true);
        settingsCoverage.setWrapText(true);
        settingsRuntime.setWrapText(true);
        settingsPanel.getStyleClass().add("panel");
        settingsPanel.getChildren().setAll(
                title,
                detailCard("Backend", settingsConnection),
                detailCard("Database", settingsDatabase),
                detailCard("Data Readiness", settingsCounts),
                detailCard("Coverage", settingsCoverage),
                detailCard("Runtime", settingsRuntime),
                testConnectionButton
        );
        return settingsPanel;
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
                HistoryRange selectedOverviewRange = overviewPeriod.getValue();
                DashboardSummary summary = marketDataService.getDashboardSummary();
                SystemStatus systemStatus = safeCall(marketDataService::getSystemStatus, null);
                List<Instrument> instruments = marketDataService.getInstruments("ALL", true, null);
                List<Quote> quotes = marketDataService.getLatestQuotes();
                List<AnalysisSnapshot> analysis = marketDataService.getLatestAnalysis();
                MarketOverview overview = safeCall(() -> marketDataService.getMarketOverview(selectedOverviewRange), new MarketOverview(null, List.of(), List.of(), PREPARE_DASHBOARD_MESSAGE));
                FixedCharts fixedCharts = safeCall(() -> marketDataService.getFixedCharts(selectedOverviewRange), new FixedCharts(List.of(), List.of(), List.of()));
                List<TopStockPerformance> topStocks = safeCall(() -> marketDataService.getTopStocks(selectedOverviewRange), List.of());
                return new DashboardData(summary, systemStatus, instruments, quotes, analysis, overview, fixedCharts, topStocks);
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
        applyMetricDataMode(effectiveDataMode(data.systemStatus(), data.summary()));
        tableController.updateData(data.instruments(), data.quotes(), data.analysis());
        updateMarketOverview(data.marketOverview());
        updateFixedCharts(data.fixedCharts());
        updateTopStocks(data.topStocks());
        updateDataModeBadge(data.systemStatus(), data.summary());
        updateSettings(data.systemStatus(), data.summary());
        List<MarketRow> rows = marketRows(data.instruments(), data.quotes(), data.analysis());
        updateTechnicalHighlights(rows);
        updateMarketsPage(data, rows);
        updateStocksPage(data, rows);
        updateFxCryptoPage(data, rows);
        updateMacroPage(data, rows);
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
            metric.setCaption(overviewPeriod.getValue().label() + " " + FormatUtils.percent(card.change()));
            String mode = dataModeOrUnknown(card.dataMode());
            metric.setBadge(dataModeBadgeText(mode, card.provider()), mode);
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
        Label title = new Label("Top 10 Companies - " + overviewPeriod.getValue().label() + " Performance");
        title.getStyleClass().add("panel-title");
        GridPane table = new GridPane();
        table.getStyleClass().add("stock-performance-table");
        table.setHgap(14);
        table.setVgap(8);
        addStockCell(table, 0, 0, "Rank", "table-header");
        addStockCell(table, 1, 0, "Symbol", "table-header");
        addStockCell(table, 2, 0, "Company", "table-header");
        addStockCell(table, 3, 0, "Latest Price", "table-header", "numeric-cell");
        addStockCell(table, 4, 0, "30D %", "table-header", "numeric-cell");
        addStockCell(table, 5, 0, "Trend", "table-header");
        addStockCell(table, 6, 0, "Signal", "table-header");
        topStocksPanel.getChildren().add(title);
        if (topStocks == null || topStocks.isEmpty()) {
            topStocksPanel.getChildren().add(emptyState(PREPARE_DASHBOARD_MESSAGE));
            return;
        }
        int rank = 1;
        for (TopStockPerformance stock : topStocks) {
            int row = rank;
            addStockCell(table, 0, row, "#" + rank, "stock-performance-row");
            addStockCell(table, 1, row, stock.symbol(), "stock-performance-row", "symbol-cell");
            addStockCell(table, 2, row, FormatUtils.text(stock.name()), "stock-performance-row");
            addStockCell(table, 3, row, FormatUtils.price(stock.latestPrice()), "stock-performance-row", "numeric-cell");
            String movementClass = stock.change30d() != null && stock.change30d() < 0 ? "negative" : "positive";
            addStockCell(table, 4, row, FormatUtils.percent(stock.change30d()), "stock-performance-row", "numeric-cell", movementClass);
            addStockCell(table, 5, row, FormatUtils.text(stock.trend()), "stock-performance-row");
            addStockCell(table, 6, row, FormatUtils.text(stock.signal()), "stock-performance-row");
            rank++;
        }
        topStocksPanel.getChildren().add(table);
    }

    private VBox miniChart(FixedChart fixedChart) {
        String titleText = fixedChart.chartTitle() == null || fixedChart.chartTitle().isBlank()
                ? fixedChart.title()
                : fixedChart.chartTitle() + " - " + FormatUtils.text(fixedChart.period());
        Label title = new Label(titleText);
        title.getStyleClass().add("metric-title");
        Label subtitle = new Label(FormatUtils.text(fixedChart.chartSubtitle()));
        subtitle.getStyleClass().add("metric-caption");
        List<ChartPoint> points = fixedChart.points() == null ? List.of() : fixedChart.points();
        String emptyMessage = fixedChart.message() == null || fixedChart.message().isBlank()
                ? PREPARE_DASHBOARD_MESSAGE
                : fixedChart.message();
        InteractiveFinanceChart chart = new InteractiveFinanceChart();
        chart.setMinHeight(205);
        chart.setPrefHeight(230);
        chart.setChartPoints(
                FormatUtils.text(fixedChart.displayPair()).replace("-", FormatUtils.text(fixedChart.symbol())),
                FormatUtils.text(fixedChart.period()),
                points,
                fixedChart.valueFormat(),
                fixedChart.displayUnit(),
                fixedChart.tooltipLabel()
        );
        VBox box = new VBox(8, title, subtitle, points.isEmpty() ? emptyState(emptyMessage) : chart);
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
                Pair / Unit: %s
                Price / Rate: %s
                30D Change: %s
                Bid / Ask: %s / %s
                Quote Change: %s
                Updated: %s
                """.formatted(
                row.symbol(),
                FormatUtils.text(row.name()),
                FormatUtils.text(row.assetType()),
                FormatUtils.text(row.displayPairOrUnit()),
                row.formattedPrice(),
                row.analysis() == null ? "-" : FormatUtils.percent(row.analysis().change30d()),
                row.quote() == null ? "-" : FormatUtils.valueWithUnit(row.quote().bid(), row.valueFormat(), row.displayUnit()),
                row.quote() == null ? "-" : FormatUtils.valueWithUnit(row.quote().ask(), row.valueFormat(), row.displayUnit()),
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
                row.analysis() == null ? "-" : FormatUtils.valueWithUnit(row.analysis().lastClose(), row.valueFormat(), row.displayUnit()),
                row.analysis() == null ? "-" : FormatUtils.valueWithUnit(row.analysis().sma20(), row.valueFormat(), row.displayUnit()),
                row.analysis() == null ? "-" : FormatUtils.valueWithUnit(row.analysis().sma50(), row.valueFormat(), row.displayUnit()),
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
            chartController.showHistory(row.displayPairOrUnit(), historyCache.get(historyKey));
            lastHistoryKey = historyKey;
            return;
        }
        if (historyTask != null && historyTask.isRunning()) {
            historyTask.cancel();
        }
        chartController.showLoading(row.displayPairOrUnit());

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
            chartController.showHistory(row.displayPairOrUnit(), task.getValue());
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
            analysisDetails.setText(baseText + "\nHistory: Limited history available.\nPoints: 0");
            return;
        }
        String start = points.get(0).date();
        String end = points.get(points.size() - 1).date();
        analysisDetails.setText(baseText + "\nHistory: " + start + " to " + end + "\nPoints: " + points.size());
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

    private void updateTechnicalHighlights(List<MarketRow> rows) {
        technicalHighlightsPane.getChildren().clear();
        List<MarketRow> scored = rows.stream()
                .filter(row -> row.technicalScore() != null)
                .toList();
        MarketRow positive = scored.stream().max(Comparator.comparing(row -> row.technicalScore())).orElse(null);
        MarketRow negative = scored.stream().min(Comparator.comparing(row -> row.technicalScore())).orElse(null);
        MarketRow breakout = scored.stream().filter(row -> "BREAKOUT".equalsIgnoreCase(row.signal())).findFirst().orElse(positive);
        MarketRow drawdown = scored.stream().filter(row -> "DRAWDOWN".equalsIgnoreCase(row.signal())).findFirst().orElse(negative);
        MarketRow stable = scored.stream().filter(row -> "STABLE".equalsIgnoreCase(row.signal())).findFirst().orElse(null);
        addTechnicalCard("Positive Momentum", positive, "positive");
        addTechnicalCard("Negative Momentum", negative, "negative");
        addTechnicalCard("Breakout Watch", breakout, "positive");
        addTechnicalCard("Drawdown Risk", drawdown, "negative");
        addTechnicalCard("Stable / Neutral", stable, "neutral");
        if (technicalHighlightsPane.getChildren().isEmpty()) {
            technicalHighlightsPane.getChildren().add(emptyState("Technical signals will appear after analysis snapshots are generated."));
        }
    }

    private void addTechnicalCard(String title, MarketRow row, String tone) {
        if (row == null) {
            return;
        }
        MetricCard card = metricCard(
                title,
                row.symbol() + " | " + row.technicalLabel(),
                FormatUtils.percent(row.change30d()) + " - " + row.technicalSummary(),
                tone
        );
        card.setMinWidth(225);
        technicalHighlightsPane.getChildren().add(card);
    }

    private void updateMarketsPage(DashboardData data, List<MarketRow> rows) {
        List<MarketRow> ranked = rows.stream()
                .filter(row -> row.change30d() != null)
                .sorted(Comparator.comparing(MarketRow::change30d).reversed())
                .toList();
        List<MarketRow> positive = ranked.stream().limit(5).toList();
        List<MarketRow> negative = ranked.stream().sorted(Comparator.comparing(MarketRow::change30d)).limit(5).toList();

        FlowPane cards = new FlowPane(12, 12);
        if (data.marketOverview() != null && data.marketOverview().cards() != null) {
            data.marketOverview().cards().forEach(card -> cards.getChildren().add(metricCard(
                    card.label(),
                    FormatUtils.price(card.value()) + (card.unit() == null ? "" : " " + card.unit()),
                    overviewPeriod.getValue().label() + " " + FormatUtils.percent(card.change()),
                    card.status()
            )));
        }

        marketsPage.getChildren().setAll(
                pageTitle("Markets", "Cross-asset snapshot across stocks, FX, crypto and macro indicators."),
                cards,
                section(new Label("Market Snapshot"), dataTable(
                        List.of("Asset", "Symbol", "Name", "Latest Price", overviewPeriod.getValue().label() + " Change", "Trend", "Signal", "Last Update"),
                        rows.stream().limit(18).map(this::marketTableRow).toList(),
                        Set.of(3, 4),
                        Set.of(4)
                )),
                twoColumnPanels(
                        section(new Label("Top 5 Positive " + overviewPeriod.getValue().label() + " Changes"), compactRanking(positive)),
                        section(new Label("Top 5 Negative " + overviewPeriod.getValue().label() + " Changes"), compactRanking(negative))
                )
        );
    }

    private void updateStocksPage(DashboardData data, List<MarketRow> rows) {
        List<MarketRow> stocks = rows.stream().filter(row -> "STOCK".equalsIgnoreCase(row.assetType())).toList();
        MarketRow best = stocks.stream().filter(row -> row.change30d() != null).max(Comparator.comparing(MarketRow::change30d)).orElse(null);
        MarketRow worst = stocks.stream().filter(row -> row.change30d() != null).min(Comparator.comparing(MarketRow::change30d)).orElse(null);
        MarketRow volatileStock = stocks.stream().filter(row -> row.volatility() != null).max(Comparator.comparing(MarketRow::volatility)).orElse(null);

        String worstLabel = worst != null && worst.change30d() != null && worst.change30d() >= 0
                ? "Weakest Stock " + overviewPeriod.getValue().label()
                : "Worst Stock " + overviewPeriod.getValue().label();
        String worstStyle = worst != null && worst.change30d() != null && worst.change30d() >= 0 ? null : "down";
        FlowPane cards = new FlowPane(12, 12,
                metricCard("Active Stocks", FormatUtils.integer(data.summary().activeStocks()), "listed instruments", null),
                metricCard("Latest Stock Quotes", FormatUtils.integer(stocks.stream().filter(row -> row.quote() != null).count()), "quotes loaded", null),
                metricCard("Best Stock " + overviewPeriod.getValue().label(), best == null ? "-" : best.symbol(), best == null ? "-" : FormatUtils.percent(best.change30d()), "up"),
                metricCard(worstLabel, worst == null ? "-" : worst.symbol(), worst == null ? "-" : FormatUtils.percent(worst.change30d()), worstStyle),
                metricCard("Most Volatile", volatileStock == null ? "-" : volatileStock.symbol(), volatileStock == null ? "-" : FormatUtils.percent(volatileStock.volatility()), null)
        );

        stocksPage.getChildren().setAll(
                pageTitle("Stocks", "Dedicated equity view with sector, exchange, signal and momentum data."),
                cards,
                section(new Label("Stock Table"), dataTable(
                        List.of("Symbol", "Company", "Exchange", "Sector", "Price", overviewPeriod.getValue().label() + " Change", "Trend", "Signal", "Last Update"),
                        stocks.stream().map(row -> List.of(
                                row.symbol(),
                                row.name(),
                                row.exchange(),
                                row.sector(),
                                row.formattedPrice(),
                                FormatUtils.percent(row.change30d()),
                                row.trend(),
                                row.signal(),
                                DateUtils.displayDateTime(row.lastUpdate())
                        )).toList(),
                        Set.of(4, 5),
                        Set.of(5)
                )),
                section(new Label("Stock Momentum Charts"), topStockCharts(data.topStocks()))
        );
    }

    private void updateFxCryptoPage(DashboardData data, List<MarketRow> rows) {
        List<MarketRow> fxCrypto = rows.stream()
                .filter(row -> "FX".equalsIgnoreCase(row.assetType()) || "CRYPTO".equalsIgnoreCase(row.assetType()))
                .toList();
        FlowPane cards = new FlowPane(12, 12,
                metricCard("Active FX", FormatUtils.integer(data.summary().activeCurrencies()), "currency pairs", null),
                metricCard("Active Crypto", FormatUtils.integer(data.summary().activeCrypto()), "crypto assets", null)
        );
        addOverviewCard(cards, data.marketOverview(), "USD/BRL");
        addOverviewCard(cards, data.marketOverview(), "USD/EUR");
        addOverviewCard(cards, data.marketOverview(), "BTC/USD");
        addOverviewCard(cards, data.marketOverview(), "ETH/USD");

        FlowPane charts = new FlowPane(14, 14);
        if (data.fixedCharts() != null) {
            java.util.stream.Stream.of(data.fixedCharts().fx(), data.fixedCharts().crypto())
                    .filter(Objects::nonNull)
                    .flatMap(List::stream)
                    .forEach(chart -> charts.getChildren().add(miniChart(chart)));
        }

        fxCryptoPage.getChildren().setAll(
                pageTitle("FX & Crypto", "Currencies and digital assets with latest quotes, signals and selected-period charts."),
                cards,
                section(new Label("FX & Crypto Snapshot"), dataTable(
                        List.of("Type", "Symbol", "Name", "Price", overviewPeriod.getValue().label() + " Change", "Trend", "Signal", "Last Update"),
                        fxCrypto.stream().map(row -> List.of(
                                row.assetType(),
                                row.symbol(),
                                row.name(),
                                row.formattedPrice(),
                                FormatUtils.percent(row.change30d()),
                                row.trend(),
                                row.signal(),
                                DateUtils.displayDateTime(row.lastUpdate())
                        )).toList(),
                        Set.of(3, 4),
                        Set.of(4)
                )),
                section(new Label("Key FX & Crypto Charts"), charts)
        );
    }

    private void updateMacroPage(DashboardData data, List<MarketRow> rows) {
        List<MarketRow> macro = rows.stream().filter(row -> "MACRO".equalsIgnoreCase(row.assetType())).toList();
        FlowPane cards = new FlowPane(12, 12,
                metricCard("Macro Indicators", FormatUtils.integer(data.summary().activeMacro()), "active series", null),
                metricCard("Latest Update", latestUpdate(macro), "from latest quote/analysis", null)
        );
        for (String key : List.of("SELIC", "CDI", "IPCA", "FED")) {
            macro.stream()
                    .filter(row -> row.symbol().contains(key))
                    .findFirst()
                    .ifPresent(row -> cards.getChildren().add(metricCard(row.symbol(), row.formattedPrice(), FormatUtils.percent(row.change30d()), null)));
        }

        FlowPane charts = new FlowPane(14, 14);
        if (data.fixedCharts() != null && data.fixedCharts().macro() != null) {
            data.fixedCharts().macro().forEach(chart -> charts.getChildren().add(miniChart(chart)));
        }
        if (charts.getChildren().isEmpty()) {
            charts.getChildren().add(emptyState("Histórico insuficiente para este indicador"));
        }

        macroPage.getChildren().setAll(
                pageTitle("Macro", "Macroeconomic indicators for rates and inflation context."),
                cards,
                section(new Label("Macro Indicators"), dataTable(
                        List.of("Indicator", "Name", "Value", overviewPeriod.getValue().label() + " Change", "Last Update"),
                        macro.stream().map(row -> List.of(
                                row.symbol(),
                                row.name(),
                                row.formattedPrice(),
                                FormatUtils.percent(row.change30d()),
                                DateUtils.displayDateTime(row.lastUpdate())
                        )).toList(),
                        Set.of(2, 3),
                        Set.of(3)
                )),
                section(new Label("Macro Charts"), charts)
        );
    }

    private Label pageTitle(String title, String subtitle) {
        Label label = new Label(title + "\n" + subtitle);
        label.getStyleClass().add("page-title");
        label.setWrapText(true);
        return label;
    }

    private MetricCard metricCard(String title, String value, String caption, String status) {
        MetricCard card = new MetricCard(title);
        card.setValue(value);
        card.setCaption(caption);
        card.getStyleClass().add("overview-card");
        if ("up".equalsIgnoreCase(status) || "positive".equalsIgnoreCase(status)) {
            card.getStyleClass().add("status-up");
        } else if ("down".equalsIgnoreCase(status) || "negative".equalsIgnoreCase(status)) {
            card.getStyleClass().add("status-down");
        }
        return card;
    }

    private void addOverviewCard(FlowPane cards, MarketOverview overview, String label) {
        if (overview == null || overview.cards() == null) {
            return;
        }
        overview.cards().stream()
                .filter(card -> label.equalsIgnoreCase(card.label()))
                .findFirst()
                .ifPresent(card -> cards.getChildren().add(metricCard(
                        card.label(),
                        FormatUtils.price(card.value()) + (card.unit() == null ? "" : " " + card.unit()),
                        overviewPeriod.getValue().label() + " " + FormatUtils.percent(card.change()),
                        card.status()
                )));
    }

    private GridPane dataTable(List<String> headers, List<List<String>> rows, Set<Integer> numericColumns, Set<Integer> movementColumns) {
        GridPane table = new GridPane();
        table.getStyleClass().add("data-table");
        table.setHgap(12);
        table.setVgap(7);
        for (int col = 0; col < headers.size(); col++) {
            addDataCell(table, col, 0, headers.get(col), "table-header");
        }
        int rowIndex = 1;
        for (List<String> row : rows) {
            for (int col = 0; col < row.size(); col++) {
                List<String> styles = new ArrayList<>(List.of("stock-performance-row"));
                if (numericColumns.contains(col)) {
                    styles.add("numeric-cell");
                }
                if (movementColumns.contains(col)) {
                    String value = row.get(col);
                    styles.add(value != null && value.startsWith("-") ? "negative" : "positive");
                }
                addDataCell(table, col, rowIndex, row.get(col), styles.toArray(new String[0]));
            }
            rowIndex++;
        }
        if (rows.isEmpty()) {
            addDataCell(table, 0, 1, PREPARE_DASHBOARD_MESSAGE, "empty-state");
        }
        return table;
    }

    private void addDataCell(GridPane table, int col, int row, String text, String... styleClasses) {
        Label label = new Label(text == null || text.isBlank() ? "-" : text);
        label.setMaxWidth(Double.MAX_VALUE);
        label.getStyleClass().addAll(styleClasses);
        if (java.util.Arrays.asList(styleClasses).contains("numeric-cell")) {
            label.setAlignment(Pos.CENTER_RIGHT);
        }
        table.add(label, col, row);
    }

    private HBox twoColumnPanels(VBox left, VBox right) {
        HBox box = new HBox(14, left, right);
        HBox.setHgrow(left, Priority.ALWAYS);
        HBox.setHgrow(right, Priority.ALWAYS);
        return box;
    }

    private VBox compactRanking(List<MarketRow> rows) {
        VBox box = new VBox(6);
        for (MarketRow row : rows) {
            Label label = new Label("%s  %s  %s".formatted(row.symbol(), FormatUtils.percent(row.change30d()), row.signal()));
            label.getStyleClass().add(row.change30d() != null && row.change30d() < 0 ? "negative" : "positive");
            box.getChildren().add(label);
        }
        if (box.getChildren().isEmpty()) {
            box.getChildren().add(emptyState("No ranked market data available."));
        }
        return box;
    }

    private FlowPane topStockCharts(List<TopStockPerformance> stocks) {
        FlowPane charts = new FlowPane(14, 14);
        if (stocks == null) {
            charts.getChildren().add(emptyState(PREPARE_DASHBOARD_MESSAGE));
            return charts;
        }
        stocks.stream().limit(3).forEach(stock -> charts.getChildren().add(stockMiniChart(stock)));
        if (charts.getChildren().isEmpty()) {
            charts.getChildren().add(emptyState(PREPARE_DASHBOARD_MESSAGE));
        }
        return charts;
    }

    private VBox stockMiniChart(TopStockPerformance stock) {
        FixedChart chart = new FixedChart(
                "stock_" + stock.symbol(),
                stock.symbol() + " - " + overviewPeriod.getValue().label(),
                "STOCK",
                null,
                stock.symbol(),
                stock.name(),
                stock.symbol(),
                "USD",
                stock.symbol() + "/USD",
                "USD",
                "currency_usd",
                stock.name() + " (" + stock.symbol() + ")",
                "Stock price in USD",
                "Price (USD)",
                stock.symbol() + " price",
                overviewPeriod.getValue().label(),
                stock.points() == null || stock.points().isEmpty() ? null : stock.points().get(0).date(),
                stock.points() == null || stock.points().isEmpty() ? null : stock.points().get(stock.points().size() - 1).date(),
                stock.points() == null ? 0 : stock.points().size(),
                stock.points(),
                stock.points() == null || stock.points().isEmpty() ? "No stock history available." : null
        );
        return miniChart(chart);
    }

    private List<String> marketTableRow(MarketRow row) {
        return List.of(
                row.assetType(),
                row.symbol(),
                row.name(),
                row.formattedPrice(),
                FormatUtils.percent(row.change30d()),
                row.trend(),
                row.signal(),
                DateUtils.displayDateTime(row.lastUpdate())
        );
    }

    private String latestUpdate(List<MarketRow> rows) {
        return rows.stream()
                .map(MarketRow::lastUpdate)
                .filter(Objects::nonNull)
                .max(String::compareTo)
                .map(DateUtils::displayDateTime)
                .orElse("-");
    }

    private List<MarketRow> marketRows(List<Instrument> instruments, List<Quote> quotes, List<AnalysisSnapshot> analysis) {
        Map<String, Quote> quoteByKey = new LinkedHashMap<>();
        for (Quote quote : quotes) {
            quoteByKey.put(key(quote.symbol(), quote.assetType(), quote.exchange()), quote);
            quoteByKey.putIfAbsent(quote.symbol(), quote);
        }
        Map<String, AnalysisSnapshot> analysisByKey = new LinkedHashMap<>();
        for (AnalysisSnapshot snapshot : analysis) {
            analysisByKey.put(key(snapshot.symbol(), snapshot.assetType(), snapshot.exchange()), snapshot);
            analysisByKey.putIfAbsent(snapshot.symbol(), snapshot);
        }
        return instruments.stream()
                .map(instrument -> new MarketRow(
                        instrument,
                        firstMatch(quoteByKey, instrument),
                        firstMatch(analysisByKey, instrument)
                ))
                .sorted(Comparator.comparing(MarketRow::assetType).thenComparing(MarketRow::symbol))
                .toList();
    }

    private static <T> T firstMatch(Map<String, T> map, Instrument instrument) {
        T exact = map.get(key(instrument.symbol(), instrument.assetType(), instrument.exchange()));
        if (exact != null) {
            return exact;
        }
        return map.get(instrument.symbol());
    }

    private static String key(String symbol, String assetType, String exchange) {
        return String.join("|", Objects.toString(symbol, ""), Objects.toString(assetType, ""), Objects.toString(exchange, ""));
    }

    private void updateSettings(SystemStatus status, DashboardSummary summary) {
        settingsConnection.setText("""
                API base URL: %s
                HTTP timeout: %s seconds
                Refresh interval: %s seconds
                Auto-refresh: %s
                """.formatted(
                config.getApiBaseUrl(),
                config.getHttpTimeoutSeconds(),
                config.getRefreshIntervalSeconds(),
                pauseButton.isSelected() ? "paused" : "enabled"
        ));
        if (status == null) {
            settingsDatabase.setText("Database status unavailable. Use the Test Connection button or check /api/system/status.");
            settingsCounts.setText("Summary instruments: " + FormatUtils.integer(summary.totalInstruments()));
            settingsCoverage.setText("-");
            settingsRuntime.setText("Recommended prepare command: " + PREPARE_DASHBOARD_MESSAGE);
            return;
        }
        settingsDatabase.setText("""
                DB path: %s
                Exists: %s
                Size: %s
                Empty: %s
                """.formatted(
                FormatUtils.text(status.dbPath()),
                Boolean.TRUE.equals(status.dbExists()) ? "yes" : "no",
                FormatUtils.bytes(status.dbSizeBytes()),
                Boolean.TRUE.equals(status.empty()) ? "yes" : "no"
        ));
        settingsCounts.setText("""
                Instruments: %s
                Stocks / FX / Crypto / Macro: %s / %s / %s / %s
                Latest quotes: %s
                Latest analysis: %s
                Historical rows: %s
                """.formatted(
                FormatUtils.integer(status.totalInstruments()),
                FormatUtils.integer(status.activeStocks()),
                FormatUtils.integer(status.activeCurrencies()),
                FormatUtils.integer(status.activeCrypto()),
                FormatUtils.integer(status.activeMacro()),
                FormatUtils.integer(status.latestQuoteCount()),
                FormatUtils.integer(status.latestAnalysisCount()),
                FormatUtils.integer(status.historicalRowCount())
        ));
        String coverage = status.coverage() == null
                ? "History: %s to %s".formatted(FormatUtils.text(status.dateMin()), FormatUtils.text(status.dateMax()))
                : "History: %s to %s\nRows: %s".formatted(
                FormatUtils.text(status.coverage().dateMin()),
                FormatUtils.text(status.coverage().dateMax()),
                FormatUtils.integer(status.coverage().historicalRows())
        );
        settingsCoverage.setText(coverage);
        String dataMode = effectiveDataMode(status, summary);
        String providers = status.providers() == null || status.providers().isEmpty() ? "-" : String.join(", ", status.providers());
        String providerSummary = formatProviderSummary(status.providerSummary());
        String modeCounts = formatDataModeCounts(status.dataModeCounts());
        settingsRuntime.setText("""
                Data mode: %s
                Providers: %s
                Provider details: %s
                Mode counts: %s
                Live provider status: %s
                Data timestamp: %s
                Warning: %s
                Recommended prepare command: %s
                """.formatted(
                dataMode.toUpperCase(),
                providers,
                providerSummary,
                modeCounts,
                formatLiveProviderStatus(status.liveProviderStatus()),
                FormatUtils.text(status.dataGeneratedAt()),
                FormatUtils.text(status.dataWarning()),
                FormatUtils.text(status.recommendedPrepareCommand())
        ));
    }

    private void updateDataModeBadge(SystemStatus status, DashboardSummary summary) {
        String mode = effectiveDataMode(status, summary);
        dataModeBadge.setText(mode.toUpperCase() + " DATA");
        dataModeBadge.getStyleClass().removeAll("data-mode-demo", "data-mode-live", "data-mode-mixed", "data-mode-unknown");
        dataModeBadge.getStyleClass().add("data-mode-" + mode);
        Tooltip.install(dataModeBadge, new Tooltip(dataModeTooltip(status, summary, mode)));
    }

    private String effectiveDataMode(SystemStatus status, DashboardSummary summary) {
        String mode = status != null ? status.dataMode() : null;
        if ((mode == null || mode.isBlank()) && summary != null) {
            mode = summary.dataMode();
        }
        if (mode == null || mode.isBlank()) {
            return "unknown";
        }
        return mode.trim().toLowerCase();
    }

    private void applyMetricDataMode(String mode) {
        String label = switch (mode) {
            case "demo" -> "Demo";
            case "live" -> "Live";
            case "mixed" -> "Mixed";
            default -> "Unknown";
        };
        for (MetricCard card : List.of(totalInstruments, activeStocks, activeCurrencies, activeCrypto, activeMacro, latestQuotes, failedRuns, lastIngest)) {
            card.setBadge(label, mode);
        }
    }

    private String dataModeOrUnknown(String mode) {
        return mode == null || mode.isBlank() ? "unknown" : mode.trim().toLowerCase();
    }

    private String dataModeBadgeText(String mode, String provider) {
        if ("live".equals(mode) && provider != null && !provider.isBlank()) {
            return provider;
        }
        return switch (mode) {
            case "demo" -> "Demo";
            case "live" -> "Live";
            case "mixed" -> "Mixed";
            default -> "Unknown";
        };
    }

    private String dataModeTooltip(SystemStatus status, DashboardSummary summary, String mode) {
        String warning = status != null ? status.dataWarning() : null;
        if ((warning == null || warning.isBlank()) && summary != null) {
            warning = summary.warning();
        }
        String providers = status != null && status.providers() != null && !status.providers().isEmpty()
                ? String.join(", ", status.providers())
                : "-";
        return "Mode: %s\nProviders: %s\n%s".formatted(mode.toUpperCase(), providers, FormatUtils.text(warning));
    }

    private String formatProviderSummary(List<SystemStatus.ProviderSummary> summary) {
        if (summary == null || summary.isEmpty()) {
            return "-";
        }
        return summary.stream()
                .map(item -> item.assetType() + "=" + (item.providers() == null || item.providers().isEmpty() ? "-" : String.join("/", item.providers())))
                .reduce((left, right) -> left + "; " + right)
                .orElse("-");
    }

    private String formatDataModeCounts(SystemStatus.DataModeCounts counts) {
        if (counts == null) {
            return "-";
        }
        return "demo=%s live=%s mixed=%s unknown=%s".formatted(
                FormatUtils.integer(counts.demo()),
                FormatUtils.integer(counts.live()),
                FormatUtils.integer(counts.mixed()),
                FormatUtils.integer(counts.unknown())
        );
    }

    private String formatLiveProviderStatus(SystemStatus.LiveProviderStatus status) {
        if (status == null || status.providers() == null || status.providers().isEmpty()) {
            return "-";
        }
        return status.providers().stream()
                .map(item -> {
                    String missing = item.missingEnv() == null || item.missingEnv().isEmpty() ? "" : " missing=" + String.join("/", item.missingEnv());
                    String configured = Boolean.TRUE.equals(item.configured()) ? "configured" : "missing";
                    return "%s:%s/%s%s".formatted(item.assetType(), item.provider(), configured, missing);
                })
                .reduce((left, right) -> left + "; " + right)
                .orElse("-");
    }

    private void addStockCell(GridPane table, int col, int row, String text, String... styleClasses) {
        Label label = new Label(text == null || text.isBlank() ? "-" : text);
        label.setMaxWidth(Double.MAX_VALUE);
        label.getStyleClass().addAll(styleClasses);
        if (java.util.Arrays.asList(styleClasses).contains("numeric-cell")) {
            label.setAlignment(Pos.CENTER_RIGHT);
        }
        table.add(label, col, row);
    }

    private record DashboardData(
            DashboardSummary summary,
            SystemStatus systemStatus,
            List<Instrument> instruments,
            List<Quote> quotes,
            List<AnalysisSnapshot> analysis,
            MarketOverview marketOverview,
            FixedCharts fixedCharts,
            List<TopStockPerformance> topStocks
    ) {
    }

    private record MarketRow(Instrument instrument, Quote quote, AnalysisSnapshot analysis) {
        String symbol() {
            return instrument.symbol();
        }

        String name() {
            return FormatUtils.text(instrument.name());
        }

        String assetType() {
            return FormatUtils.text(instrument.assetType());
        }

        String exchange() {
            return FormatUtils.text(instrument.exchange());
        }

        String sector() {
            return FormatUtils.text(instrument.sector());
        }

        Double price() {
            if (quote != null && quote.price() != null) {
                return quote.price();
            }
            return analysis == null ? null : analysis.lastPrice();
        }

        String displayPair() {
            if (quote != null && quote.displayPair() != null && !quote.displayPair().isBlank()) {
                return quote.displayPair();
            }
            if (analysis != null && analysis.displayPair() != null && !analysis.displayPair().isBlank()) {
                return analysis.displayPair();
            }
            return instrument.displayPair();
        }

        String displayUnit() {
            if (quote != null && quote.displayUnit() != null && !quote.displayUnit().isBlank()) {
                return quote.displayUnit();
            }
            if (analysis != null && analysis.displayUnit() != null && !analysis.displayUnit().isBlank()) {
                return analysis.displayUnit();
            }
            return instrument.displayUnit();
        }

        String valueFormat() {
            if (quote != null && quote.valueFormat() != null && !quote.valueFormat().isBlank()) {
                return quote.valueFormat();
            }
            if (analysis != null && analysis.valueFormat() != null && !analysis.valueFormat().isBlank()) {
                return analysis.valueFormat();
            }
            return instrument.valueFormat();
        }

        String formattedPrice() {
            return FormatUtils.valueWithUnit(price(), valueFormat(), displayUnit());
        }

        Double change30d() {
            return analysis == null ? null : analysis.change30d();
        }

        Double volatility() {
            return analysis == null ? null : analysis.volatility20();
        }

        Integer technicalScore() {
            return analysis == null ? null : analysis.technicalScore();
        }

        String technicalLabel() {
            return analysis == null ? "Neutral" : FormatUtils.text(analysis.technicalLabel());
        }

        String technicalSummary() {
            return analysis == null ? "Technical signal pending." : FormatUtils.text(analysis.technicalSummary());
        }

        String trend() {
            return analysis == null ? "-" : FormatUtils.text(analysis.trend());
        }

        String signal() {
            return analysis == null ? "-" : FormatUtils.text(analysis.signal());
        }

        String lastUpdate() {
            if (quote != null && quote.fetchedAt() != null) {
                return quote.fetchedAt();
            }
            return analysis == null ? null : analysis.generatedAt();
        }
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
