package com.example.financedashboard.ui;

import com.example.financedashboard.model.AnalysisSnapshot;
import com.example.financedashboard.model.Instrument;
import com.example.financedashboard.model.Quote;
import com.example.financedashboard.util.DateUtils;
import com.example.financedashboard.util.FormatUtils;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

public class InstrumentTableController {
    private final ObservableList<WatchRow> rows = FXCollections.observableArrayList();
    private final FilteredList<WatchRow> filteredRows = new FilteredList<>(rows);
    private final TableView<WatchRow> table = new TableView<>(filteredRows);
    private final TextField searchField = new TextField();
    private final HBox assetChips = new HBox(6);
    private final HBox signalChips = new HBox(6);
    private final HBox trendChips = new HBox(6);
    private final ComboBox<String> exchangeFilter = new ComboBox<>();
    private final ComboBox<String> sectorFilter = new ComboBox<>();
    private final CheckBox activeOnly = new CheckBox("Active only");
    private final Button clearFilters = new Button("Clear Filters");
    private final VBox view = new VBox(10);
    private Consumer<WatchRow> selectionHandler = row -> {};
    private String selectedAsset = "ALL";
    private String selectedSignal = "ALL";
    private String selectedTrend = "ALL";

    public InstrumentTableController() {
        configureFilters();
        configureTable();
        Label title = new Label("Watchlist");
        title.getStyleClass().add("panel-title");
        HBox filterBar = new HBox(10, new Label("Search"), searchField, activeOnly, clearFilters);
        filterBar.getStyleClass().add("filter-bar");
        filterBar.setAlignment(Pos.CENTER_LEFT);
        HBox.setHgrow(searchField, Priority.ALWAYS);
        HBox dropdownBar = new HBox(10,
                new Label("Exchange"), exchangeFilter,
                new Label("Sector"), sectorFilter);
        dropdownBar.getStyleClass().add("filter-bar");
        dropdownBar.setAlignment(Pos.CENTER_LEFT);
        view.getChildren().addAll(title, filterBar, chipRow("Asset", assetChips), chipRow("Signal", signalChips), chipRow("Trend", trendChips), dropdownBar, table);
        VBox.setVgrow(table, Priority.ALWAYS);
    }

    public VBox getView() {
        return view;
    }

    public void setOnSelection(Consumer<WatchRow> selectionHandler) {
        this.selectionHandler = selectionHandler == null ? row -> {} : selectionHandler;
    }

    public WatchRow getSelected() {
        return table.getSelectionModel().getSelectedItem();
    }

    public void updateData(List<Instrument> instruments, List<Quote> quotes, List<AnalysisSnapshot> analysis) {
        Map<String, Quote> quoteByKey = new HashMap<>();
        for (Quote quote : quotes) {
            quoteByKey.put(key(quote.symbol(), quote.assetType(), quote.exchange()), quote);
            quoteByKey.putIfAbsent(quote.symbol(), quote);
        }

        Map<String, AnalysisSnapshot> analysisByKey = new HashMap<>();
        for (AnalysisSnapshot snapshot : analysis) {
            analysisByKey.put(key(snapshot.symbol(), snapshot.assetType(), snapshot.exchange()), snapshot);
            analysisByKey.putIfAbsent(snapshot.symbol(), snapshot);
        }

        String selectedSymbol = getSelected() == null ? null : getSelected().symbol();
        List<WatchRow> updatedRows = instruments.stream()
                .map(instrument -> {
                    Quote quote = firstMatch(quoteByKey, instrument);
                    AnalysisSnapshot snapshot = firstMatch(analysisByKey, instrument);
                    return new WatchRow(instrument, quote, snapshot);
                })
                .sorted(Comparator.comparing(WatchRow::assetType).thenComparing(WatchRow::symbol))
                .toList();
        updateDynamicFilters(updatedRows);
        rows.setAll(updatedRows);
        applyFilter();
        if (selectedSymbol != null) {
            rows.stream()
                    .filter(row -> row.symbol().equals(selectedSymbol))
                    .findFirst()
                    .ifPresent(row -> table.getSelectionModel().select(row));
        }
    }

    private void configureFilters() {
        searchField.setPromptText("Symbol or name");
        buildChipGroup(assetChips, List.of("ALL", "STOCK", "FX"), value -> {
            selectedAsset = value;
            applyFilter();
        });
        buildChipGroup(signalChips, List.of("ALL", "STABLE", "WATCH", "VOLATILE", "BREAKOUT", "DRAWDOWN", "UNKNOWN"), value -> {
            selectedSignal = value;
            applyFilter();
        });
        buildChipGroup(trendChips, List.of("ALL", "UP", "DOWN", "SIDEWAYS", "UNKNOWN"), value -> {
            selectedTrend = value;
            applyFilter();
        });
        exchangeFilter.setItems(FXCollections.observableArrayList("ALL"));
        exchangeFilter.setValue("ALL");
        sectorFilter.setItems(FXCollections.observableArrayList("ALL"));
        sectorFilter.setValue("ALL");
        activeOnly.setSelected(true);
        clearFilters.getStyleClass().add("neon-button-secondary");
        clearFilters.setOnAction(event -> clearFilters());

        searchField.textProperty().addListener((obs, oldValue, newValue) -> applyFilter());
        exchangeFilter.valueProperty().addListener((obs, oldValue, newValue) -> applyFilter());
        sectorFilter.valueProperty().addListener((obs, oldValue, newValue) -> applyFilter());
        activeOnly.selectedProperty().addListener((obs, oldValue, newValue) -> applyFilter());
    }

    private void configureTable() {
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
        table.getColumns().add(column("Symbol", row -> row.symbol()));
        table.getColumns().add(column("Name", row -> row.name()));
        table.getColumns().add(column("Asset", row -> row.assetType()));
        table.getColumns().add(column("Exchange", row -> row.exchange()));
        table.getColumns().add(column("Latest Price", row -> FormatUtils.price(row.price())));
        table.getColumns().add(column("% Change", row -> FormatUtils.percent(row.percentChange())));
        table.getColumns().add(column("Trend", row -> row.trend()));
        table.getColumns().add(column("Signal", row -> row.signal()));
        table.getColumns().add(column("Last Update", row -> DateUtils.displayDateTime(row.lastUpdate())));
        table.getSelectionModel().selectedItemProperty().addListener((obs, oldRow, newRow) -> {
            if (newRow != null) {
                selectionHandler.accept(newRow);
            }
        });
    }

    private TableColumn<WatchRow, String> column(String title, java.util.function.Function<WatchRow, String> extractor) {
        TableColumn<WatchRow, String> column = new TableColumn<>(title);
        column.setCellValueFactory(data -> new SimpleStringProperty(FormatUtils.text(extractor.apply(data.getValue()))));
        return column;
    }

    private void applyFilter() {
        String search = searchField.getText() == null ? "" : searchField.getText().trim().toLowerCase(Locale.ROOT);
        String exchange = exchangeFilter.getValue() == null ? "ALL" : exchangeFilter.getValue();
        String sector = sectorFilter.getValue() == null ? "ALL" : sectorFilter.getValue();
        boolean onlyActive = activeOnly.isSelected();

        filteredRows.setPredicate(row -> {
            if (onlyActive && !row.active()) {
                return false;
            }
            if (!"ALL".equals(selectedAsset) && !selectedAsset.equalsIgnoreCase(row.assetType())) {
                return false;
            }
            if (!"ALL".equals(selectedSignal) && !selectedSignal.equalsIgnoreCase(row.signal())) {
                return false;
            }
            if (!"ALL".equals(selectedTrend) && !selectedTrend.equalsIgnoreCase(row.trend())) {
                return false;
            }
            if (!"ALL".equals(exchange) && !exchange.equalsIgnoreCase(row.exchange())) {
                return false;
            }
            if (!"ALL".equals(sector) && !sector.equalsIgnoreCase(row.sector())) {
                return false;
            }
            return search.isBlank()
                    || Objects.toString(row.symbol(), "").toLowerCase(Locale.ROOT).contains(search)
                    || Objects.toString(row.name(), "").toLowerCase(Locale.ROOT).contains(search);
        });
    }

    private HBox chipRow(String label, HBox chips) {
        HBox row = new HBox(10, new Label(label), chips);
        row.getStyleClass().add("filter-bar");
        row.setAlignment(Pos.CENTER_LEFT);
        return row;
    }

    private void buildChipGroup(HBox container, List<String> values, Consumer<String> onSelected) {
        container.getChildren().clear();
        for (String value : values) {
            Button chip = new Button(value);
            chip.getStyleClass().add("filter-chip");
            if ("ALL".equals(value)) {
                chip.getStyleClass().add("filter-chip-active");
            }
            chip.setOnAction(event -> {
                for (javafx.scene.Node node : container.getChildren()) {
                    node.getStyleClass().remove("filter-chip-active");
                }
                chip.getStyleClass().add("filter-chip-active");
                onSelected.accept(value);
            });
            container.getChildren().add(chip);
        }
    }

    private void updateDynamicFilters(List<WatchRow> updatedRows) {
        String selectedExchange = exchangeFilter.getValue();
        String selectedSector = sectorFilter.getValue();
        Set<String> exchanges = new LinkedHashSet<>();
        Set<String> sectors = new LinkedHashSet<>();
        exchanges.add("ALL");
        sectors.add("ALL");
        for (WatchRow row : updatedRows) {
            if (row.exchange() != null && !row.exchange().isBlank()) {
                exchanges.add(row.exchange());
            }
            if (row.sector() != null && !row.sector().isBlank()) {
                sectors.add(row.sector());
            }
        }
        exchangeFilter.setItems(FXCollections.observableArrayList(exchanges));
        sectorFilter.setItems(FXCollections.observableArrayList(sectors));
        exchangeFilter.setValue(exchanges.contains(selectedExchange) ? selectedExchange : "ALL");
        sectorFilter.setValue(sectors.contains(selectedSector) ? selectedSector : "ALL");
    }

    private void clearFilters() {
        searchField.clear();
        activeOnly.setSelected(true);
        exchangeFilter.setValue("ALL");
        sectorFilter.setValue("ALL");
        selectedAsset = "ALL";
        selectedSignal = "ALL";
        selectedTrend = "ALL";
        resetChipGroup(assetChips);
        resetChipGroup(signalChips);
        resetChipGroup(trendChips);
        applyFilter();
    }

    private void resetChipGroup(HBox container) {
        for (javafx.scene.Node node : container.getChildren()) {
            node.getStyleClass().remove("filter-chip-active");
            if (node instanceof Button button && "ALL".equals(button.getText())) {
                button.getStyleClass().add("filter-chip-active");
            }
        }
    }

    private static <T> T firstMatch(Map<String, T> map, Instrument instrument) {
        T exact = map.get(key(instrument.symbol(), instrument.assetType(), instrument.exchange()));
        if (exact != null) {
            return exact;
        }
        return map.get(instrument.symbol());
    }

    private static String key(String symbol, String assetType, String exchange) {
        return String.join("|",
                Objects.toString(symbol, ""),
                Objects.toString(assetType, ""),
                Objects.toString(exchange, ""));
    }

    public record WatchRow(Instrument instrument, Quote quote, AnalysisSnapshot analysis) {
        public String symbol() {
            return instrument.symbol();
        }

        public String name() {
            return instrument.name();
        }

        public String assetType() {
            return instrument.assetType();
        }

        public String exchange() {
            return instrument.exchange();
        }

        public String sector() {
            return instrument.sector();
        }

        public boolean active() {
            return instrument.active();
        }

        public Double price() {
            if (quote != null && quote.price() != null) {
                return quote.price();
            }
            return analysis == null ? null : analysis.lastPrice();
        }

        public Double percentChange() {
            return quote == null ? null : quote.percentChange();
        }

        public String trend() {
            return analysis == null ? "UNKNOWN" : FormatUtils.text(analysis.trend());
        }

        public String signal() {
            return analysis == null ? "UNKNOWN" : FormatUtils.text(analysis.signal());
        }

        public String lastUpdate() {
            if (quote != null && quote.fetchedAt() != null) {
                return quote.fetchedAt();
            }
            return analysis == null ? null : analysis.generatedAt();
        }
    }
}
