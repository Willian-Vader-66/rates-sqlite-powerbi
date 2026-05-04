package com.example.financedashboard.ui.chart;

import com.example.financedashboard.model.PricePoint;
import com.example.financedashboard.util.FormatUtils;
import javafx.animation.FadeTransition;
import javafx.scene.control.Label;
import javafx.scene.layout.Pane;
import javafx.scene.layout.StackPane;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;
import javafx.scene.shape.Line;
import javafx.scene.shape.Polyline;
import javafx.scene.shape.Rectangle;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.scene.text.Text;
import javafx.util.Duration;

import java.util.ArrayList;
import java.util.List;

public class InteractiveFinanceChart extends StackPane {
    private static final int MAX_RENDERED_POINTS = 420;
    private static final double LEFT = 58;
    private static final double RIGHT = 28;
    private static final double TOP = 28;
    private static final double BOTTOM = 38;

    private final Pane plot = new Pane();
    private final Label stateLabel = new Label();
    private final Label hoverLabel = new Label();
    private List<PlotPoint> points = List.of();
    private String symbol = "";
    private String periodLabel = "";
    private String valueFormat = "";
    private String displayUnit = "";
    private String tooltipLabel = "";
    private String emptyMessage = "No historical data available.\nRun: python -m fx_rates dashboard prepare-demo --years 4 --demo";
    private Double hoverX;

    public InteractiveFinanceChart() {
        getStyleClass().add("interactive-chart");
        setMinHeight(280);
        setPrefHeight(360);
        stateLabel.getStyleClass().add("chart-state-label");
        hoverLabel.getStyleClass().add("chart-hover-label");
        hoverLabel.setVisible(false);
        hoverLabel.setMouseTransparent(true);
        plot.prefWidthProperty().bind(widthProperty());
        plot.prefHeightProperty().bind(heightProperty());
        getChildren().addAll(plot, stateLabel, hoverLabel);
        widthProperty().addListener((obs, oldValue, newValue) -> draw());
        heightProperty().addListener((obs, oldValue, newValue) -> draw());
        setOnMouseMoved(event -> {
            hoverX = event.getX();
            updateHover(event.getX(), event.getY());
            draw();
        });
        setOnMouseExited(event -> {
            hoverX = null;
            hoverLabel.setVisible(false);
            draw();
        });
        clear();
    }

    public void showLoading(String message) {
        points = List.of();
        stateLabel.setText(message == null || message.isBlank() ? "Loading chart..." : message);
        stateLabel.setVisible(true);
        hoverLabel.setVisible(false);
        draw();
    }

    public void showError(String message) {
        points = List.of();
        stateLabel.setText(message == null || message.isBlank() ? emptyMessage : message);
        stateLabel.setVisible(true);
        hoverLabel.setVisible(false);
        draw();
    }

    public void setSeries(String symbol, String periodLabel, List<PricePoint> rawPoints) {
        setSeries(symbol, periodLabel, rawPoints, null, null, null);
    }

    public void setSeries(
            String symbol,
            String periodLabel,
            List<PricePoint> rawPoints,
            String valueFormat,
            String displayUnit,
            String tooltipLabel
    ) {
        this.symbol = symbol == null ? "" : symbol;
        this.periodLabel = periodLabel == null ? "" : periodLabel;
        this.valueFormat = valueFormat == null ? "" : valueFormat;
        this.displayUnit = displayUnit == null ? "" : displayUnit;
        this.tooltipLabel = tooltipLabel == null || tooltipLabel.isBlank() ? this.symbol : tooltipLabel;
        this.points = downsample(mapPoints(rawPoints));
        stateLabel.setVisible(points.isEmpty());
        stateLabel.setText(points.isEmpty() ? emptyMessage : "");
        hoverLabel.setVisible(false);
        hoverX = null;
        draw();
        fadePlot();
    }

    public void setChartPoints(
            String symbol,
            String periodLabel,
            List<com.example.financedashboard.model.ChartPoint> rawPoints,
            String valueFormat,
            String displayUnit,
            String tooltipLabel
    ) {
        this.symbol = symbol == null ? "" : symbol;
        this.periodLabel = periodLabel == null ? "" : periodLabel;
        this.valueFormat = valueFormat == null ? "" : valueFormat;
        this.displayUnit = displayUnit == null ? "" : displayUnit;
        this.tooltipLabel = tooltipLabel == null || tooltipLabel.isBlank() ? this.symbol : tooltipLabel;
        this.points = downsample(mapChartPoints(rawPoints));
        stateLabel.setVisible(points.isEmpty());
        stateLabel.setText(points.isEmpty() ? emptyMessage : "");
        hoverLabel.setVisible(false);
        hoverX = null;
        draw();
        fadePlot();
    }

    public void clear() {
        points = List.of();
        stateLabel.setText(emptyMessage);
        stateLabel.setVisible(true);
        hoverLabel.setVisible(false);
        draw();
    }

    public int renderedPointCount() {
        return points.size();
    }

    private void draw() {
        double width = Math.max(1, getWidth());
        double height = Math.max(1, getHeight());
        plot.getChildren().clear();
        Rectangle background = new Rectangle(width, height);
        background.setArcWidth(8);
        background.setArcHeight(8);
        background.setFill(Color.web("#0b1220"));
        plot.getChildren().add(background);

        if (points.isEmpty()) {
            return;
        }

        ChartScaleUtils.Bounds bounds = bounds();
        drawGrid(width, height, bounds);
        drawLine(width, height, bounds);
        drawLastMarker(width, height, bounds);
        if (hoverX != null) {
            drawCrosshair(width, height, bounds, nearestIndex(hoverX));
        }
    }

    private void drawGrid(double width, double height, ChartScaleUtils.Bounds bounds) {
        double plotHeight = height - TOP - BOTTOM;
        for (int i = 0; i <= 4; i++) {
            double y = TOP + (plotHeight * i / 4.0);
            Line line = new Line(LEFT, y, width - RIGHT, y);
            line.setStroke(Color.web("#263247"));
            plot.getChildren().add(line);
            double value = bounds.max() - ((bounds.max() - bounds.min()) * i / 4.0);
            Text label = axisText(FormatUtils.compact(value), 8, y + 4);
            plot.getChildren().add(label);
        }
        int labels = Math.min(4, points.size());
        for (int i = 0; i < labels; i++) {
            int index = labels == 1 ? 0 : (int) Math.round((points.size() - 1) * (i / (double) (labels - 1)));
            double x = xFor(index, width);
            Text label = axisText(shortDate(points.get(index).date()), Math.max(LEFT, Math.min(width - RIGHT - 55, x - 22)), height - 12);
            plot.getChildren().add(label);
        }
    }

    private void drawLine(double width, double height, ChartScaleUtils.Bounds bounds) {
        Polyline line = new Polyline();
        for (int i = 0; i < points.size(); i++) {
            line.getPoints().addAll(xFor(i, width), yFor(points.get(i).value(), height, bounds));
        }
        line.setFill(null);
        line.setStroke(Color.web("#38bdf8"));
        line.setStrokeWidth(2.2);
        plot.getChildren().add(line);
    }

    private void drawLastMarker(double width, double height, ChartScaleUtils.Bounds bounds) {
        int index = points.size() - 1;
        PlotPoint latest = points.get(index);
        double x = xFor(index, width);
        double y = yFor(latest.value(), height, bounds);
        Circle marker = new Circle(x, y, 4.5, Color.web("#22c55e"));
        Text label = new Text(
                Math.min(width - RIGHT - 150, x + 8),
                Math.max(TOP + 12, y - 8),
                symbol + " " + FormatUtils.valueWithUnit(latest.value(), valueFormat, displayUnit)
        );
        label.setFill(Color.web("#e5edf7"));
        label.setFont(Font.font("Segoe UI", FontWeight.BOLD, 12));
        plot.getChildren().addAll(marker, label);
    }

    private void drawCrosshair(double width, double height, ChartScaleUtils.Bounds bounds, int index) {
        if (index < 0 || index >= points.size()) {
            return;
        }
        double x = xFor(index, width);
        double y = yFor(points.get(index).value(), height, bounds);
        Line crosshair = new Line(x, TOP, x, height - BOTTOM);
        crosshair.setStroke(Color.web("#94a3b8"));
        Circle marker = new Circle(x, y, 3.8, Color.web("#f8fafc"));
        plot.getChildren().addAll(crosshair, marker);
    }

    private void updateHover(double mouseX, double mouseY) {
        if (points.isEmpty()) {
            hoverLabel.setVisible(false);
            return;
        }
        int index = nearestIndex(mouseX);
        PlotPoint point = points.get(index);
        Double previous = index > 0 ? points.get(index - 1).value() : null;
        String change = previous == null || previous == 0.0
                ? "-"
                : FormatUtils.percent((point.value() / previous) - 1.0);
        hoverLabel.setText("""
                %s %s
                %s
                Change: %s
                %s
                """.formatted(
                tooltipLabel,
                periodLabel,
                point.date(),
                change,
                FormatUtils.valueWithUnit(point.value(), valueFormat, displayUnit)
        ).trim());
        hoverLabel.autosize();
        double x = Math.min(getWidth() - hoverLabel.getWidth() - 14, Math.max(10, mouseX + 14));
        double y = Math.min(getHeight() - hoverLabel.getHeight() - 14, Math.max(10, mouseY - 54));
        hoverLabel.relocate(x, y);
        hoverLabel.setVisible(true);
    }

    private int nearestIndex(double x) {
        double plotWidth = Math.max(1, getWidth() - LEFT - RIGHT);
        double ratio = Math.max(0, Math.min(1, (x - LEFT) / plotWidth));
        return (int) Math.round(ratio * Math.max(0, points.size() - 1));
    }

    private double xFor(int index, double width) {
        return LEFT + (width - LEFT - RIGHT) * index / Math.max(1, points.size() - 1);
    }

    private double yFor(double value, double height, ChartScaleUtils.Bounds bounds) {
        double span = Math.max(0.000001, bounds.max() - bounds.min());
        double ratio = (value - bounds.min()) / span;
        return height - BOTTOM - ((height - TOP - BOTTOM) * ratio);
    }

    private ChartScaleUtils.Bounds bounds() {
        return ChartScaleUtils.paddedBounds(points.stream().map(PlotPoint::value).toList());
    }

    private static Text axisText(String value, double x, double y) {
        Text text = new Text(x, y, value);
        text.setFill(Color.web("#8ea0b8"));
        text.setFont(Font.font("Segoe UI", 10));
        return text;
    }

    private static List<PlotPoint> mapPoints(List<PricePoint> rawPoints) {
        if (rawPoints == null || rawPoints.isEmpty()) {
            return List.of();
        }
        List<PlotPoint> mapped = new ArrayList<>();
        for (PricePoint point : rawPoints) {
            Double value = point.displayValue();
            if (value != null && Double.isFinite(value)) {
                mapped.add(new PlotPoint(point.date(), value));
            }
        }
        return mapped;
    }

    private static List<PlotPoint> mapChartPoints(List<com.example.financedashboard.model.ChartPoint> rawPoints) {
        if (rawPoints == null || rawPoints.isEmpty()) {
            return List.of();
        }
        List<PlotPoint> mapped = new ArrayList<>();
        for (com.example.financedashboard.model.ChartPoint point : rawPoints) {
            if (point.value() != null && Double.isFinite(point.value())) {
                mapped.add(new PlotPoint(point.date(), point.value()));
            }
        }
        return mapped;
    }

    private void fadePlot() {
        plot.setOpacity(0.38);
        FadeTransition transition = new FadeTransition(Duration.millis(220), plot);
        transition.setFromValue(0.38);
        transition.setToValue(1.0);
        transition.play();
    }

    private static List<PlotPoint> downsample(List<PlotPoint> source) {
        if (source.size() <= MAX_RENDERED_POINTS) {
            return source;
        }
        List<PlotPoint> result = new ArrayList<>(MAX_RENDERED_POINTS);
        double step = (source.size() - 1) / (double) (MAX_RENDERED_POINTS - 1);
        for (int i = 0; i < MAX_RENDERED_POINTS; i++) {
            result.add(source.get((int) Math.round(i * step)));
        }
        return result;
    }

    private static String shortDate(String raw) {
        if (raw == null || raw.length() < 10) {
            return FormatUtils.text(raw);
        }
        return raw.substring(5, 10);
    }

    private record PlotPoint(String date, double value) {
    }

}
