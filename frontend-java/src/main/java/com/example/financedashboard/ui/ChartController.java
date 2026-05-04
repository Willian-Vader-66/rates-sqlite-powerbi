package com.example.financedashboard.ui;

import com.example.financedashboard.model.PricePoint;
import com.example.financedashboard.ui.chart.InteractiveFinanceChart;
import com.example.financedashboard.util.FormatUtils;
import javafx.scene.control.Label;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;

import java.util.List;

public class ChartController {
    private final InteractiveFinanceChart chart = new InteractiveFinanceChart();
    private final VBox view = new VBox(8);
    private final Label title = new Label("Historical Price / Rate");
    private String periodLabel = "";

    public ChartController() {
        title.getStyleClass().add("panel-title");
        chart.setMinHeight(280);
        VBox.setVgrow(chart, Priority.ALWAYS);
        view.getStyleClass().add("chart-panel");
        view.getChildren().addAll(title, chart);
    }

    public VBox getView() {
        return view;
    }

    public void setPeriodLabel(String periodLabel) {
        this.periodLabel = periodLabel == null ? "" : periodLabel;
        title.setText("Historical Price / Rate - " + this.periodLabel);
    }

    public void showHistory(String symbol, List<PricePoint> points) {
        PricePoint first = points == null || points.isEmpty() ? null : points.get(0);
        String chartTitle = first == null ? symbol : FormatUtils.text(first.chartTitle());
        String unit = first == null ? "" : first.displayUnit();
        String subtitle = first == null ? "" : FormatUtils.text(first.chartSubtitle());
        title.setText("%s - %s%s".formatted(
                "-".equals(chartTitle) ? symbol : chartTitle,
                periodLabel,
                unit == null || unit.isBlank() ? "" : " | " + ("-".equals(subtitle) ? unit : subtitle)
        ));
        chart.setSeries(
                first == null ? symbol : FormatUtils.text(first.displayPair()).replace("-", symbol),
                periodLabel,
                points,
                first == null ? null : first.valueFormat(),
                unit,
                first == null ? symbol : first.tooltipLabel()
        );
    }

    public void showLoading(String symbol) {
        chart.showLoading("Loading " + symbol + " history...");
    }

    public int renderedPointCount() {
        return chart.renderedPointCount();
    }

    public void clear() {
        chart.clear();
    }
}
