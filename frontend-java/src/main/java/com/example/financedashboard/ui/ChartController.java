package com.example.financedashboard.ui;

import com.example.financedashboard.model.PricePoint;
import com.example.financedashboard.util.FormatUtils;
import javafx.scene.Node;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.control.Label;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;

import java.util.List;

public class ChartController {
    private final CategoryAxis xAxis = new CategoryAxis();
    private final NumberAxis yAxis = new NumberAxis();
    private final LineChart<String, Number> chart = new LineChart<>(xAxis, yAxis);
    private final VBox view = new VBox(8);

    public ChartController() {
        Label title = new Label("Historical Price / Rate");
        title.getStyleClass().add("panel-title");
        chart.setAnimated(false);
        chart.setLegendVisible(false);
        chart.setCreateSymbols(true);
        chart.setMinHeight(280);
        xAxis.setLabel("Date");
        yAxis.setLabel("Value");
        VBox.setVgrow(chart, Priority.ALWAYS);
        view.getStyleClass().add("panel");
        view.getChildren().addAll(title, chart);
    }

    public VBox getView() {
        return view;
    }

    public void showHistory(String symbol, List<PricePoint> points) {
        chart.getData().clear();
        XYChart.Series<String, Number> series = new XYChart.Series<>();
        series.setName(symbol);
        for (PricePoint point : points) {
            Double value = point.displayValue();
            if (value != null) {
                XYChart.Data<String, Number> data = new XYChart.Data<>(point.date(), value);
                series.getData().add(data);
            }
        }
        chart.getData().add(series);
        for (XYChart.Data<String, Number> data : series.getData()) {
            Node node = data.getNode();
            if (node != null) {
                Tooltip.install(node, new Tooltip(data.getXValue() + ": " + FormatUtils.price(data.getYValue())));
            }
        }
    }

    public void clear() {
        chart.getData().clear();
    }
}
