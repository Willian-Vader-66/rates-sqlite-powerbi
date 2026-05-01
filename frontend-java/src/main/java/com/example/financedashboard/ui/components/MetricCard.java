package com.example.financedashboard.ui.components;

import javafx.geometry.Pos;
import javafx.scene.control.Label;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;

public class MetricCard extends VBox {
    private final Label valueLabel = new Label("-");

    public MetricCard(String title) {
        Label titleLabel = new Label(title);
        titleLabel.getStyleClass().add("metric-title");
        valueLabel.getStyleClass().add("metric-value");
        getStyleClass().add("metric-card");
        setAlignment(Pos.CENTER_LEFT);
        setSpacing(6);
        getChildren().addAll(titleLabel, valueLabel);
        HBox.setHgrow(this, Priority.ALWAYS);
    }

    public void setValue(String value) {
        valueLabel.setText(value == null || value.isBlank() ? "-" : value);
    }

}
