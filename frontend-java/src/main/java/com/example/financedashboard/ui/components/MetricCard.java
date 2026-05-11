package com.example.financedashboard.ui.components;

import javafx.geometry.Pos;
import javafx.scene.control.Label;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;

public class MetricCard extends VBox {
    private final Label valueLabel = new Label("-");
    private final Label captionLabel = new Label("");
    private final Label badgeLabel = new Label("");

    public MetricCard(String title) {
        Label titleLabel = new Label(title);
        titleLabel.getStyleClass().add("metric-title");
        badgeLabel.getStyleClass().add("metric-badge");
        badgeLabel.setVisible(false);
        badgeLabel.setManaged(false);
        HBox header = new HBox(8, titleLabel, badgeLabel);
        header.setAlignment(Pos.CENTER_LEFT);
        valueLabel.getStyleClass().add("metric-value");
        captionLabel.getStyleClass().add("metric-caption");
        getStyleClass().add("metric-card");
        setAlignment(Pos.CENTER_LEFT);
        setSpacing(6);
        getChildren().addAll(header, valueLabel, captionLabel);
        HBox.setHgrow(this, Priority.ALWAYS);
    }

    public void setValue(String value) {
        valueLabel.setText(value == null || value.isBlank() ? "-" : value);
    }

    public void setCaption(String value) {
        captionLabel.setText(value == null || value.isBlank() ? "" : value);
        captionLabel.setVisible(value != null && !value.isBlank());
        captionLabel.setManaged(value != null && !value.isBlank());
    }

    public void setBadge(String value, String mode) {
        badgeLabel.getStyleClass().removeAll("data-mode-demo", "data-mode-live", "data-mode-mixed", "data-mode-unknown");
        boolean visible = value != null && !value.isBlank();
        badgeLabel.setText(visible ? value : "");
        badgeLabel.setVisible(visible);
        badgeLabel.setManaged(visible);
        if (visible) {
            badgeLabel.getStyleClass().add("data-mode-" + (mode == null || mode.isBlank() ? "unknown" : mode.toLowerCase()));
        }
    }
}
