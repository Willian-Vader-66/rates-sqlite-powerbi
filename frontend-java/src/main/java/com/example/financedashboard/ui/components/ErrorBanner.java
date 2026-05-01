package com.example.financedashboard.ui.components;

import javafx.scene.control.Label;
import javafx.scene.layout.HBox;

public class ErrorBanner extends HBox {
    private final Label messageLabel = new Label();

    public ErrorBanner() {
        getStyleClass().add("error-banner");
        getChildren().add(messageLabel);
        setVisible(false);
        setManaged(false);
    }

    public void showMessage(String message) {
        messageLabel.setText(message);
        setVisible(true);
        setManaged(true);
    }

    public void hide() {
        setVisible(false);
        setManaged(false);
    }
}
