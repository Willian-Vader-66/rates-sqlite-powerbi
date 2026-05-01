package com.example.financedashboard.ui.components;

import javafx.geometry.Pos;
import javafx.scene.control.Label;
import javafx.scene.layout.StackPane;

public class LoadingOverlay extends StackPane {
    public LoadingOverlay() {
        Label label = new Label("Loading...");
        label.getStyleClass().add("loading-label");
        getStyleClass().add("loading-overlay");
        setAlignment(Pos.CENTER);
        getChildren().add(label);
        setVisible(false);
        setManaged(false);
    }

    public void show() {
        setVisible(true);
        setManaged(true);
    }

    public void hide() {
        setVisible(false);
        setManaged(false);
    }
}
