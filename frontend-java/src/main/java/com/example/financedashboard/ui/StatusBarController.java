package com.example.financedashboard.ui;

import com.example.financedashboard.util.DateUtils;
import javafx.scene.control.Label;
import javafx.scene.layout.HBox;

import java.time.Instant;

public class StatusBarController {
    private final Label apiLabel = new Label();
    private final Label refreshLabel = new Label("Last refresh: -");
    private final HBox view = new HBox(16, apiLabel, refreshLabel);

    public StatusBarController() {
        view.getStyleClass().add("status-text");
        setDisconnected("Not checked");
    }

    public HBox getView() {
        return view;
    }

    public void setConnected(String apiBaseUrl) {
        apiLabel.setText("API: Connected (" + apiBaseUrl + ")");
        apiLabel.getStyleClass().removeAll("disconnected");
        if (!apiLabel.getStyleClass().contains("connected")) {
            apiLabel.getStyleClass().add("connected");
        }
    }

    public void setRefreshing(String apiBaseUrl) {
        apiLabel.setText("API: Refreshing... (" + apiBaseUrl + ")");
        apiLabel.getStyleClass().removeAll("disconnected");
        if (!apiLabel.getStyleClass().contains("connected")) {
            apiLabel.getStyleClass().add("connected");
        }
    }

    public void setPaused(String apiBaseUrl) {
        apiLabel.setText("API: Auto-refresh paused (" + apiBaseUrl + ")");
        apiLabel.getStyleClass().removeAll("disconnected");
        if (!apiLabel.getStyleClass().contains("connected")) {
            apiLabel.getStyleClass().add("connected");
        }
    }

    public void setDisconnected(String reason) {
        apiLabel.setText("API: Backend unavailable" + (reason == null || reason.isBlank() ? "" : " - " + reason));
        apiLabel.getStyleClass().removeAll("connected");
        if (!apiLabel.getStyleClass().contains("disconnected")) {
            apiLabel.getStyleClass().add("disconnected");
        }
    }

    public void setLastRefreshNow() {
        refreshLabel.setText("Last refresh: " + DateUtils.displayDateTime(Instant.now().toString()));
    }

    public void setLastRefreshFailed() {
        refreshLabel.setText("Last update failed: " + DateUtils.displayDateTime(Instant.now().toString()));
    }
}
