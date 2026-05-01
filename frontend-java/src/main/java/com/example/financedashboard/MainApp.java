package com.example.financedashboard;

import com.example.financedashboard.api.ApiClient;
import com.example.financedashboard.config.AppConfig;
import com.example.financedashboard.service.MarketDataService;
import com.example.financedashboard.ui.DashboardController;
import com.example.financedashboard.ui.SplashScreen;
import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.layout.StackPane;
import javafx.stage.Stage;

import java.util.Objects;

public class MainApp extends Application {
    private DashboardController dashboardController;

    @Override
    public void start(Stage stage) {
        AppConfig config = AppConfig.load();
        ApiClient apiClient = new ApiClient(config);
        MarketDataService service = new MarketDataService(apiClient);
        dashboardController = new DashboardController(config, service);

        SplashScreen splashScreen = new SplashScreen();
        StackPane shell = new StackPane(splashScreen.getView());
        Scene scene = new Scene(shell, 1280, 860);
        scene.getStylesheets().add(Objects.requireNonNull(
                MainApp.class.getResource("/styles/app.css"),
                "styles/app.css not found"
        ).toExternalForm());

        stage.setTitle("Finance Monitor");
        stage.setScene(scene);
        stage.setMinWidth(980);
        stage.setMinHeight(720);
        stage.show();
        splashScreen.play(() -> shell.getChildren().setAll(dashboardController.createView()));
    }

    @Override
    public void stop() {
        if (dashboardController != null) {
            dashboardController.stop();
        }
    }

    public static void main(String[] args) {
        launch(args);
    }
}
