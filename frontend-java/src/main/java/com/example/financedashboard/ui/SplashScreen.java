package com.example.financedashboard.ui;

import javafx.animation.FadeTransition;
import javafx.animation.ParallelTransition;
import javafx.animation.PauseTransition;
import javafx.animation.RotateTransition;
import javafx.animation.ScaleTransition;
import javafx.geometry.Pos;
import javafx.scene.Parent;
import javafx.scene.control.Label;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;
import javafx.scene.shape.Polygon;
import javafx.scene.shape.Rectangle;
import javafx.util.Duration;

public class SplashScreen {
    private final StackPane root = new StackPane();
    private final StackPane logo = new StackPane();

    public SplashScreen() {
        root.getStyleClass().add("splash-root");
        root.setOpacity(0);

        Circle outerRing = new Circle(72);
        outerRing.getStyleClass().add("splash-ring");
        outerRing.setFill(Color.TRANSPARENT);

        Polygon hexagon = new Polygon(
                0.0, -62.0,
                54.0, -31.0,
                54.0, 31.0,
                0.0, 62.0,
                -54.0, 31.0,
                -54.0, -31.0
        );
        hexagon.getStyleClass().add("splash-hex");

        Rectangle scanLine = new Rectangle(126, 2);
        scanLine.getStyleClass().add("splash-scan");

        Label monogram = new Label("FM");
        monogram.getStyleClass().add("splash-logo");

        logo.getChildren().addAll(outerRing, hexagon, scanLine, monogram);
        logo.setMinSize(180, 180);
        logo.setMaxSize(180, 180);

        Label title = new Label("Finance Monitor");
        title.getStyleClass().add("splash-title");
        Label subtitle = new Label("Local Market Intelligence Console");
        subtitle.getStyleClass().add("splash-subtitle");

        VBox content = new VBox(18, logo, title, subtitle);
        content.setAlignment(Pos.CENTER);
        root.getChildren().add(content);
    }

    public Parent getView() {
        return root;
    }

    public void play(Runnable onFinished) {
        FadeTransition fadeIn = new FadeTransition(Duration.millis(450), root);
        fadeIn.setFromValue(0);
        fadeIn.setToValue(1);

        RotateTransition rotate = new RotateTransition(Duration.millis(2200), logo);
        rotate.setFromAngle(-8);
        rotate.setToAngle(352);

        ScaleTransition pulse = new ScaleTransition(Duration.millis(900), logo);
        pulse.setFromX(0.96);
        pulse.setFromY(0.96);
        pulse.setToX(1.03);
        pulse.setToY(1.03);
        pulse.setCycleCount(2);
        pulse.setAutoReverse(true);

        PauseTransition hold = new PauseTransition(Duration.millis(1700));
        FadeTransition fadeOut = new FadeTransition(Duration.millis(420), root);
        fadeOut.setFromValue(1);
        fadeOut.setToValue(0);
        fadeOut.setOnFinished(event -> onFinished.run());

        fadeIn.setOnFinished(event -> new ParallelTransition(rotate, pulse, hold).play());
        hold.setOnFinished(event -> fadeOut.play());
        fadeIn.play();
    }
}
