package com.example.financedashboard.ui.components;

import com.example.financedashboard.service.MarketDataService.HistoryRange;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.HBox;

import java.util.EnumMap;
import java.util.Map;
import java.util.function.Consumer;

public class PeriodSelector extends HBox {
    private final Map<HistoryRange, Button> buttons = new EnumMap<>(HistoryRange.class);
    private HistoryRange value;
    private Consumer<HistoryRange> onChange = range -> {};

    public PeriodSelector(HistoryRange initialValue) {
        getStyleClass().add("period-selector");
        setAlignment(Pos.CENTER_LEFT);
        setSpacing(6);
        HistoryRange start = initialValue == null ? HistoryRange.NINETY_D : initialValue;
        for (HistoryRange range : HistoryRange.values()) {
            Button button = new Button(range.label());
            button.getStyleClass().add("period-chip");
            button.setOnAction(event -> setValue(range));
            if (!range.enabled()) {
                button.setDisable(true);
                Tooltip.install(button, new Tooltip(range.disabledTooltip()));
            }
            buttons.put(range, button);
            getChildren().add(button);
        }
        setValue(start, false);
    }

    public HistoryRange getValue() {
        return value == null ? HistoryRange.NINETY_D : value;
    }

    public void setValue(HistoryRange value) {
        setValue(value, true);
    }

    public void setOnChange(Consumer<HistoryRange> onChange) {
        this.onChange = onChange == null ? range -> {} : onChange;
    }

    public void setLoading(boolean loading) {
        buttons.forEach((range, button) -> button.setDisable(loading || !range.enabled()));
    }

    private void setValue(HistoryRange newValue, boolean notify) {
        HistoryRange safeValue = newValue == null ? HistoryRange.NINETY_D : newValue;
        if (!safeValue.enabled()) {
            return;
        }
        if (safeValue == value && notify) {
            return;
        }
        value = safeValue;
        buttons.forEach((range, button) -> {
            button.getStyleClass().remove("period-chip-active");
            if (range == value) {
                button.getStyleClass().add("period-chip-active");
            }
        });
        if (notify) {
            onChange.accept(value);
        }
    }
}
