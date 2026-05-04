package com.example.financedashboard.ui.chart;

import java.util.List;

public final class ChartScaleUtils {
    private ChartScaleUtils() {
    }

    public static Bounds paddedBounds(List<Double> values) {
        if (values == null || values.isEmpty()) {
            return new Bounds(0, 1);
        }
        double min = values.stream().mapToDouble(Double::doubleValue).min().orElse(0.0);
        double max = values.stream().mapToDouble(Double::doubleValue).max().orElse(1.0);
        double average = values.stream().mapToDouble(Double::doubleValue).average().orElse((min + max) / 2.0);
        double range = max - min;
        double reference = Math.max(0.000001, Math.abs(average));
        double padding = Math.max(range * 0.16, reference * 0.025);
        if (range < reference * 0.004) {
            padding = Math.max(padding, reference * 0.05);
        }
        double lower = min - padding;
        double upper = max + padding;
        if (Double.compare(lower, upper) == 0) {
            lower -= 1;
            upper += 1;
        }
        return new Bounds(lower, upper);
    }

    public record Bounds(double min, double max) {
    }
}
