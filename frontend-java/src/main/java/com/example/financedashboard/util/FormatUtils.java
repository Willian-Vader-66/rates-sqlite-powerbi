package com.example.financedashboard.util;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

public final class FormatUtils {
    private static final NumberFormat INTEGER = NumberFormat.getIntegerInstance(Locale.US);
    private static final DecimalFormat PRICE = new DecimalFormat("#,##0.00##");
    private static final DecimalFormat PERCENT = new DecimalFormat("+#,##0.00%;-#,##0.00%");

    private FormatUtils() {
    }

    public static String integer(Number value) {
        return value == null ? "-" : INTEGER.format(value.longValue());
    }

    public static String price(Number value) {
        return value == null ? "-" : PRICE.format(value.doubleValue());
    }

    public static String percent(Number value) {
        if (value == null) {
            return "-";
        }
        double raw = value.doubleValue();
        double ratio = Math.abs(raw) > 1.0 ? raw / 100.0 : raw;
        return PERCENT.format(ratio);
    }

    public static String text(String value) {
        return value == null || value.isBlank() ? "-" : value;
    }
}
