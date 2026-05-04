package com.example.financedashboard.util;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Locale;

public final class FormatUtils {
    // Dashboard labels are English today, so numeric display is intentionally Locale.US.
    private static final Locale DASHBOARD_LOCALE = Locale.US;
    private static final DecimalFormatSymbols DASHBOARD_SYMBOLS = DecimalFormatSymbols.getInstance(DASHBOARD_LOCALE);
    private static final NumberFormat INTEGER = NumberFormat.getIntegerInstance(DASHBOARD_LOCALE);
    private static final DecimalFormat PRICE = new DecimalFormat("#,##0.00", DASHBOARD_SYMBOLS);
    private static final DecimalFormat PERCENT = new DecimalFormat("+#,##0.00%;-#,##0.00%", DASHBOARD_SYMBOLS);

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
