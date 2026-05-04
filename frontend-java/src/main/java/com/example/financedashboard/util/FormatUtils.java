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
    private static final DecimalFormat FX_RATE = new DecimalFormat("#,##0.0000##", DASHBOARD_SYMBOLS);
    private static final DecimalFormat PERCENT = new DecimalFormat("+#,##0.00%;-#,##0.00%", DASHBOARD_SYMBOLS);
    private static final DecimalFormat COMPACT = new DecimalFormat("#,##0.##", DASHBOARD_SYMBOLS);

    private FormatUtils() {
    }

    public static String integer(Number value) {
        return value == null ? "-" : INTEGER.format(value.longValue());
    }

    public static String price(Number value) {
        return value == null ? "-" : PRICE.format(value.doubleValue());
    }

    public static String usd(Number value) {
        return value == null ? "-" : PRICE.format(value.doubleValue()) + " USD";
    }

    public static String fx(Number value) {
        return value == null ? "-" : FX_RATE.format(value.doubleValue());
    }

    public static String valueWithUnit(Number value, String valueFormat, String unit) {
        if (value == null) {
            return "-";
        }
        String normalizedFormat = valueFormat == null ? "" : valueFormat.trim().toLowerCase(Locale.ROOT);
        String normalizedUnit = unit == null ? "" : unit.trim();
        String formatted = switch (normalizedFormat) {
            case "fx_rate" -> fx(value);
            case "percent" -> PRICE.format(value.doubleValue());
            case "currency_usd" -> PRICE.format(value.doubleValue());
            default -> price(value);
        };
        return normalizedUnit.isBlank() ? formatted : formatted + " " + normalizedUnit;
    }

    public static String percent(Number value) {
        if (value == null) {
            return "-";
        }
        double raw = value.doubleValue();
        double ratio = Math.abs(raw) > 1.0 ? raw / 100.0 : raw;
        return PERCENT.format(ratio);
    }

    public static String compact(Number value) {
        if (value == null) {
            return "-";
        }
        double raw = value.doubleValue();
        double abs = Math.abs(raw);
        if (abs >= 1_000_000_000) {
            return COMPACT.format(raw / 1_000_000_000.0) + "B";
        }
        if (abs >= 1_000_000) {
            return COMPACT.format(raw / 1_000_000.0) + "M";
        }
        if (abs >= 10_000) {
            return COMPACT.format(raw / 1_000.0) + "K";
        }
        return PRICE.format(raw);
    }

    public static String bytes(Number value) {
        if (value == null) {
            return "-";
        }
        double raw = value.doubleValue();
        if (raw >= 1024 * 1024) {
            return PRICE.format(raw / (1024.0 * 1024.0)) + " MB";
        }
        if (raw >= 1024) {
            return PRICE.format(raw / 1024.0) + " KB";
        }
        return INTEGER.format(raw) + " B";
    }

    public static String text(String value) {
        return value == null || value.isBlank() ? "-" : value;
    }
}
