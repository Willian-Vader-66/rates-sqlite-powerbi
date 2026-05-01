package com.example.financedashboard.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public final class DateUtils {
    private static final DateTimeFormatter DISPLAY_DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private DateUtils() {
    }

    public static LocalDate todayMinusDays(int days) {
        return LocalDate.now().minusDays(days);
    }

    public static String displayDateTime(String isoDateTime) {
        if (isoDateTime == null || isoDateTime.isBlank()) {
            return "-";
        }
        try {
            return Instant.parse(isoDateTime).atZone(ZoneId.systemDefault()).format(DISPLAY_DATE_TIME);
        } catch (DateTimeParseException ex) {
            try {
                return LocalDateTime.parse(isoDateTime).format(DISPLAY_DATE_TIME);
            } catch (DateTimeParseException ignored) {
                return isoDateTime;
            }
        }
    }
}
