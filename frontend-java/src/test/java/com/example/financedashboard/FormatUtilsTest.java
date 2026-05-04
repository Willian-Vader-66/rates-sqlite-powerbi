package com.example.financedashboard;

import com.example.financedashboard.util.DateUtils;
import com.example.financedashboard.util.FormatUtils;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FormatUtilsTest {
    @Test
    void formatsIntegersForDashboardDisplay() {
        assertEquals("1,234", FormatUtils.integer(1234));
        assertEquals("-1,234", FormatUtils.integer(-1234));
        assertEquals("0", FormatUtils.integer(0));
        assertEquals("-", FormatUtils.integer(null));
    }

    @Test
    void formatsPricesWithTwoDecimalsForDashboardDisplay() {
        assertEquals("123.46", FormatUtils.price(123.456));
        assertEquals("-123.46", FormatUtils.price(-123.456));
        assertEquals("0.00", FormatUtils.price(0));
        assertEquals("-", FormatUtils.price(null));
    }

    @Test
    void formatsPercentagesForDashboardDisplay() {
        assertEquals("+1.25%", FormatUtils.percent(0.0125));
        assertEquals("-2.50%", FormatUtils.percent(-2.5));
        assertEquals("+0.00%", FormatUtils.percent(0));
        assertEquals("-", FormatUtils.percent(null));
    }

    @Test
    void formatsNumbersIndependentOfDefaultLocale() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("pt-BR"));

            assertEquals("1,234", FormatUtils.integer(1234));
            assertEquals("123.46", FormatUtils.price(123.456));
            assertEquals("+1.25%", FormatUtils.percent(0.0125));
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    void formatsBlankTextAndIsoDateFallbacks() {
        assertEquals("-", FormatUtils.text(""));
        assertEquals("AAPL", FormatUtils.text("AAPL"));
        assertEquals("not-a-date", DateUtils.displayDateTime("not-a-date"));
    }
}
