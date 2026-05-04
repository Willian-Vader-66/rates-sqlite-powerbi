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
        assertEquals("123.46 USD", FormatUtils.usd(123.456));
    }

    @Test
    void formatsValuesWithExplicitDisplayUnits() {
        assertEquals("5.27368 BRL per 1 USD", FormatUtils.valueWithUnit(5.27368, "fx_rate", "BRL per 1 USD"));
        assertEquals("263.10 USD", FormatUtils.valueWithUnit(263.0982, "currency_usd", "USD"));
        assertEquals("10.52 % a.a.", FormatUtils.valueWithUnit(10.5235, "percent", "% a.a."));
        assertEquals("-", FormatUtils.valueWithUnit(null, "currency_usd", "USD"));
    }

    @Test
    void formatsCompactAndByteValuesForDashboardDisplay() {
        assertEquals("104.65K", FormatUtils.compact(104653.36));
        assertEquals("1.50 MB", FormatUtils.bytes(1572864));
        assertEquals("-", FormatUtils.compact(null));
        assertEquals("-", FormatUtils.bytes(null));
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
