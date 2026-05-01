package com.example.financedashboard;

import com.example.financedashboard.util.DateUtils;
import com.example.financedashboard.util.FormatUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FormatUtilsTest {
    @Test
    void formatsNumbersForDashboardDisplay() {
        assertEquals("1,234", FormatUtils.integer(1234));
        assertEquals("123.46", FormatUtils.price(123.456));
        assertEquals("+1.25%", FormatUtils.percent(0.0125));
        assertEquals("-2.50%", FormatUtils.percent(-2.5));
        assertEquals("-", FormatUtils.price(null));
    }

    @Test
    void formatsBlankTextAndIsoDateFallbacks() {
        assertEquals("-", FormatUtils.text(""));
        assertEquals("AAPL", FormatUtils.text("AAPL"));
        assertEquals("not-a-date", DateUtils.displayDateTime("not-a-date"));
    }
}
