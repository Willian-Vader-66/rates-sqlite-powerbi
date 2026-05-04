package com.example.financedashboard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ChartPoint(
        String date,
        Double value
) {
}
