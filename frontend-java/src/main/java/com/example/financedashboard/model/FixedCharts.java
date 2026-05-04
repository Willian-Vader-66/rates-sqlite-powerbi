package com.example.financedashboard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record FixedCharts(
        List<FixedChart> fx,
        List<FixedChart> crypto,
        List<FixedChart> macro
) {
}
