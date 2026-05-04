package com.example.financedashboard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DashboardCard(
        String label,
        Double value,
        Double change,
        String unit,
        String status
) {
}
