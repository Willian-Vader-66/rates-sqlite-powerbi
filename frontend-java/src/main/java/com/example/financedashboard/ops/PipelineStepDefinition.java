package com.example.financedashboard.ops;

import java.time.Duration;
import java.util.List;

public record PipelineStepDefinition(
        int number,
        String name,
        List<String> fxRatesArguments,
        boolean requiresTwelveKey,
        boolean requiresConfirmation,
        boolean promoteStep,
        Duration timeout
) {
    public PipelineStepDefinition {
        fxRatesArguments = List.copyOf(fxRatesArguments == null ? List.of() : fxRatesArguments);
        timeout = timeout == null ? Duration.ofMinutes(20) : timeout;
    }

    public String label() {
        return "Step " + number + " - " + name;
    }
}
