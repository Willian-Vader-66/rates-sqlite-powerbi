package com.example.financedashboard.ops;

import java.time.Duration;

public record CommandResult(
        String label,
        int exitCode,
        Duration duration,
        String stdout,
        String stderr,
        Status status
) {
    public enum Status {
        PASSED,
        WARNING,
        FAILED,
        CANCELLED,
        TIMEOUT
    }

    public boolean successful() {
        return status == Status.PASSED || status == Status.WARNING;
    }
}
