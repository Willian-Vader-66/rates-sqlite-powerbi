package com.example.financedashboard.ops;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record CommandSpec(
        String label,
        Path workingDirectory,
        String executable,
        List<String> arguments,
        Map<String, String> environment,
        Duration timeout
) {
    public CommandSpec {
        Objects.requireNonNull(label, "label");
        Objects.requireNonNull(workingDirectory, "workingDirectory");
        Objects.requireNonNull(executable, "executable");
        arguments = List.copyOf(arguments == null ? List.of() : arguments);
        environment = Map.copyOf(environment == null ? Map.of() : environment);
        timeout = timeout == null ? Duration.ofMinutes(15) : timeout;
    }

    public List<String> commandLine() {
        List<String> command = new ArrayList<>(arguments.size() + 1);
        command.add(executable);
        command.addAll(arguments);
        return List.copyOf(command);
    }

    public String displayCommand(SecretRedactor redactor) {
        SecretRedactor effectiveRedactor = redactor == null ? SecretRedactor.empty() : redactor;
        return effectiveRedactor.redact(String.join(" ", commandLine()));
    }

    public CommandSpec withEnvironment(Map<String, String> additions) {
        Map<String, String> merged = new LinkedHashMap<>(environment);
        if (additions != null) {
            merged.putAll(additions);
        }
        return new CommandSpec(label, workingDirectory, executable, arguments, merged, timeout);
    }
}
