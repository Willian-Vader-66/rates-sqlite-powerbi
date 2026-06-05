package com.example.financedashboard.ops;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public final class LocalCommandRunner implements AutoCloseable {
    private final Path projectRoot;
    private final Path pythonExecutable;
    private final ExecutorService executor;

    public LocalCommandRunner(Path projectRoot) {
        this(projectRoot, createDaemonExecutor());
    }

    LocalCommandRunner(Path projectRoot, ExecutorService executor) {
        this.projectRoot = requireProjectRoot(projectRoot);
        this.pythonExecutable = resolvePythonExecutable(this.projectRoot);
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    public static Path detectProjectRoot() {
        Path current = Path.of("").toAbsolutePath().normalize();
        for (Path candidate = current; candidate != null; candidate = candidate.getParent()) {
            if (looksLikeProjectRoot(candidate)) {
                return candidate;
            }
        }
        return current;
    }

    public Path projectRoot() {
        return projectRoot;
    }

    public Path pythonExecutable() {
        return pythonExecutable;
    }

    public CommandSpec fxRatesCommand(String label, List<String> fxRatesArguments, Map<String, String> sessionEnvironment, Duration timeout) {
        List<String> arguments = new ArrayList<>();
        arguments.add("-m");
        arguments.add("fx_rates");
        arguments.addAll(fxRatesArguments == null ? List.of() : fxRatesArguments);
        return new CommandSpec(label, projectRoot, pythonExecutable.toString(), arguments, sensitiveEnvironment(sessionEnvironment), timeout);
    }

    public RunningCommand runAsync(
            CommandSpec spec,
            Consumer<StreamLine> lineConsumer,
            Consumer<CommandResult> completionConsumer
    ) {
        Objects.requireNonNull(spec, "spec");
        AtomicReference<Process> processRef = new AtomicReference<>();
        AtomicBoolean cancelRequested = new AtomicBoolean(false);
        AtomicLong pid = new AtomicLong(-1L);
        CompletableFuture<?> future = CompletableFuture.runAsync(() -> runBlocking(spec, processRef, cancelRequested, pid, lineConsumer, completionConsumer), executor);
        return new RunningCommand(processRef, cancelRequested, pid, future);
    }

    public CommandResult runForTests(CommandSpec spec) {
        AtomicReference<CommandResult> result = new AtomicReference<>();
        runBlocking(spec, new AtomicReference<>(), new AtomicBoolean(false), new AtomicLong(-1L), null, result::set);
        return result.get();
    }

    private void runBlocking(
            CommandSpec spec,
            AtomicReference<Process> processRef,
            AtomicBoolean cancelRequested,
            AtomicLong pid,
            Consumer<StreamLine> lineConsumer,
            Consumer<CommandResult> completionConsumer
    ) {
        SecretRedactor redactor = redactorFor(spec.environment());
        Instant started = Instant.now();
        StringBuilder stdout = new StringBuilder();
        StringBuilder stderr = new StringBuilder();
        int exitCode = -1;
        CommandResult.Status status = CommandResult.Status.FAILED;
        try {
            ProcessBuilder builder = new ProcessBuilder(spec.commandLine());
            builder.directory(spec.workingDirectory().toFile());
            builder.environment().putAll(spec.environment());
            builder.environment().putIfAbsent("PYTHONUNBUFFERED", "1");
            Process process = builder.start();
            processRef.set(process);
            pid.set(process.pid());

            CompletableFuture<Void> stdoutReader = readStream(process.getInputStream(), StreamType.STDOUT, stdout, redactor, lineConsumer);
            CompletableFuture<Void> stderrReader = readStream(process.getErrorStream(), StreamType.STDERR, stderr, redactor, lineConsumer);

            boolean finished = process.waitFor(spec.timeout().toMillis(), TimeUnit.MILLISECONDS);
            if (!finished) {
                status = CommandResult.Status.TIMEOUT;
                cancelRequested.set(true);
                process.destroyForcibly();
            }
            exitCode = process.waitFor();
            CompletableFuture.allOf(stdoutReader, stderrReader).get(5, TimeUnit.SECONDS);
            if (status != CommandResult.Status.TIMEOUT) {
                status = cancelRequested.get()
                        ? CommandResult.Status.CANCELLED
                        : classifyStatus(exitCode, stdout + "\n" + stderr);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            status = CommandResult.Status.CANCELLED;
        } catch (Exception ex) {
            String message = redactor.redact(ex.getClass().getSimpleName() + ": " + ex.getMessage());
            appendLine(stderr, message);
            if (lineConsumer != null) {
                lineConsumer.accept(new StreamLine(spec.label(), StreamType.STDERR, message));
            }
            status = CommandResult.Status.FAILED;
        } finally {
            Duration duration = Duration.between(started, Instant.now());
            CommandResult result = new CommandResult(
                    spec.label(),
                    exitCode,
                    duration,
                    stdout.toString(),
                    stderr.toString(),
                    status
            );
            if (completionConsumer != null) {
                completionConsumer.accept(result);
            }
        }
    }

    private CompletableFuture<Void> readStream(
            InputStream stream,
            StreamType streamType,
            StringBuilder target,
            SecretRedactor redactor,
            Consumer<StreamLine> lineConsumer
    ) {
        return CompletableFuture.runAsync(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String redacted = redactor.redact(line);
                    appendLine(target, redacted);
                    if (lineConsumer != null) {
                        lineConsumer.accept(new StreamLine("", streamType, redacted));
                    }
                }
            } catch (IOException ex) {
                String message = redactor.redact("stream read failed: " + ex.getMessage());
                appendLine(target, message);
                if (lineConsumer != null) {
                    lineConsumer.accept(new StreamLine("", streamType, message));
                }
            }
        }, executor);
    }

    private static synchronized void appendLine(StringBuilder builder, String line) {
        builder.append(line).append(System.lineSeparator());
    }

    private static CommandResult.Status classifyStatus(int exitCode, String combinedOutput) {
        if (exitCode != 0) {
            return CommandResult.Status.FAILED;
        }
        String text = combinedOutput == null ? "" : combinedOutput.toUpperCase(Locale.ROOT);
        if (text.contains("STATUS: NOT_READY") || text.contains("STATUS: FAIL") || text.contains("OVERALL STATUS: **NOT_READY**") || text.contains("OVERALL STATUS: **FAIL**")) {
            return CommandResult.Status.FAILED;
        }
        if (text.contains("EXTERNAL_TEST=FAIL") || text.contains("STATUS=NOT_CONFIGURED")) {
            return CommandResult.Status.FAILED;
        }
        if (text.contains("READY_WITH_WARNINGS") || text.contains(" STATUS: WARN") || text.contains("OVERALL STATUS: **WARN**")) {
            return CommandResult.Status.WARNING;
        }
        return CommandResult.Status.PASSED;
    }

    private static Map<String, String> sensitiveEnvironment(Map<String, String> sessionEnvironment) {
        Map<String, String> result = new LinkedHashMap<>();
        if (sessionEnvironment == null) {
            return result;
        }
        for (Map.Entry<String, String> entry : sessionEnvironment.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null && !entry.getValue().isBlank()) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    private static SecretRedactor redactorFor(Map<String, String> environment) {
        if (environment == null || environment.isEmpty()) {
            return SecretRedactor.empty();
        }
        List<String> values = environment.entrySet().stream()
                .filter(entry -> isSensitiveName(entry.getKey()))
                .map(Map.Entry::getValue)
                .filter(value -> value != null && !value.isBlank())
                .toList();
        return new SecretRedactor(values);
    }

    private static boolean isSensitiveName(String name) {
        if (name == null) {
            return false;
        }
        String normalized = name.toUpperCase(Locale.ROOT);
        return normalized.contains("API_KEY")
                || normalized.contains("TOKEN")
                || normalized.contains("SECRET")
                || normalized.contains("PASSWORD");
    }

    private static Path requireProjectRoot(Path root) {
        Path normalized = (root == null ? detectProjectRoot() : root).toAbsolutePath().normalize();
        if (!looksLikeProjectRoot(normalized)) {
            throw new IllegalArgumentException("Project root not detected at " + normalized);
        }
        return normalized;
    }

    private static boolean looksLikeProjectRoot(Path candidate) {
        return candidate != null
                && Files.exists(candidate.resolve("frontend-java").resolve("pom.xml"))
                && Files.exists(candidate.resolve("src").resolve("fx_rates"));
    }

    private static Path resolvePythonExecutable(Path root) {
        Path windows = root.resolve(".venv").resolve("Scripts").resolve("python.exe");
        if (Files.exists(windows)) {
            return windows;
        }
        Path posix = root.resolve(".venv").resolve("bin").resolve("python");
        if (Files.exists(posix)) {
            return posix;
        }
        return windows;
    }

    private static ExecutorService createDaemonExecutor() {
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "finance-local-command");
            thread.setDaemon(true);
            return thread;
        };
        return Executors.newCachedThreadPool(factory);
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    public enum StreamType {
        STDOUT,
        STDERR
    }

    public record StreamLine(String label, StreamType streamType, String text) {
    }

    public static final class RunningCommand {
        private final AtomicReference<Process> processRef;
        private final AtomicBoolean cancelRequested;
        private final AtomicLong pid;
        private final CompletableFuture<?> future;

        private RunningCommand(
                AtomicReference<Process> processRef,
                AtomicBoolean cancelRequested,
                AtomicLong pid,
                CompletableFuture<?> future
        ) {
            this.processRef = processRef;
            this.cancelRequested = cancelRequested;
            this.pid = pid;
            this.future = future;
        }

        public void cancel() {
            cancelRequested.set(true);
            Process process = processRef.get();
            if (process != null && process.isAlive()) {
                process.destroy();
                try {
                    if (!process.waitFor(3, TimeUnit.SECONDS)) {
                        process.destroyForcibly();
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    process.destroyForcibly();
                }
            }
        }

        public OptionalLong pid() {
            long value = pid.get();
            return value > 0 ? OptionalLong.of(value) : OptionalLong.empty();
        }

        public boolean isDone() {
            return future.isDone();
        }
    }
}
