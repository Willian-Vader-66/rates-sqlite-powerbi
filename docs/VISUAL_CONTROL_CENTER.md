# Finance Monitor Visual Control Center

The Visual Control Center is the JavaFX operations screen for the local Finance Monitor. It is intended to replace day-to-day memorization of PowerShell commands. PowerShell starts the system; JavaFX runs the operational actions through the local Python CLI/API.

## Start

From the repository root:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_finance_monitor.ps1
```

Optional startup flags:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_finance_monitor.ps1 -StartBackend
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_finance_monitor.ps1 -CheckOnly
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_finance_monitor.ps1 -LiveMode
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_finance_monitor.ps1 -DemoMode
```

`-CheckOnly` validates paths, Java, Maven, and Python. It does not open the UI, ask for keys, start the backend, build data, or touch SQLite.

## Control Center Sections

- System Status: reads `/api/system/status` and shows whether the local backend is online.
- Data Mode: shows DEMO, LIVE 365D, MIXED, or UNKNOWN plus health, coverage, requested days, providers, warnings, and failures.
- Secret Keys: accepts `TWELVE_DATA_API_KEY` for the current JavaFX session only.
- Providers: runs `python -m fx_rates providers status --external-test`.
- Live 365D Pipeline: runs provider validation, crypto history test, candidate DB build, sample validation, audit, API smoke, and promotion dry-run.
- Backend Server: starts, stops, restarts, and health-checks the local API server.
- Database: shows main and candidate SQLite status and exposes audit/promotion actions.
- Reports & Logs: reads generated markdown reports and local logs inside JavaFX.
- Advanced History: documents 3Y/5Y/10Y as future-only.

## Twelve Data Key

Paste only the key value in Secret Keys. Do not paste a PowerShell command, path, quoted string, multiline text, or a full `python -m fx_rates ...` command.

The JavaFX app keeps `TWELVE_DATA_API_KEY` in memory for the current session and passes it only to child Python processes through environment variables. It does not write the key to `.env`, docs, reports, logs, or committed files. The UI shows only `present=true`, key length, and a masked preview such as `abcd****`. Invalid input is rejected before provider calls.

If JavaFX is started from a PowerShell session where `TWELVE_DATA_API_KEY` is already set, the Control Center detects the environment key automatically when the format is plausible. It does not copy the value into the password field, does not display it, and uses it only for the current JavaFX process and child provider commands. Provider Validation uses the provider status output as the source of truth, so a Twelve Data `external_test=pass` cannot remain visually blocked as a missing secret.

## Live 365D Flow

Use the Live 365D Pipeline section:

1. Run Full Validation.
2. Review step logs and reports, especially Step 4 - Validate Samples.
3. Confirm that Step 7 - Promote Dry Run passed.
4. Use Promote Candidate only after manual review.

Step 8 is never run by Run Full Validation. Promotion requires a confirmation dialog and calls `promote-live` with backup enabled.

Step 4 summarizes:

- Samples OK
- Samples WARN
- Samples FAIL
- Provider failures
- Rate limit detected
- Internal validation: Passed/Failed
- External validation: Passed/Rate Limited/Blocked by Missing Secret/Blocked by Provider/TLS/Failed/Skipped
- Promotion gate: Allowed/Allowed with warning/Blocked
- Report path
- Main reason code

Historical sample validation uses historical endpoints. For crypto, old BTC/ETH/BNB/SOL/XRP points are validated with CoinGecko `market_chart/range`; `simple/price` is used only for latest quote/current-price validation. If a provider rate limit is detected and the candidate remains internally valid, the UI shows the step as Passed with Warnings and explains: `Validação externa limitada pelo provider. O banco candidato passou nas auditorias internas, mas a confirmação por amostragem externa ficou parcial.`

Status badges are Pending, Running, Passed, Passed with Warnings, Failed, Blocked by Missing Secret, Blocked by Provider/TLS, Ready for Dry Run, Ready for Promotion, and Skipped.

`Blocked by Missing Secret` means the UI refused to call a stock provider without a plausible in-memory `TWELVE_DATA_API_KEY`. `Blocked by Provider/TLS` means the provider check failed because of TLS/CA, SSL, or external connectivity and is reported separately from candidate DB data quality. Logs and report previews are redacted before display.

Important reason codes include `VALIDATION_OK`, `HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE`, `HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE`, `HISTORICAL_SAMPLE_NEAREST_DATE_WARN`, `HISTORICAL_SAMPLE_DIVERGENCE_FAIL`, `CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST`, `EXTERNAL_RATE_LIMIT`, `EXTERNAL_PROVIDER_UNAVAILABLE`, `PROVIDER_TLS_ERROR`, `PROVIDER_KEY_MISSING`, `INSUFFICIENT_HISTORY_POINTS`, `MISSING_LIVE_HISTORY`, `DUPLICATED_HISTORY_DATE`, `NON_POSITIVE_PRICE`, `FUTURE_HISTORY_DATE`, `STALE_LATEST_QUOTE`, `LATEST_QUOTE_DIVERGENCE_WARN`, and `LATEST_QUOTE_DIVERGENCE_FAIL`.

Step 5 - Audit Live summarizes critical failures, warnings, and monthly macro policy notes. `IPCA_MONTHLY` is monthly and may lag daily market data. When its latest value is inside the allowed monthly freshness window, the Control Center shows the audit as Passed or Passed with Warnings instead of Failed and displays: `IPCA is a monthly macro series and may lag daily market data. Latest value is within allowed monthly freshness window.`

## Data Modes

- DEMO: simulated deterministic data for development, tests, and demonstrations. It is not real market data.
- LIVE: real provider data for the current product scope, standard 365D.
- MIXED: live and demo records exist together; use only for transition or diagnosis.
- UNKNOWN: origin cannot be identified; treat as unsafe until audited.

## Backend

The Control Center starts the backend with:

```powershell
python -m fx_rates serve --host 127.0.0.1 --port 8000
```

It does not start a second instance if the port is already in use. It stops only a backend process that was started by the JavaFX UI.

## Reports

The Reports & Logs section can display:

- `docs/LIVE_BUILD_REPORT.md`
- `docs/LIVE_SAMPLE_VALIDATION_REPORT.md`
- `docs/LIVE_AUDIT_REPORT.md`
- `docs/API_LIVE_SMOKE_REPORT.md`
- `docs/LIVE_365D_RELEASE_GATE_REPORT.md`
- `docs/LIVE_DATA_SCOPE.md`
- `docs/LIVE_PROMOTION_GUIDE.md`
- `docs/WORKSPACE_MIGRATION_AUDIT.md`

Logs are redacted before display.
