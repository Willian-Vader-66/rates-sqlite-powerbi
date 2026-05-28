# PowerShell Session Guide

## Why the Key Disappears After setup_live_env.ps1

PowerShell environment variables under `$env:` live in the current process and child processes.

When you run:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_live_env.ps1
```

Windows starts a new PowerShell process. The script can set `TWELVE_DATA_API_KEY` inside that process, and every command run inside the script can see it. When that process exits, the original terminal does not inherit the variable.

## Keep Variables In The Current Session

Use dot-source when you want `TWELVE_DATA_API_KEY`, `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE`, and `FX_RATES_USE_TRUSTSTORE` to remain available for the next commands in the same terminal. The setup script uses hidden input and prints only `present`, `key_length`, and a masked preview:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi-git
Set-ExecutionPolicy -Scope Process Bypass -Force
. .\scripts\setup_live_env.ps1
```

Then run:

```powershell
.\.venv\Scripts\python.exe -m fx_rates providers status --external-test
.\.venv\Scripts\python.exe -m fx_rates dashboard build-live-db --days 365 --db-path .tmp\live-main-candidate.sqlite --external-test
```

## Recommended One-Shot Pipeline

The safest release flow is to run everything in one PowerShell process:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi-git
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_live_pipeline.ps1
```

This script configures TLS, asks for `TWELVE_DATA_API_KEY`, runs provider checks, builds the live candidate, validates samples, runs audits, runs API smoke, and finishes with `promote-live --dry-run`.

It does not save the key, does not create `.env`, and does not promote `data/fx.sqlite`. If the key appeared in a screenshot, terminal transcript, log, or command history, rotate it in the Twelve Data dashboard.

## Manual Same-Session Flow

Manual commands work only when the key is present in the same PowerShell session:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi-git
$secure = Read-Host "Paste Twelve Data API key" -AsSecureString
$bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
try { $env:TWELVE_DATA_API_KEY = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) } finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }

.\.venv\Scripts\python.exe -m fx_rates providers status --external-test
.\.venv\Scripts\python.exe -m fx_rates dashboard validate-samples --db-path .tmp\live-main-candidate.sqlite --samples-per-symbol 5 --external-test
.\.venv\Scripts\python.exe -m fx_rates dashboard promote-live --candidate-db .tmp\live-main-candidate.sqlite --dry-run
```

Do not paste the key into docs, committed `.env` files, logs, screenshots, or chat transcripts.

## Check Only

Use this to validate local paths and dependencies without asking for a key, calling providers, or creating a DB:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_live_pipeline.ps1 -CheckOnly
```
