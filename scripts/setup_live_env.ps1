param(
    [switch]$SkipInstall,
    [switch]$Help
)

$ErrorActionPreference = "Stop"

function Show-Help {
    Write-Host "setup_live_env.ps1"
    Write-Host ""
    Write-Host "Configures certifi/truststore and TWELVE_DATA_API_KEY for the current PowerShell process."
    Write-Host "It does not save API keys to files and does not create .env."
    Write-Host ""
    Write-Host "Recommended dot-source usage when you want variables to remain available:"
    Write-Host "  cd C:\Projetos_Local\rates-sqlite-powerbi-git"
    Write-Host "  Set-ExecutionPolicy -Scope Process Bypass -Force"
    Write-Host "  . .\scripts\setup_live_env.ps1"
    Write-Host ""
    Write-Host "One-shot isolated usage:"
    Write-Host "  powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_live_env.ps1"
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -SkipInstall   Do not run pip install --upgrade pip certifi truststore."
    Write-Host "  -Help          Show this help and exit without asking for a key."
}

if ($Help) {
    Show-Help
    return
}

function Write-Step {
    param([string]$Message)
    Write-Host "[live-env] $Message" -ForegroundColor Cyan
}

function Fail {
    param([string]$Message)
    throw "[live-env] $Message"
}

function Validate-TwelveKey {
    param([string]$Value)
    $key = $Value.Trim()
    $lower = $key.ToLowerInvariant()
    $badMarkers = @("cd c:", "python", "fx_rates", '$env:', "powershell", "setx ", " -m ", "dashboard ", "providers ", "read-host", "twelve_data_api_key=")
    $badValues = @("none", "null", "sua_chave_aqui", "your_key", "your_api_key", "your_twelve_data_api_key", "change_me", "changeme", "todo", "test", "fake", "demo", "placeholder")

    if (-not $key) {
        Fail "TWELVE_DATA_API_KEY is empty."
    }
    if ($key -match "[`r`n]") {
        Fail "TWELVE_DATA_API_KEY must be a single line."
    }
    foreach ($marker in $badMarkers) {
        if ($lower.Contains($marker)) {
            Fail "The pasted value looks like a command, not an API key. Paste only the Twelve Data key."
        }
    }
    if (($key -split "\s+").Count -gt 1) {
        Fail "The pasted value contains spaces. Paste only the Twelve Data key."
    }
    foreach ($bad in $badValues) {
        if ($lower -eq $bad -or $lower.Contains($bad)) {
            Fail "TWELVE_DATA_API_KEY is placeholder-like. Paste only the real Twelve Data key."
        }
    }
    if ($key.Length -lt 12) {
        Fail "TWELVE_DATA_API_KEY is too short or placeholder-like."
    }
    return $key
}

function ConvertFrom-SecureStringInMemory {
    param([Security.SecureString]$SecureValue)
    $ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecureValue)
    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)
    } finally {
        if ($ptr -ne [IntPtr]::Zero) {
            [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr)
        }
    }
}

function Get-MaskedPreview {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return "-"
    }
    $prefixLength = [Math]::Min(4, $Value.Length)
    return $Value.Substring(0, $prefixLength) + "****"
}

function Write-TwelveKeySummary {
    $present = -not [string]::IsNullOrWhiteSpace($env:TWELVE_DATA_API_KEY)
    $length = if ($present) { $env:TWELVE_DATA_API_KEY.Length } else { 0 }
    $preview = if ($present) { Get-MaskedPreview $env:TWELVE_DATA_API_KEY } else { "-" }
    Write-Step ("TWELVE_DATA_API_KEY present: " + $present.ToString().ToLowerInvariant())
    Write-Step "TWELVE_DATA_API_KEY key_length: $length"
    Write-Step "TWELVE_DATA_API_KEY masked_preview: $preview"
}

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $RepoRoot
$IsDotSourced = $MyInvocation.InvocationName -eq "."

$Python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    Fail "Python virtualenv not found at $Python. Create it first with: python -m venv .venv"
}

if (-not $SkipInstall) {
    Write-Step "Upgrading pip, certifi, and truststore in .venv"
    & $Python -m pip install --upgrade pip certifi truststore
    if ($LASTEXITCODE -ne 0) {
        Fail "pip install failed."
    }
} else {
    Write-Step "Skipping package install because -SkipInstall was provided"
}

Write-Step "Configuring certificate environment variables from certifi"
$cert = (& $Python -c "import certifi; print(certifi.where())").Trim()
if ($LASTEXITCODE -ne 0 -or -not $cert -or -not (Test-Path $cert)) {
    Fail "Could not resolve certifi CA bundle."
}

$env:SSL_CERT_FILE = $cert
$env:REQUESTS_CA_BUNDLE = $cert
$env:CURL_CA_BUNDLE = $cert
$env:FX_RATES_USE_TRUSTSTORE = "1"

if ($env:TWELVE_DATA_API_KEY) {
    $env:TWELVE_DATA_API_KEY = Validate-TwelveKey $env:TWELVE_DATA_API_KEY
    Write-Step "TWELVE_DATA_API_KEY already present in this PowerShell process. Value will not be printed."
} else {
    Write-Step "Reading Twelve Data key for this PowerShell session only"
    $secure = Read-Host "Paste Twelve Data API key" -AsSecureString
    $plain = $null
    try {
        $plain = ConvertFrom-SecureStringInMemory $secure
        $env:TWELVE_DATA_API_KEY = Validate-TwelveKey $plain
    } finally {
        $plain = $null
        if ($secure -and ($secure -is [IDisposable])) {
            $secure.Dispose()
        }
    }
    Write-Step "TWELVE_DATA_API_KEY set in process environment only. Value will not be printed."
}
Write-TwelveKeySummary

Write-Step "Running env doctor"
& $Python -m fx_rates env doctor
if ($LASTEXITCODE -ne 0) {
    Write-Warning "env doctor reported issues. Review docs/ENV_DOCTOR_REPORT.md"
}

Write-Step "Running provider external tests"
& $Python -m fx_rates providers status --external-test
if ($LASTEXITCODE -ne 0) {
    Write-Warning "provider status reported issues."
}

Write-Host ""
if ($IsDotSourced) {
    Write-Step "Dot-source mode detected. Environment variables remain available in this PowerShell session."
} else {
    Write-Warning "IMPORTANTE: se este script foi executado com powershell.exe -File, as variaveis de ambiente configuradas aqui nao persistem na janela PowerShell original."
    Write-Host "Para manter TWELVE_DATA_API_KEY disponivel para os proximos comandos, use dot-source na sessao atual:"
    Write-Host ""
    Write-Host "cd C:\Projetos_Local\rates-sqlite-powerbi-git"
    Write-Host "Set-ExecutionPolicy -Scope Process Bypass -Force"
    Write-Host ". .\scripts\setup_live_env.ps1"
    Write-Host ""
    Write-Host "Depois rode:"
    Write-Host ".\.venv\Scripts\python.exe -m fx_rates providers status --external-test"
    Write-Host ".\.venv\Scripts\python.exe -m fx_rates dashboard build-live-db --days 365 --db-path .tmp\live-main-candidate.sqlite --external-test"
}
