param(
    [switch]$CheckOnly,
    [switch]$SkipProviderTest,
    [switch]$SkipBuild,
    [switch]$SkipSamples,
    [switch]$SkipSmoke,
    [switch]$NoPromptKey,
    [string]$TwelveKey = ""
)

$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host "[live-pipeline] $Message" -ForegroundColor Cyan
}

function Write-Ok {
    param([string]$Message)
    Write-Host "[live-pipeline] OK: $Message" -ForegroundColor Green
}

function Write-Fail {
    param([string]$Message)
    Write-Host "[live-pipeline] FAIL: $Message" -ForegroundColor Red
}

function Validate-TwelveKey {
    param([string]$Value)
    $key = $Value.Trim()
    $lower = $key.ToLowerInvariant()
    $badMarkers = @("cd c:", "python", "fx_rates", '$env:', "powershell", "setx ", " -m ", "dashboard ", "providers ", "read-host", "twleve_data_api_key", "twelve_data_api_key=")
    $badValues = @("none", "null", "sua_chave_aqui", "your_key", "your_api_key", "your_twelve_data_api_key", "change_me", "changeme", "todo", "test", "fake", "demo", "placeholder")

    if (-not $key) {
        throw "TWELVE_DATA_API_KEY is empty."
    }
    if ($key -match "[`r`n]") {
        throw "TWELVE_DATA_API_KEY must be a single line."
    }
    foreach ($marker in $badMarkers) {
        if ($lower.Contains($marker)) {
            throw "The pasted value looks like a command, not an API key. Paste only the Twelve Data key."
        }
    }
    if (($key -split "\s+").Count -gt 1) {
        throw "The pasted value contains spaces. Paste only the Twelve Data key."
    }
    foreach ($bad in $badValues) {
        if ($lower -eq $bad -or $lower.Contains($bad)) {
            throw "TWELVE_DATA_API_KEY is placeholder-like. Paste only the real Twelve Data key."
        }
    }
    if ($key.Length -lt 12) {
        throw "TWELVE_DATA_API_KEY is too short or placeholder-like."
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

function Read-TwelveKeyIntoEnvironment {
    if ($TwelveKey) {
        Write-Warning "Passing -TwelveKey can expose the key in shell history. Prefer the hidden prompt."
        $env:TWELVE_DATA_API_KEY = Validate-TwelveKey $TwelveKey
        Write-Step "TWELVE_DATA_API_KEY set from -TwelveKey for this process only. Value will not be printed."
        Write-TwelveKeySummary
        return
    }
    if ($env:TWELVE_DATA_API_KEY) {
        $env:TWELVE_DATA_API_KEY = Validate-TwelveKey $env:TWELVE_DATA_API_KEY
        Write-Step "TWELVE_DATA_API_KEY already present in this process. Value will not be printed."
        Write-TwelveKeySummary
        return
    }
    if ($NoPromptKey) {
        throw "TWELVE_DATA_API_KEY is not configured in this PowerShell session. Run run_live_pipeline.ps1 and enter the key when prompted, or set it manually only for this session."
    }

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
    Write-Step "TWELVE_DATA_API_KEY set in this PowerShell process only. Value will not be printed."
    Write-TwelveKeySummary
}

function Ensure-TwelveKeyConfigured {
    if ([string]::IsNullOrWhiteSpace($env:TWELVE_DATA_API_KEY)) {
        throw "TWELVE_DATA_API_KEY is not configured in this PowerShell session. Run run_live_pipeline.ps1 and enter the key when prompted, or set it manually only for this session."
    }
    $env:TWELVE_DATA_API_KEY = Validate-TwelveKey $env:TWELVE_DATA_API_KEY
}

$StepStatuses = [ordered]@{}
$Warnings = New-Object System.Collections.Generic.List[string]
$Failures = New-Object System.Collections.Generic.List[string]

function Invoke-Checked {
    param(
        [string]$Label,
        [string]$Command,
        [string[]]$Arguments,
        [switch]$RequiresTwelve
    )
    Write-Step $Label
    if ($RequiresTwelve) {
        Ensure-TwelveKeyConfigured
    }
    $script:StepStatuses[$Label] = "RUNNING"
    $output = & $Command @Arguments 2>&1
    $code = $LASTEXITCODE
    foreach ($line in $output) {
        Write-Host $line
    }
    if ($code -ne 0) {
        $script:StepStatuses[$Label] = "FAIL"
        $script:Failures.Add("$Label failed with exit code $code.") | Out-Null
        throw "$Label failed with exit code $LASTEXITCODE."
    }
    $text = ($output | Out-String)
    $parsedStatus = "OK"
    if ($text -match "LIVE-FIRST DB BUILD STATUS:\s*([A-Z_ ]+)") {
        $parsedStatus = $Matches[1].Trim()
    } elseif ($text -match "LIVE SAMPLE VALIDATION STATUS:\s*([A-Z_ ]+)") {
        $parsedStatus = $Matches[1].Trim()
    } elseif ($text -match "LIVE AUDIT STATUS:\s*([A-Z_ ]+)") {
        $parsedStatus = $Matches[1].Trim()
    } elseif ($text -match "API LIVE SMOKE Status:\s*([A-Z_ ]+)") {
        $parsedStatus = $Matches[1].Trim()
    }
    if ($parsedStatus -eq "WARN" -or $parsedStatus -eq "READY_WITH_WARNINGS") {
        $script:Warnings.Add("$Label reported $parsedStatus.") | Out-Null
    }
    $script:StepStatuses[$Label] = $parsedStatus
}

function Write-PipelineSummary {
    param(
        [string]$Status,
        [string]$CandidateDb,
        [string]$NextAction
    )
    Write-Host "LIVE PIPELINE STATUS: $Status"
    Write-Host "providers status: $($StepStatuses['Running provider external tests'])"
    Write-Host "crypto test-history: $($StepStatuses['Testing CoinGecko 365D history'])"
    Write-Host "build-live-db: $($StepStatuses['Building live DB candidate'])"
    Write-Host "audit-live: $($StepStatuses['Running audit-live'])"
    Write-Host "validate-samples: $($StepStatuses['Validating live samples'])"
    Write-Host "api smoke-live: $($StepStatuses['Running API smoke-live'])"
    Write-Host "promote dry-run: $($StepStatuses['Running promotion dry-run'])"
    Write-Host "candidate DB: $CandidateDb"
    Write-Host "warnings: $($Warnings.Count)"
    foreach ($warning in $Warnings) {
        Write-Host "WARN: $warning"
    }
    Write-Host "failures: $($Failures.Count)"
    foreach ($failure in $Failures) {
        Write-Host "FAIL: $failure"
    }
    Write-Host "next action: $NextAction"
}

$FinalStatus = "NOT READY"

try {
    $RepoRoot = Resolve-Path $PSScriptRoot
    Set-Location $RepoRoot

    $Python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
    if (-not (Test-Path $Python)) {
        throw "Python virtualenv not found at $Python. Create it first with: python -m venv .venv"
    }
    Write-Ok "Repository root: $RepoRoot"
    Write-Ok "Python: $Python"

    if ($CheckOnly) {
        Write-Step "CheckOnly mode: validating paths and dependencies only. No key prompt, no provider calls, no DB build."
        Invoke-Checked "Checking Python version" $Python @("--version")
        Invoke-Checked "Checking certifi/truststore imports" $Python @("-c", "import certifi, truststore; print('certifi=' + certifi.where()); print('truststore=installed')")
        if (Test-Path ".tmp\live-main-candidate.sqlite") {
            Write-Step "Existing staging DB detected but not touched: .tmp\live-main-candidate.sqlite"
        }
        $FinalStatus = "READY"
        Write-Host "LIVE PIPELINE CHECK STATUS: READY"
        Write-Host "Requested period: 365 days"
        Write-Host "History mode: standard"
        Write-Host "Advanced history: disabled"
        Write-Host "Advanced max: 10 years with paid providers"
        exit 0
    }

    Write-Step "Upgrading pip, certifi, and truststore in .venv"
    Invoke-Checked "Installing TLS dependencies" $Python @("-m", "pip", "install", "--upgrade", "pip", "certifi", "truststore")

    Write-Step "Configuring certificate environment variables from certifi"
    $cert = (& $Python -c "import certifi; print(certifi.where())").Trim()
    if ($LASTEXITCODE -ne 0 -or -not $cert -or -not (Test-Path $cert)) {
        throw "Could not resolve certifi CA bundle."
    }
    $env:SSL_CERT_FILE = $cert
    $env:REQUESTS_CA_BUNDLE = $cert
    $env:CURL_CA_BUNDLE = $cert
    $env:FX_RATES_USE_TRUSTSTORE = "1"

    Read-TwelveKeyIntoEnvironment

    Invoke-Checked "Running env doctor" $Python @("-m", "fx_rates", "env", "doctor")

    if (-not $SkipProviderTest) {
        Invoke-Checked "Running provider external tests" $Python @("-m", "fx_rates", "providers", "status", "--external-test") -RequiresTwelve
    } else {
        Write-Step "Skipping provider external tests because -SkipProviderTest was provided."
        $StepStatuses["Running provider external tests"] = "SKIPPED"
    }

    $FinalStatus = "PARTIALLY FUNCTIONAL"

    if ($SkipBuild) {
        $FinalStatus = "PARTIALLY FUNCTIONAL"
        $StepStatuses["Building live DB candidate"] = "SKIPPED"
        $StepStatuses["Testing CoinGecko 365D history"] = "SKIPPED"
        $StepStatuses["Validating live samples"] = "SKIPPED"
        $StepStatuses["Running audit-live"] = "SKIPPED"
        $StepStatuses["Running API smoke-live"] = "SKIPPED"
        $StepStatuses["Running promotion dry-run"] = "SKIPPED"
        Write-PipelineSummary "PARTIALLY FUNCTIONAL" ".tmp\live-main-candidate.sqlite" "Build was skipped; rerun without -SkipBuild for the release gate."
        exit 0
    }

    $CandidateDb = Join-Path $RepoRoot ".tmp\live-main-candidate.sqlite"
    $TmpDir = Join-Path $RepoRoot ".tmp"
    if (-not (Test-Path $TmpDir)) {
        New-Item -ItemType Directory -Path $TmpDir | Out-Null
    }
    Write-Step "Removing previous staging candidate if present"
    Remove-Item -LiteralPath $CandidateDb -Force -ErrorAction SilentlyContinue

    Invoke-Checked "Testing CoinGecko 365D history" $Python @("-m", "fx_rates", "crypto", "test-history", "--symbols", "BTC,ETH,BNB,SOL,XRP", "--days", "365")

    Invoke-Checked "Building live DB candidate" $Python @("-m", "fx_rates", "dashboard", "build-live-db", "--days", "365", "--db-path", ".tmp\live-main-candidate.sqlite", "--external-test") -RequiresTwelve

    if (-not $SkipSamples) {
        Invoke-Checked "Validating live samples" $Python @("-m", "fx_rates", "dashboard", "validate-samples", "--db-path", ".tmp\live-main-candidate.sqlite", "--samples-per-symbol", "5", "--external-test") -RequiresTwelve
    } else {
        Write-Step "Skipping sample validation because -SkipSamples was provided."
        $StepStatuses["Validating live samples"] = "SKIPPED"
    }

    Invoke-Checked "Running audit-live" $Python @("-m", "fx_rates", "dashboard", "audit-live", "--db-path", ".tmp\live-main-candidate.sqlite")

    if (-not $SkipSmoke) {
        Invoke-Checked "Running API smoke-live" $Python @("-m", "fx_rates", "api", "smoke-live", "--db-path", ".tmp\live-main-candidate.sqlite", "--port", "8001")
    } else {
        Write-Step "Skipping API smoke-live because -SkipSmoke was provided."
        $StepStatuses["Running API smoke-live"] = "SKIPPED"
    }

    Invoke-Checked "Running promotion dry-run" $Python @("-m", "fx_rates", "dashboard", "promote-live", "--candidate-db", ".tmp\live-main-candidate.sqlite", "--dry-run") -RequiresTwelve

    if ($SkipProviderTest -or $SkipSamples -or $SkipSmoke) {
        $FinalStatus = "PARTIALLY FUNCTIONAL"
    } elseif ($Warnings.Count -gt 0) {
        $FinalStatus = "READY_WITH_WARNINGS"
    } else {
        $FinalStatus = "READY"
    }
    Write-PipelineSummary $FinalStatus ".tmp\live-main-candidate.sqlite" "Review dry-run output. Promotion remains manual."
    Write-Host "Requested period: 365 days"
    Write-Host "History mode: standard"
    Write-Host "Advanced history: disabled"
    Write-Host "Advanced max: 10 years with paid providers"
    Write-Host "No real promotion was performed. To promote manually after review:"
    Write-Host ".\.venv\Scripts\python.exe -m fx_rates dashboard promote-live --candidate-db .tmp\live-main-candidate.sqlite --to-db data\fx.sqlite --backup"
    exit 0
} catch {
    $message = $_.Exception.Message
    if (-not ($Failures -contains $message)) {
        $Failures.Add($message) | Out-Null
    }
    Write-Fail $message
    Write-PipelineSummary "NOT READY" ".tmp\live-main-candidate.sqlite" "Fix the failure above and rerun the same pipeline command."
    exit 1
}
