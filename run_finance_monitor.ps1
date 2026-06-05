param(
    [switch]$CheckOnly,
    [switch]$StartBackend,
    [switch]$SkipBackend,
    [int]$Port = 8000,
    [string]$HostAddress = "127.0.0.1",
    [switch]$DemoMode,
    [switch]$LiveMode
)

$ErrorActionPreference = "Stop"

function Write-StartupLog {
    param([string]$Message)
    if ($script:StartupLog) {
        Add-Content -LiteralPath $script:StartupLog -Value ("[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message)
    }
}

function Write-Info {
    param([string]$Message)
    Write-Host "[finance-monitor] $Message" -ForegroundColor Cyan
    Write-StartupLog $Message
}

function Write-Ok {
    param([string]$Message)
    Write-Host "[finance-monitor] OK: $Message" -ForegroundColor Green
    Write-StartupLog "OK: $Message"
}

function Write-Warn {
    param([string]$Message)
    Write-Host "[finance-monitor] WARN: $Message" -ForegroundColor Yellow
    Write-StartupLog "WARN: $Message"
}

function Write-Fail {
    param([string]$Message)
    Write-Host "[finance-monitor] FAIL: $Message" -ForegroundColor Red
    Write-StartupLog "FAIL: $Message"
}

function Test-CommandAvailable {
    param([string]$CommandName)
    return $null -ne (Get-Command $CommandName -ErrorAction SilentlyContinue)
}

function Test-PortInUse {
    param([int]$PortNumber)
    try {
        $connection = Get-NetTCPConnection -LocalPort $PortNumber -State Listen -ErrorAction SilentlyContinue
        return $null -ne $connection
    } catch {
        try {
            $client = New-Object Net.Sockets.TcpClient
            $async = $client.BeginConnect($HostAddress, $PortNumber, $null, $null)
            $connected = $async.AsyncWaitHandle.WaitOne(500, $false)
            if ($connected) {
                $client.EndConnect($async)
            }
            $client.Close()
            return $connected
        } catch {
            return $false
        }
    }
}

function Assert-RequiredPath {
    param(
        [string]$Path,
        [string]$Description
    )
    if (-not (Test-Path -LiteralPath $Path)) {
        throw "$Description not found: $Path"
    }
    Write-Ok "$Description found."
}

function Stop-StartedBackend {
    param([System.Diagnostics.Process]$Process)
    if ($null -eq $Process) {
        return
    }
    try {
        $current = Get-Process -Id $Process.Id -ErrorAction SilentlyContinue
        if ($null -eq $current) {
            return
        }
        Write-Info "Stopping backend process PID $($Process.Id)."
        Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
        Write-Ok "Backend process stopped."
    } catch {
        Write-Warn "Could not stop backend PID $($Process.Id): $($_.Exception.Message)"
    }
}

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = (Get-Location).Path
}
$RepoRoot = (Resolve-Path $RepoRoot).Path
$LogFolder = Join-Path $RepoRoot "logs"
if (-not (Test-Path -LiteralPath $LogFolder)) {
    New-Item -ItemType Directory -Path $LogFolder | Out-Null
}
$script:StartupLog = Join-Path $LogFolder "finance-monitor-startup.log"
if (-not (Test-Path -LiteralPath $script:StartupLog)) {
    New-Item -ItemType File -Path $script:StartupLog -Force | Out-Null
}

$PythonPath = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$FrontendPath = Join-Path $RepoRoot "frontend-java"
$FrontendPom = Join-Path $FrontendPath "pom.xml"
$LivePipeline = Join-Path $RepoRoot "run_live_pipeline.ps1"
$FxRatesSource = Join-Path $RepoRoot "src\fx_rates"
$BackendLog = Join-Path $LogFolder "backend-finance-monitor.log"
$BackendErrorLog = Join-Path $LogFolder "backend-finance-monitor.err.log"
$BackendUrl = "http://$HostAddress`:$Port"
$BackendProcess = $null
$BackendStarted = $false

Write-Host ""
Write-Host "Finance Monitor - Local Control Center" -ForegroundColor White
Write-Host "======================================" -ForegroundColor DarkGray
Write-Host ""

try {
    if ($StartBackend -and $SkipBackend) {
        throw "Use either -StartBackend or -SkipBackend, not both."
    }
    if ($DemoMode -and $LiveMode) {
        throw "Use either -DemoMode or -LiveMode, not both."
    }

    if ((Get-Location).Path -ne $RepoRoot) {
        Write-Warn "Switching to repository root: $RepoRoot"
        Set-Location $RepoRoot
    }

    Write-Info "Repository root: $RepoRoot"
    Assert-RequiredPath $PythonPath "Python virtual environment"
    Assert-RequiredPath $FrontendPom "JavaFX Maven project"
    Assert-RequiredPath $LivePipeline "LIVE pipeline script"
    Assert-RequiredPath $FxRatesSource "fx_rates source package"

    if (-not (Test-CommandAvailable "java")) {
        throw "Java is not available on PATH. Install Java 21 or open a terminal with java available."
    }
    if (-not (Test-CommandAvailable "mvn")) {
        throw "Maven is not available on PATH. Install Maven or open a terminal with mvn available."
    }
    Write-Ok "Java command available."
    Write-Ok "Maven command available."

    Write-Info "Checking Python version."
    & $PythonPath --version
    if ($LASTEXITCODE -ne 0) {
        throw "Python version check failed."
    }

    Write-Info "Checking Java version."
    & java -version
    if ($LASTEXITCODE -ne 0) {
        throw "Java version check failed."
    }

    Write-Info "Checking Maven version."
    & mvn -v
    if ($LASTEXITCODE -ne 0) {
        throw "Maven version check failed."
    }

    if ($CheckOnly) {
        Write-Ok "CheckOnly completed. No UI opened, no backend started, no API key requested, no SQLite touched."
        Write-Host "FINANCE MONITOR CHECK STATUS: READY"
        exit 0
    }

    $env:FINANCE_API_BASE_URL = $BackendUrl
    if ($DemoMode) {
        $env:FINANCE_INITIAL_DATA_MODE = "DEMO"
        Write-Info "Initial visual data mode set to DEMO. No demo data will be prepared automatically."
    } elseif ($LiveMode) {
        $env:FINANCE_INITIAL_DATA_MODE = "LIVE"
        Write-Info "Initial visual data mode set to LIVE. No live pipeline will run automatically."
    }

    if ($StartBackend) {
        if (Test-PortInUse $Port) {
            Write-Warn "Port $Port is already in use. The Control Center will connect to the existing local API if compatible."
        } else {
            Write-Info "Starting backend on $BackendUrl."
            if (Test-Path -LiteralPath $BackendLog) {
                Remove-Item -LiteralPath $BackendLog -Force
            }
            if (Test-Path -LiteralPath $BackendErrorLog) {
                Remove-Item -LiteralPath $BackendErrorLog -Force
            }
            $BackendProcess = Start-Process `
                -FilePath $PythonPath `
                -ArgumentList @("-m", "fx_rates", "serve", "--host", $HostAddress, "--port", "$Port") `
                -WorkingDirectory $RepoRoot `
                -WindowStyle Hidden `
                -RedirectStandardOutput $BackendLog `
                -RedirectStandardError $BackendErrorLog `
                -PassThru
            $BackendStarted = $true
            Write-Ok "Backend started. PID: $($BackendProcess.Id)"
            Write-Info "Backend logs: $BackendLog"
        }
    } else {
        Write-Info "Backend startup skipped by default. Use the Control Center to start it when needed."
    }

    Write-Info "Opening JavaFX Control Center."
    Push-Location $FrontendPath
    try {
        & mvn javafx:run
        if ($LASTEXITCODE -ne 0) {
            throw "JavaFX frontend exited with code $LASTEXITCODE."
        }
    } finally {
        Pop-Location
    }
    Write-Ok "Finance Monitor closed."
} catch {
    Write-Fail $_.Exception.Message
    exit 1
} finally {
    if ($BackendStarted) {
        Stop-StartedBackend $BackendProcess
    }
}
