param(
    [switch]$PrepareDemo,
    [switch]$SkipTests,
    [int]$Port = 8000,
    [string]$HostAddress = "127.0.0.1",
    [switch]$KeepBackendAlive,
    [switch]$NoFrontend
)

$ErrorActionPreference = "Stop"

function Write-Info($Message) {
    Write-Host "[INFO] $Message" -ForegroundColor Cyan
}

function Write-Ok($Message) {
    Write-Host "[OK] $Message" -ForegroundColor Green
}

function Write-Warn($Message) {
    Write-Host "[WARN] $Message" -ForegroundColor Yellow
}

function Write-Err($Message) {
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

function Write-VisualLog($LogPath, $Message) {
    if ($LogPath) {
        Add-Content -LiteralPath $LogPath -Value ("[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message)
    }
}

function Invoke-CheckedCommand($Label, $Command, $Arguments, $WorkingDirectory, $LogPath) {
    Write-Info $Label
    Push-Location $WorkingDirectory
    try {
        if ($LogPath) {
            & $Command @Arguments 2>&1 | Tee-Object -FilePath $LogPath
        } else {
            & $Command @Arguments
        }
        if ($LASTEXITCODE -ne 0) {
            throw "$Label failed with exit code $LASTEXITCODE"
        }
    } finally {
        Pop-Location
    }
    Write-Ok "$Label completed."
}

function Test-CommandAvailable($CommandName) {
    $command = Get-Command $CommandName -ErrorAction SilentlyContinue
    return $null -ne $command
}

function Test-PortInUse($PortNumber) {
    try {
        $connection = Get-NetTCPConnection -LocalPort $PortNumber -State Listen -ErrorAction SilentlyContinue
        return $null -ne $connection
    } catch {
        return $false
    }
}

function Stop-StartedProcess($Process, $Name) {
    if ($null -eq $Process) {
        return
    }
    try {
        $current = Get-Process -Id $Process.Id -ErrorAction SilentlyContinue
        if ($null -eq $current) {
            return
        }
        Write-Info "Stopping $Name process PID $($Process.Id)."
        Write-VisualLog $script:VisualTestBackendLog "Stopping $Name process PID $($Process.Id)."
        Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
        Write-Ok "$Name process stopped."
    } catch {
        Write-Warn "Could not stop $Name process PID $($Process.Id): $($_.Exception.Message)"
    }
}

function Start-BackendProcess($PythonPath, $RepoRoot, $HostAddress, $Port, $BackendLog) {
    if (Test-Path $BackendLog) {
        Remove-Item -LiteralPath $BackendLog -Force
    }
    New-Item -ItemType File -Path $BackendLog -Force | Out-Null
    $script:VisualTestBackendLog = $BackendLog

    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = $PythonPath
    $startInfo.Arguments = "-m fx_rates serve --host $HostAddress --port $Port"
    $startInfo.WorkingDirectory = $RepoRoot
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $false
    $startInfo.RedirectStandardError = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.EnvironmentVariables["PYTHONUNBUFFERED"] = "1"

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $startInfo
    $process.EnableRaisingEvents = $true

    [void]$process.Start()
    Write-VisualLog $BackendLog "Started backend command: $PythonPath $($startInfo.Arguments)"
    Write-VisualLog $BackendLog "Backend PID: $($process.Id)"
    return $process
}

function Wait-ForBackend($StatusUrl, $TimeoutSeconds) {
    Write-Info "Waiting for backend health..."
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $lastError = $null
    while ((Get-Date) -lt $deadline) {
        try {
            $status = Invoke-RestMethod -Uri $StatusUrl -TimeoutSec 5
            if ($status.db_exists -ne $true) {
                throw "Backend responded, but SQLite database does not exist."
            }
            if ($status.is_empty -eq $true -or [int]$status.total_instruments -le 0) {
                Write-Err "Backend is running, but dashboard database is empty."
                Write-Host "Run:"
                Write-Host ".\run_visual_test.ps1 -PrepareDemo"
                Write-Host ""
                Write-Host "Or:"
                Write-Host ".\.venv\Scripts\python.exe -m fx_rates dashboard prepare-demo --years 4 --demo"
                throw "Dashboard database is empty."
            }
            return $status
        } catch {
            $lastError = $_.Exception.Message
            Start-Sleep -Seconds 1
        }
    }
    throw "Backend did not become ready within $TimeoutSeconds seconds. Last error: $lastError"
}

function Show-ApiSummary($Status) {
    Write-Ok "Backend is ready."
    Write-Ok "Dashboard data loaded: $($Status.total_instruments) instruments."
    Write-Host ""
    Write-Host "API summary:"
    Write-Host "  db_path: $($Status.db_path)"
    Write-Host "  db_size_bytes: $($Status.db_size_bytes)"
    Write-Host "  total_instruments: $($Status.total_instruments)"
    Write-Host "  active_stocks: $($Status.active_stocks)"
    Write-Host "  active_currencies: $($Status.active_currencies)"
    Write-Host "  active_crypto: $($Status.active_crypto)"
    Write-Host "  active_macro: $($Status.active_macro)"
    Write-Host "  latest_quote_count: $($Status.latest_quote_count)"
    Write-Host "  latest_analysis_count: $($Status.latest_analysis_count)"
    Write-Host "  date_min: $($Status.date_min)"
    Write-Host "  date_max: $($Status.date_max)"
    Write-Host ""
    Write-VisualLog $script:VisualTestBackendLog "Backend ready. Dashboard data loaded: $($Status.total_instruments) instruments."
    Write-VisualLog $script:VisualTestBackendLog "DB path: $($Status.db_path)"
    Write-VisualLog $script:VisualTestBackendLog "Quotes: $($Status.latest_quote_count); Analysis: $($Status.latest_analysis_count); Date range: $($Status.date_min) to $($Status.date_max)"
}

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = (Get-Location).Path
}
$RepoRoot = (Resolve-Path $RepoRoot).Path
$PythonPath = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$FrontendPath = Join-Path $RepoRoot "frontend-java"
$FrontendPom = Join-Path $FrontendPath "pom.xml"
$LogFolder = Join-Path $RepoRoot "logs"
$BackendLog = Join-Path $LogFolder "backend-visual-test.log"
$FrontendLog = Join-Path $LogFolder "frontend-visual-test.log"
$BackendUrl = "http://$HostAddress`:$Port"
$StatusUrl = "$BackendUrl/api/system/status"
$BackendProcess = $null
$BackendStarted = $false

Write-Host ""
Write-Host "Finance Monitor - Local Visual Test Runner" -ForegroundColor White
Write-Host "==========================================" -ForegroundColor DarkGray
Write-Host ""

try {
    if ((Get-Location).Path -ne $RepoRoot) {
        Write-Warn "This script is intended to run from the repository root."
        Write-Warn "Current path: $(Get-Location)"
        Write-Warn "Switching to: $RepoRoot"
        Set-Location $RepoRoot
    }

    if (-not (Test-Path $PythonPath)) {
        Write-Err "Python virtual environment not found: $PythonPath"
        Write-Err "Create/install the local .venv before running visual tests."
        exit 1
    }
    if (-not (Test-Path $FrontendPom)) {
        Write-Err "Frontend Maven project not found: $FrontendPom"
        exit 1
    }
    if (-not (Test-CommandAvailable "mvn")) {
        Write-Err "Maven is not available on PATH. Install Maven or open a terminal with mvn available."
        exit 1
    }
    if (-not (Test-CommandAvailable "java")) {
        Write-Err "Java is not available on PATH. Install Java 21 or open a terminal with java available."
        exit 1
    }

    if (-not (Test-Path $LogFolder)) {
        New-Item -ItemType Directory -Path $LogFolder | Out-Null
    }

    Write-Host "Detected paths:"
    Write-Host "  Repo root: $RepoRoot"
    Write-Host "  Python path: $PythonPath"
    Write-Host "  Frontend path: $FrontendPath"
    Write-Host "  Backend URL: $BackendUrl"
    Write-Host "  Log folder: $LogFolder"
    Write-Host ""

    Write-Info "Checking Maven..."
    & mvn -v
    if ($LASTEXITCODE -ne 0) {
        throw "mvn -v failed."
    }
    Write-Info "Checking Java..."
    $javaVersionOutput = & cmd.exe /c "java -version 2>&1"
    $javaVersionExitCode = $LASTEXITCODE
    $javaVersionOutput | ForEach-Object { Write-Host $_ }
    if ($javaVersionExitCode -ne 0) {
        throw "java -version failed."
    }

    if (-not $SkipTests) {
        Invoke-CheckedCommand "Running backend tests" $PythonPath @("-m", "pytest", "-q") $RepoRoot $null
        Invoke-CheckedCommand "Running frontend tests" "mvn" @("clean", "test") $FrontendPath $null
        Invoke-CheckedCommand "Compiling frontend" "mvn" @("-q", "-DskipTests", "compile") $FrontendPath $null
    } else {
        Write-Warn "Skipping Python and Java tests because -SkipTests was provided."
    }

    if ($PrepareDemo) {
        Invoke-CheckedCommand "Preparing demo dashboard data" $PythonPath @("-m", "fx_rates", "dashboard", "prepare-demo", "--years", "4", "--demo") $RepoRoot $null
    }

    if (Test-PortInUse $Port) {
        Write-Err "Port $Port is already in use. Stop the existing server or run with -Port <another port>."
        exit 1
    }

    Write-Info "Starting backend on $BackendUrl"
    $BackendProcess = Start-BackendProcess $PythonPath $RepoRoot $HostAddress $Port $BackendLog
    $BackendStarted = $true
    Write-Info "Backend PID: $($BackendProcess.Id)"
    Write-Info "Backend log: $BackendLog"

    $status = Wait-ForBackend $StatusUrl 60
    Show-ApiSummary $status

    if ($NoFrontend) {
        Write-Info "NoFrontend was provided; frontend launch skipped."
    } else {
        Write-Info "Starting JavaFX frontend..."
        Write-Info "Frontend log: $FrontendLog"
        if (Test-Path $FrontendLog) {
            Remove-Item -LiteralPath $FrontendLog -Force
        }
        Push-Location $FrontendPath
        try {
            & mvn javafx:run 2>&1 | Tee-Object -FilePath $FrontendLog
            if ($LASTEXITCODE -ne 0) {
                throw "JavaFX frontend exited with code $LASTEXITCODE"
            }
        } finally {
            Pop-Location
        }
        Write-Info "Frontend closed."
    }

    Write-Ok "Done."
} catch {
    Write-Err $_.Exception.Message
    Write-Err "Backend log: $BackendLog"
    Write-Err "Frontend log: $FrontendLog"
    exit 1
} finally {
    if ($BackendStarted -and -not $KeepBackendAlive) {
        Stop-StartedProcess $BackendProcess "backend"
    } elseif ($BackendStarted -and $KeepBackendAlive) {
        Write-Warn "Keeping backend alive because -KeepBackendAlive was provided. PID: $($BackendProcess.Id)"
        Write-Warn "Stop it manually when finished."
    }
}
