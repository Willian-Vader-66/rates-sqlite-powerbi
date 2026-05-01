$ErrorActionPreference = "Stop"

Write-Host "Finance Dashboard Java Front-End Verification" -ForegroundColor Cyan
Write-Host ""

function Require-Command {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if (-not $command) {
        throw "$Name was not found on PATH. Install Java 21 and Maven 3.9+ before running this script."
    }
}

Require-Command "java"
Require-Command "mvn"

Write-Host "Java version:" -ForegroundColor Yellow
java -version
Write-Host ""

Write-Host "Maven version:" -ForegroundColor Yellow
mvn -v
Write-Host ""

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$frontendDir = Split-Path -Parent $scriptDir
Set-Location $frontendDir

Write-Host "Running mvn clean test..." -ForegroundColor Yellow
mvn clean test
Write-Host ""

Write-Host "Running mvn -q -DskipTests compile..." -ForegroundColor Yellow
mvn -q -DskipTests compile
Write-Host ""

Write-Host "Automated checks completed." -ForegroundColor Green
Write-Host "Manual UI validation is still required. Start the backend, then run:" -ForegroundColor Cyan
Write-Host "  mvn javafx:run"
