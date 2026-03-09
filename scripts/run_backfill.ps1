Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$endDate = Get-Date
$startDate = $endDate.AddDays(-90)

Push-Location $repoRoot
try {
    python -m fx_rates backfill `
        --start $startDate.ToString("yyyy-MM-dd") `
        --end $endDate.ToString("yyyy-MM-dd") `
        --base USD `
        --symbols BRL,EUR
}
finally {
    Pop-Location
}
