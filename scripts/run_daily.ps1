Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot

Push-Location $repoRoot
try {
    python -m fx_rates daily --base USD --symbols BRL,EUR
}
finally {
    Pop-Location
}
