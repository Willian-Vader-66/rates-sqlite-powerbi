param(
  [string]$Base = "USD",
  [string]$Symbols = "BRL,EUR"
)

$ErrorActionPreference = "Stop"
$env:PYTHONPATH = "src"
python -m fx_rates daily --base $Base --symbols $Symbols
