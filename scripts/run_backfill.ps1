param(
  [string]$Start = "2025-11-01",
  [string]$End = "2026-02-10",
  [string]$Base = "USD",
  [string]$Symbols = "BRL,EUR"
)

$ErrorActionPreference = "Stop"
$env:PYTHONPATH = "src"
python -m fx_rates backfill --start $Start --end $End --base $Base --symbols $Symbols
