param(
  [string]$Start = (Get-Date).AddDays(-90).ToString("yyyy-MM-dd"),
  [string]$End = (Get-Date).ToString("yyyy-MM-dd"),
  [string]$Base = "USD",
  [string]$Symbols = "BRL,EUR",
  [string]$DbPath = "data/fx.sqlite",
  [string]$CacheDir = "cache",
  [string]$LogFile = "logs/app.log",
  [string]$LogLevel = "INFO",
  [int]$Timeout = 20,
  [int]$Retries = 3,
  [switch]$NoCache
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"

$argsList = @(
  "-m", "fx_rates", "backfill",
  "--start", $Start,
  "--end", $End,
  "--base", $Base,
  "--symbols", $Symbols,
  "--db-path", $DbPath,
  "--cache-dir", $CacheDir,
  "--log-file", $LogFile,
  "--log-level", $LogLevel,
  "--timeout", $Timeout.ToString(),
  "--retries", $Retries.ToString()
)

if ($NoCache) {
  $argsList += "--no-cache"
}

Push-Location $repoRoot
try {
  if (Test-Path $venvPython) {
    & $venvPython @argsList
  } elseif (Get-Command python -ErrorAction SilentlyContinue) {
    python @argsList
  } elseif (Get-Command py -ErrorAction SilentlyContinue) {
    py -3 @argsList
  } else {
    throw "Nenhum interpretador Python encontrado. Use .venv ou instale python/py no PATH."
  }
} finally {
  Pop-Location
}
