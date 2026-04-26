param(
  [string]$DsnName = "FX_SQLITE"
)

$ErrorActionPreference = "Stop"

function Get-RepoRoot {
  return (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
}

function Test-NameMatchesAnyPattern {
  param(
    [Parameter(Mandatory = $true)][string]$Name,
    [Parameter(Mandatory = $true)][string[]]$Patterns
  )

  foreach ($pattern in $Patterns) {
    if ($Name -like $pattern) {
      return $true
    }
  }

  return $false
}

function Get-SqliteOdbcDriverCandidates {
  param([string[]]$Patterns)

  $drivers = @()

  if (Get-Command Get-OdbcDriver -ErrorAction SilentlyContinue) {
    foreach ($platform in @("64-bit", "32-bit")) {
      try {
        $drivers += Get-OdbcDriver -Name $Patterns -Platform $platform -ErrorAction SilentlyContinue |
          ForEach-Object {
            [pscustomobject]@{
              Name = $_.Name
              Platform = $platform
            }
          }
      } catch {
        # Some older Windows builds do not support every Get-OdbcDriver parameter combination.
      }
    }

    if (-not $drivers) {
      try {
        $drivers += Get-OdbcDriver -ErrorAction SilentlyContinue |
          Where-Object { Test-NameMatchesAnyPattern -Name $_.Name -Patterns $Patterns } |
          ForEach-Object {
            [pscustomobject]@{
              Name = $_.Name
              Platform = if ($_.Platform) { $_.Platform } else { "64-bit" }
            }
          }
      } catch {
        # Registry fallback below handles machines without working ODBC cmdlets.
      }
    }
  }

  $registryLocations = @(
    @{ Path = "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers"; Platform = "64-bit" },
    @{ Path = "HKLM:\SOFTWARE\WOW6432Node\ODBC\ODBCINST.INI\ODBC Drivers"; Platform = "32-bit" }
  )

  foreach ($location in $registryLocations) {
    if (Test-Path -LiteralPath $location.Path) {
      $properties = Get-ItemProperty -LiteralPath $location.Path
      foreach ($property in $properties.PSObject.Properties) {
        if ($property.Name -like "PS*" -or $property.Value -ne "Installed") {
          continue
        }

        if (Test-NameMatchesAnyPattern -Name $property.Name -Patterns $Patterns) {
          $drivers += [pscustomobject]@{
            Name = $property.Name
            Platform = $location.Platform
          }
        }
      }
    }
  }

  $unique = @{}
  foreach ($driver in $drivers) {
    if (-not $driver.Name) {
      continue
    }

    $key = "{0}|{1}" -f $driver.Platform, $driver.Name
    $unique[$key] = $driver
  }

  return $unique.Values |
    Sort-Object `
      @{ Expression = { if ($_.Platform -eq "64-bit") { 0 } else { 1 } } }, `
      @{ Expression = { if ($_.Name -like "SQLite3*") { 0 } elseif ($_.Name -like "SQLite*") { 1 } else { 2 } } }, `
      Name
}

function Remove-ExistingUserDsn {
  param(
    [Parameter(Mandatory = $true)][string]$Name,
    [Parameter(Mandatory = $true)][string]$Platform
  )

  $existingDsn = Get-OdbcDsn -Name $Name -DsnType User -Platform $Platform -ErrorAction SilentlyContinue
  if ($existingDsn) {
    Write-Host "Existing USER DSN '$Name' found for $Platform. Recreating it with the project database path..."
    Remove-OdbcDsn -Name $Name -DsnType User -Platform $Platform -ErrorAction Stop
  }
}

function Add-ProjectUserDsn {
  param(
    [Parameter(Mandatory = $true)][string]$Name,
    [Parameter(Mandatory = $true)][string]$DriverName,
    [Parameter(Mandatory = $true)][string]$Platform,
    [Parameter(Mandatory = $true)][string[]]$Properties
  )

  Add-OdbcDsn `
    -Name $Name `
    -DriverName $DriverName `
    -DsnType User `
    -Platform $Platform `
    -SetPropertyValue $Properties `
    -ErrorAction Stop
}

try {
  $repoRoot = Get-RepoRoot
  $databasePath = Join-Path $repoRoot "data\fx.sqlite"
  $databasePath = [System.IO.Path]::GetFullPath($databasePath)

  if (-not (Test-Path -LiteralPath $databasePath -PathType Leaf)) {
    Write-Host "SQLite database was not found at:" -ForegroundColor Red
    Write-Host "  $databasePath"
    Write-Host ""
    Write-Host "Create it first by running this command from the repository root:"
    Write-Host "  python -m fx_rates backfill --start 2026-02-01 --end 2026-02-03 --base USD --symbols BRL,EUR"
    exit 1
  }

  foreach ($requiredCommand in @("Add-OdbcDsn", "Get-OdbcDsn", "Remove-OdbcDsn")) {
    if (-not (Get-Command $requiredCommand -ErrorAction SilentlyContinue)) {
      throw "PowerShell ODBC cmdlet '$requiredCommand' is not available. Run this from Windows PowerShell on a Windows machine with ODBC PowerShell cmdlets installed."
    }
  }

  $driverPatterns = @("SQLite*", "*SQLite*", "SQLite3*", "*SQLite3*")
  $driver = Get-SqliteOdbcDriverCandidates -Patterns $driverPatterns | Select-Object -First 1

  if (-not $driver) {
    Write-Host "No SQLite ODBC driver was detected." -ForegroundColor Red
    Write-Host "Install a 64-bit SQLite ODBC driver, then rerun this script."
    Write-Host "Power BI Desktop is usually 64-bit, so the SQLite ODBC driver should also be 64-bit."
    exit 1
  }

  Write-Host "Detected SQLite ODBC driver:"
  Write-Host "  Driver:   $($driver.Name)"
  Write-Host "  Platform: $($driver.Platform)"

  if ($driver.Platform -ne "64-bit") {
    Write-Host "Warning: using a $($driver.Platform) driver. 64-bit Power BI Desktop usually requires a 64-bit ODBC driver." -ForegroundColor Yellow
  }

  $requiredProperties = @("Database=$databasePath")
  $optionalProperties = @(
    "Timeout=1000",
    "LongNames=0",
    "NoTXN=0",
    "StepAPI=0"
  )
  $propertiesWithOptions = $requiredProperties + $optionalProperties
  $usedOptionalProperties = $true

  Remove-ExistingUserDsn -Name $DsnName -Platform $driver.Platform

  try {
    Add-ProjectUserDsn `
      -Name $DsnName `
      -DriverName $driver.Name `
      -Platform $driver.Platform `
      -Properties $propertiesWithOptions
  } catch {
    Write-Host "The driver rejected one or more optional SQLite properties. Retrying with Database only..." -ForegroundColor Yellow
    Remove-ExistingUserDsn -Name $DsnName -Platform $driver.Platform
    Add-ProjectUserDsn `
      -Name $DsnName `
      -DriverName $driver.Name `
      -Platform $driver.Platform `
      -Properties $requiredProperties
    $usedOptionalProperties = $false
  }

  Write-Host ""
  Write-Host "SQLite ODBC USER DSN configured successfully." -ForegroundColor Green
  Write-Host "  DSN name:      $DsnName"
  Write-Host "  Driver name:   $($driver.Name)"
  Write-Host "  Driver bitness: $($driver.Platform)"
  Write-Host "  Database path: $databasePath"
  if ($usedOptionalProperties) {
    Write-Host "  Optional properties: Timeout=1000, LongNames=0, NoTXN=0, StepAPI=0"
  } else {
    Write-Host "  Optional properties: skipped because this driver did not accept them"
  }

  exit 0
} catch {
  Write-Host "ODBC DSN setup failed: $($_.Exception.Message)" -ForegroundColor Red
  exit 1
}
