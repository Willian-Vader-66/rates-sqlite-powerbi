param(
  [string]$DsnName = "FX_SQLITE"
)

$ErrorActionPreference = "Stop"

function Write-EnvironmentDiagnostics {
  param([Parameter(Mandatory = $true)][string]$Name)

  $edition = $PSVersionTable.PSEdition
  if (-not $edition) {
    $edition = "Desktop"
  }

  Write-Host "ODBC DSN validation diagnostics:"
  Write-Host "  PowerShell version: $($PSVersionTable.PSVersion)"
  Write-Host "  PowerShell edition: $edition"
  Write-Host "  Is64BitProcess: $([Environment]::Is64BitProcess)"
  Write-Host "  DSN name: $Name"
  Write-Host ""
}

function Test-UserDsnExists {
  param([Parameter(Mandatory = $true)][string]$Name)

  if (-not (Get-Command Get-OdbcDsn -ErrorAction SilentlyContinue)) {
    Write-Host "Warning: Get-OdbcDsn is not available in this PowerShell host. Continuing with the connection test." -ForegroundColor Yellow
    return $true
  }

  $dsn = Get-OdbcDsn -Name $Name -DsnType User -ErrorAction SilentlyContinue

  if (-not $dsn) {
    foreach ($platform in @("64-bit", "32-bit")) {
      try {
        $dsn = Get-OdbcDsn -Name $Name -DsnType User -Platform $platform -ErrorAction SilentlyContinue
        if ($dsn) {
          break
        }
      } catch {
        # Some hosts do not support the Platform parameter. The plain lookup above is the primary path.
      }
    }
  }

  if (-not $dsn) {
    Write-Host "ODBC USER DSN '$Name' was not found." -ForegroundColor Red
    Write-Host "Run:"
    Write-Host "  powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_sqlite_odbc_dsn.ps1"
    return $false
  }

  Write-Host "ODBC USER DSN found:"
  Write-Host "  Name: $($dsn.Name)"
  if ($dsn.DriverName) {
    Write-Host "  Driver: $($dsn.DriverName)"
  }
  Write-Host ""

  return $true
}

function Initialize-OdbcTypes {
  try {
    Add-Type -AssemblyName System.Data -ErrorAction Stop
  } catch {
    Write-Host "System.Data / ODBC type cannot be loaded: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host ""
    Write-Host "System.Data.Odbc could not be loaded in this PowerShell host. Try running with Windows PowerShell 5.1:"
    Write-Host "powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\test_sqlite_odbc_dsn.ps1"
    return $false
  }

  if (-not ([System.Management.Automation.PSTypeName]"System.Data.Odbc.OdbcConnection").Type) {
    Write-Host "System.Data / ODBC type cannot be loaded: System.Data.Odbc.OdbcConnection is unavailable." -ForegroundColor Red
    Write-Host ""
    Write-Host "System.Data.Odbc could not be loaded in this PowerShell host. Try running with Windows PowerShell 5.1:"
    Write-Host "powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\test_sqlite_odbc_dsn.ps1"
    return $false
  }

  return $true
}

function Invoke-OdbcScalar {
  param(
    [Parameter(Mandatory = $true)]$Connection,
    [Parameter(Mandatory = $true)][string]$Query,
    [Parameter(Mandatory = $true)][string]$TableName
  )

  $command = $Connection.CreateCommand()
  $command.CommandText = $Query

  try {
    return $command.ExecuteScalar()
  } catch {
    throw "Query failed for table '$TableName'. The table may not exist, or the DSN may point to the wrong database. Underlying error: $($_.Exception.Message)"
  } finally {
    $command.Dispose()
  }
}

Write-EnvironmentDiagnostics -Name $DsnName

if (-not (Test-UserDsnExists -Name $DsnName)) {
  exit 1
}

if (-not (Initialize-OdbcTypes)) {
  exit 1
}

$connection = $null

try {
  $connectionString = "DSN=$DsnName;"
  $connection = New-Object -TypeName System.Data.Odbc.OdbcConnection -ArgumentList $connectionString

  try {
    $connection.Open()
  } catch {
    throw "Could not open ODBC connection using '$connectionString'. Confirm that the DSN exists, uses a 64-bit SQLite ODBC driver when running 64-bit PowerShell or Power BI, and points to data\fx.sqlite. Underlying error: $($_.Exception.Message)"
  }

  $fxRatesCount = Invoke-OdbcScalar -Connection $connection -Query "SELECT COUNT(*) FROM fx_rates;" -TableName "fx_rates"
  $ingestRunsCount = Invoke-OdbcScalar -Connection $connection -Query "SELECT COUNT(*) FROM ingest_runs;" -TableName "ingest_runs"

  Write-Host "ODBC DSN validation succeeded." -ForegroundColor Green
  Write-Host "fx_rates rows: $fxRatesCount"
  Write-Host "ingest_runs rows: $ingestRunsCount"

  exit 0
} catch {
  Write-Host "ODBC DSN validation failed: $($_.Exception.Message)" -ForegroundColor Red
  exit 1
} finally {
  if ($connection) {
    $connection.Close()
    $connection.Dispose()
  }
}
