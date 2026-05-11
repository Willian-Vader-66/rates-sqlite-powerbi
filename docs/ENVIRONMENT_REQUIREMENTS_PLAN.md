# Finance Monitor - Environment Requirements Plan

Generated: 2026-05-10

## Project Location

Expected project path:

```powershell
C:\Projetos_Local\rates-sqlite-powerbi
```

Current detected workspace:

```powershell
C:\Projetos_Local\rates-sqlite-powerbi-main
```

Do not invent a repository URL if the expected path is missing. Use this template only after the real repository URL is known:

```powershell
cd C:\Projetos_Local
git clone <URL_DO_REPOSITORIO> rates-sqlite-powerbi
cd rates-sqlite-powerbi
```

## Required Tools

- Git for Windows.
- Python 3.11+ x64. Recommended: Python 3.13 x64 or Python 3.12 x64.
- Python packaging tools: pip, venv, setuptools, wheel.
- Java JDK 21 LTS x64. Recommended distribution: Eclipse Temurin 21.
- Apache Maven 3.9+.
- PowerShell 5.1+ or PowerShell 7+.
- Microsoft Visual C++ Redistributable 2015+ x64, if needed by native dependencies.
- Internet access for pip and Maven dependency downloads.

## Python Requirements

Project files:

- `requirements.txt`
- `pyproject.toml`
- `src/fx_rates`

Expected setup commands from the project root:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip setuptools wheel
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip install -e .
```

Validation commands:

```powershell
.\.venv\Scripts\python.exe -m fx_rates --help
.\.venv\Scripts\python.exe -m fx_rates dashboard --help
.\.venv\Scripts\python.exe -m pytest -q
```

The project uses FastAPI and Uvicorn for the local API. SQLite support is provided by Python's standard library.

## SQLite Data Requirements

Local database path:

```powershell
data\fx.sqlite
```

Do not commit SQLite databases or SQLite sidecar files.

Demo data preparation:

```powershell
.\.venv\Scripts\python.exe -m fx_rates dashboard prepare-demo --years 4 --demo
.\.venv\Scripts\python.exe -m fx_rates dashboard audit
```

The audit should show:

- Total instruments greater than 0.
- Historical rows greater than 0.
- Date range of approximately 4 years.
- Latest quotes greater than 0.
- Analysis snapshots greater than 0.
- Duplicate instruments equal to 0.
- Duplicate quotes equal to 0.
- No critical alerts.

## Backend/API Requirements

Start the API:

```powershell
.\.venv\Scripts\python.exe -m fx_rates serve --host 127.0.0.1 --port 8000
```

Validation commands from another PowerShell:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
Invoke-RestMethod http://127.0.0.1:8000/api/system/status
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/summary
Invoke-RestMethod "http://127.0.0.1:8000/api/dashboard/overview?period=90D"
Invoke-RestMethod "http://127.0.0.1:8000/api/dashboard/fixed-charts?period=90D"
```

Expected API status:

- HTTP 200 responses.
- `db_exists = true`.
- `is_empty = false`.
- `total_instruments > 0`.
- `latest_quote_count > 0`.
- `latest_analysis_count > 0`.
- `historical_row_count > 0`.
- `date_min` and `date_max` consistent with the prepared demo range.

## Java Requirements

Project files:

- `frontend-java\pom.xml`

Required runtime/build tools:

- Java 21.
- Maven 3.9+.
- JavaFX dependencies resolved through Maven.

Expected commands:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi\frontend-java
java -version
mvn -v
Test-Path pom.xml
mvn clean test
mvn -q -DskipTests compile
mvn javafx:run
```

If Maven reports `No plugin found for prefix 'javafx'`, check that `frontend-java\pom.xml` contains the `org.openjfx:javafx-maven-plugin` plugin and retry:

```powershell
mvn clean test
mvn javafx:run
```

## Visual Runner Requirement

Run from the project root:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_visual_test.ps1 -PrepareDemo -SkipTests
```

Expected behavior:

- Prepares demo data.
- Starts the backend.
- Validates API endpoints.
- Opens the JavaFX frontend.
- Stops the backend started by the script after the JavaFX window closes.
- Creates `logs\backend-visual-test.log` and `logs\frontend-visual-test.log`.
- Does not leave orphan processes.

## Optional Power BI/ODBC Requirements

Optional scripts detected in this project:

```powershell
scripts\setup_sqlite_odbc_dsn.ps1
scripts\test_sqlite_odbc_dsn.ps1
```

Power BI integration needs:

- Power BI Desktop 64-bit.
- SQLite ODBC Driver 64-bit.
- DSN `FX_SQLITE` pointing to `data\fx.sqlite`.

Optional commands:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_sqlite_odbc_dsn.ps1
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\test_sqlite_odbc_dsn.ps1
```

ODBC is optional and must not block the local backend/frontend setup.

## Winget Package Discovery

Before installing, search package IDs:

```powershell
winget search Git --source winget
winget search Python --source winget
winget search Temurin --source winget
winget search Maven --source winget
winget search "Visual C++" --source winget
```

Suggested installation commands, subject to `winget search` confirmation:

```powershell
winget install --id Git.Git -e
winget install --id Python.Python.3.13 -e
winget install --id EclipseAdoptium.Temurin.21.JDK -e
winget install --id Microsoft.VCRedist.2015+.x64 -e
```

Current discovery result on this machine:

- `Git.Git` found.
- `Python.Python.3.13` found.
- `EclipseAdoptium.Temurin.21.JDK` found.
- `Microsoft.VCRedist.2015+.x64` found.
- `Apache.Maven` was not found in the `winget` source.

Maven fallback if no winget package is available:

1. Download the Apache Maven binary ZIP from the official Apache Maven website.
2. Extract it to a stable folder, for example `C:\Tools\apache-maven-3.9.x`.
3. Set `MAVEN_HOME` to that folder.
4. Add `%MAVEN_HOME%\bin` to PATH.
5. Reopen PowerShell and validate with `mvn -v`.

Some installers may request administrator permission. After installation, close and reopen PowerShell if PATH does not refresh.

## Java and Maven Environment Variables

If Java is installed but `JAVA_HOME` is empty, locate the JDK 21 folder and configure it. Example:

```powershell
[Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Eclipse Adoptium\jdk-21.x.x.x-hotspot", "User")
```

If Maven is installed but `mvn` does not work, locate the Maven folder and configure it. Example:

```powershell
[Environment]::SetEnvironmentVariable("MAVEN_HOME", "C:\Program Files\Apache\Maven\apache-maven-3.9.x", "User")
```

Then add both bins to the user PATH if missing:

```powershell
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
[Environment]::SetEnvironmentVariable("Path", "$userPath;%JAVA_HOME%\bin;%MAVEN_HOME%\bin", "User")
```

Open a new PowerShell and validate:

```powershell
git --version
python --version
py --version
java -version
mvn -v
```

## Files That Must Not Be Versioned

- `.venv\`
- `data\*.sqlite`
- `data\*.sqlite-*`
- `logs\`
- `cache\`
- `frontend-java\target\`
- `__pycache__\`
- `.pytest_cache\`
- temporary files
