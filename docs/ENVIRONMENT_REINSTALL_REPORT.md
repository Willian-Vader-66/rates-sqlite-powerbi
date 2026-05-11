# Finance Monitor - Environment Reinstall Report

Generated: 2026-05-10 18:43:00 -03:00

## Final Status

ENVIRONMENT SETUP STATUS: NOT READY

The project is now in the expected path and the Python/backend flow works locally. The environment is still not fully ready because this folder does not contain `.git` and Maven cannot download dependencies from Maven Central due a Java TLS/PKIX trust failure.

## Project Path Status

Project path:

```powershell
C:\Projetos_Local\rates-sqlite-powerbi
```

Result:

- Project path exists: yes
- `.git` exists: no
- Python backend files exist: yes
- Java frontend files exist: yes
- `run_visual_test.ps1` exists: yes
- `.venv` exists: yes
- `data/fx.sqlite` exists: yes

GIT STATUS: NOT READY

The folder has the code, but it is not a Git repository. It is not safe to commit or push here. Restore the original `.git` folder or clone the correct repository into `C:\Projetos_Local\rates-sqlite-powerbi` using the real repository URL.

Do not run `git init` automatically for this folder.

## Toolchain Status

Git:

- available: yes
- version: `git version 2.54.0.windows.1`
- action: tool works, but project has no `.git`

Python:

- available: yes
- version: `Python 3.13.13`
- action: OK

Py launcher:

- available: command exists, but not functional
- result: `No installed Python found!`
- action: optional repair/reinstall Python launcher; not blocking because `python` works

Java:

- available: yes
- version: `openjdk version "21.0.11" 2026-04-21 LTS`
- JAVA_HOME: `C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot`
- action: Java 21 OK

Maven:

- available: yes
- version: `Apache Maven 3.9.15`
- MAVEN_HOME: `C:\Tools\apache-maven-3.9.15`
- action: Maven starts, but dependency download is blocked by TLS/PKIX certificate validation

Visual C++ Redistributable:

- available: yes, detected previously by winget as `Microsoft.VCRedist.2015+.x64`

## Java/Maven/PATH

Detected paths:

- Maven folder: `C:\Tools\apache-maven-3.9.15` exists
- Maven command: `C:\Tools\apache-maven-3.9.15\bin\mvn.cmd` exists
- JDK folder: `C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot` exists

Current PATH includes:

- `%JAVA_HOME%\bin`
- `%MAVEN_HOME%\bin`
- Git
- Python 3.13

No Java/Maven PATH correction was needed after the rename.

## Python Setup

Existing `.venv` was reused, not recreated.

Validation:

```powershell
.\.venv\Scripts\python.exe --version
```

Result:

- `Python 3.13.13`

Dependency maintenance performed:

```powershell
.\.venv\Scripts\python.exe -m pip install --upgrade pip setuptools wheel
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip install -e .
```

Notes:

- `setuptools` and `wheel` needed an elevated retry after a local access denied error.
- `pip install -e .` needed an elevated retry because the editable egg-info timestamp could not be updated after the rename.
- Editable install now points to `C:\Projetos_Local\rates-sqlite-powerbi`.

CLI validation:

- `python -m fx_rates --help`: OK
- `python -m fx_rates dashboard --help`: OK

Python test result:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

- `35 passed in 9.78s`

Corrections applied during this recheck:

- `tests\test_cli_smoke.py`: status smoke now writes logs to pytest `tmp_path` instead of repo `logs/app.log`.
- `src\fx_rates\logging_setup.py`: if file logging cannot open the configured log file, CLI falls back to console logging with a warning instead of crashing.
- ACL granted Modify to the current user for `data\fx.sqlite` and `logs\app.log` to recover from local permission issues after earlier elevated/sandbox runs.

## SQLite Status

Database:

```powershell
data\fx.sqlite
```

Result:

- exists: yes
- size: approximately 13.5 MB
- no manual deletion performed
- no manual database editing performed
- `prepare-demo` was not rerun during the audit phase because existing data was valid
- `run_visual_test.ps1 -PrepareDemo` later refreshed demo data through the official project command

Audit command:

```powershell
.\.venv\Scripts\python.exe -m fx_rates dashboard audit
```

Audit result:

- Total instruments: 68
- Historical rows: 83638
- Overall date range: `2022-04-30` to `2026-05-10`
- Latest quotes: CRYPTO=10, FX=19, MACRO=7, STOCK=32
- Analysis snapshots: CRYPTO=10, FX=19, MACRO=7, STOCK=32
- Duplicate instruments: 0
- Duplicate quotes: 0
- Suspicious values: 0
- Alerts: none

## Backend/API Smoke

Backend started temporarily with:

```powershell
.\.venv\Scripts\python.exe -m fx_rates serve --host 127.0.0.1 --port 8000
```

Validated endpoints:

- `/health`: OK
- `/api/system/status`: OK
- `/api/dashboard/summary`: OK
- `/api/instruments`: OK
- `/api/quotes/latest`: OK
- `/api/analysis/latest`: OK
- `/api/dashboard/overview?period=90D`: OK
- `/api/dashboard/fixed-charts?period=90D`: OK

Smoke result:

- `db_exists`: true
- `is_empty`: false
- `total_instruments`: 68
- `latest_quote_count`: 68
- `latest_analysis_count`: 68
- `historical_row_count`: 83638
- `date_min`: `2022-04-30`
- `date_max`: `2026-05-10`
- `overview_period`: `90D`
- fixed charts counts: FX=2, CRYPTO=2, MACRO=1
- backend process exited: yes

## Frontend JavaFX / Maven

Frontend path:

```powershell
C:\Projetos_Local\rates-sqlite-powerbi\frontend-java
```

Commands attempted:

```powershell
mvn clean test
mvn -q -DskipTests compile
```

Result:

- `mvn clean test`: failed
- `mvn -q -DskipTests compile`: failed

Failure:

```text
PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target
```

Maven can start, but Java cannot validate the certificate chain for `https://repo.maven.apache.org/maven2`. This blocks Maven plugin/dependency downloads from Maven Central.

Additional network/TLS check:

```powershell
curl.exe -I https://repo.maven.apache.org/maven2/
```

Result:

```text
schannel: AcquireCredentialsHandle failed: SEC_E_NO_CREDENTIALS
```

JavaFX UI was not launched because Maven dependency resolution is not healthy yet.

## Visual Runner

Command:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_visual_test.ps1 -PrepareDemo -SkipTests -NoFrontend
```

Result:

- Maven version check: OK
- Java version check: OK
- demo data prepared through official command: OK
- backend started: OK
- `/api/system/status` validated: OK
- total instruments: 68
- backend stopped by script: OK
- status: PASS without frontend

The full frontend runner was not executed because Maven build/dependency resolution currently fails with PKIX.

## Commit Readiness

COMMIT STATUS: NOT READY

Reasons:

- `.git` does not exist in `C:\Projetos_Local\rates-sqlite-powerbi`.
- `git status --short`, branch, and remote cannot be reviewed.
- Maven tests/compile are blocked by certificate trust failure.

It is not safe to commit or push from this folder yet.

When `.git` is restored and Maven TLS is fixed, review these paths before committing:

PODE COMMITAR:

- `docs/*.md`
- `src/`
- `tests/`
- `frontend-java/src/`
- `frontend-java/pom.xml`
- `README.md`
- `run_visual_test.ps1`
- `data/reference/`

NAO COMMITAR:

- `.venv/`
- `data/*.sqlite`
- `data/*.sqlite-*`
- `logs/`
- `cache/`
- `frontend-java/target/`
- `__pycache__/`
- `.pytest_cache/`
- temporary files

## Exact Next Steps

1. Restore Git metadata or clone the real repository.

If cloning is needed, use the real repository URL:

```powershell
cd C:\Projetos_Local
git clone <URL_DO_REPOSITORIO> rates-sqlite-powerbi
cd C:\Projetos_Local\rates-sqlite-powerbi
```

Do not invent the URL and do not run `git init` as a substitute for the real repository history.

2. Fix Maven Central TLS trust for Java/Maven.

First test:

```powershell
curl.exe -I https://repo.maven.apache.org/maven2/
mvn -U clean test
```

If the network uses a corporate proxy, antivirus HTTPS inspection, or a custom root certificate, import the correct root CA into the JDK truststore. Example pattern, using the real certificate path:

```powershell
& "$env:JAVA_HOME\bin\keytool.exe" -importcert -trustcacerts -alias local-root-ca -file C:\path\to\root-ca.cer -keystore "$env:JAVA_HOME\lib\security\cacerts" -storepass changeit
```

This may require administrator permission depending on JDK installation permissions.

3. After Maven TLS is fixed, run:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi\frontend-java
mvn -U clean test
mvn -q -DskipTests compile
```

4. Then run the full visual runner if frontend launch is desired:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_visual_test.ps1 -PrepareDemo -SkipTests
```

5. After `.git` is restored, review:

```powershell
git status --short
git branch --show-current
git remote -v
```