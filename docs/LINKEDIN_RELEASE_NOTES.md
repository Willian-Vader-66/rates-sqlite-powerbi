# LinkedIn Release Notes

## Project name

Finance Monitor - Local Financial Dashboard

## Short summary

Local financial dashboard with a Python/FastAPI backend, SQLite storage, data audit commands, and a JavaFX desktop frontend. The project uses explicit demo data by default and includes architecture prepared for future live ingestion.

## Stack

- Python
- FastAPI
- SQLite
- JavaFX
- Maven
- Power BI/ODBC initial path
- REST API
- Data validation
- Market data providers

## What the project demonstrates

- Market-data ingestion workflows
- Local persistence with SQLite
- REST API design for a desktop frontend
- JavaFX dashboard integration
- Explicit separation between demo, live, mixed, and unknown data modes
- Data consistency audits for quote/history/analysis alignment
- Safety checks against silently mixing demo and live data
- Technical documentation and release checklists
- Automated Python and Java validation flows

## Current scope

- Demo data is deterministic, audit-friendly, and clearly labeled.
- Live providers are architecturally prepared: Frankfurter, CoinGecko, Twelve Data, and BCB SGS.
- Twelve Data stock ingestion requires a valid API key and provider validation before use.
- The project is a portfolio/engineering project, not a trading tool.
- Values shown in demo mode are not financial advice.

## How to run

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m fx_rates dashboard prepare-demo --years 1 --demo
.\.venv\Scripts\python.exe -m fx_rates serve --host 127.0.0.1 --port 8000
```

In another terminal:

```powershell
cd frontend-java
mvn javafx:run
```

Visual smoke runner:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_visual_test.ps1 -PrepareDemo -SkipTests
```

Validation commands:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m fx_rates dashboard audit
.\.venv\Scripts\python.exe -m fx_rates dashboard audit-market
cd frontend-java
mvn -U clean test
mvn -q -DskipTests compile
```

## LinkedIn post - short version

Publiquei um projeto de portfolio chamado Finance Monitor: um dashboard financeiro local com backend Python/FastAPI, SQLite, auditoria de dados e frontend JavaFX.

O foco foi construir uma arquitetura clara para ingestao, API REST, persistencia local e separacao explicita entre dados demo e dados live-ready. O modo padrao usa dados demo auditaveis e identificados, sem apresentar simulacao como mercado real.

Repositorio: https://github.com/Willian-Vader-66/rates-sqlite-powerbi

## LinkedIn post - medium version

Hoje estou publicando um projeto de portfolio tecnico: Finance Monitor - Local Financial Dashboard.

A ideia foi construir um dashboard financeiro local de ponta a ponta, com backend em Python/FastAPI, banco SQLite, comandos de ingestao/auditoria, API REST e frontend desktop em JavaFX.

O ponto mais importante do projeto nao e tentar parecer um produto financeiro pronto. O foco esta em arquitetura e confiabilidade: dados demo sao explicitamente marcados como DEMO DATA, o backend trabalha com modos `demo/live/mixed/unknown`, e existem auditorias para validar consistencia entre historico, cotacoes recentes e analises.

Tambem deixei a arquitetura preparada para providers live como Frankfurter, CoinGecko, Twelve Data e BCB SGS. O live real depende de chaves/configuracao e validacao de provider, entao o release de portfolio usa dados demo auditaveis por padrao.

Stack: Python, FastAPI, SQLite, JavaFX, Maven, REST API, data validation e uma trilha inicial para Power BI/ODBC.

Repositorio: https://github.com/Willian-Vader-66/rates-sqlite-powerbi

## LinkedIn post - technical bullets

Finance Monitor - projeto de portfolio tecnico:

- Backend Python com FastAPI
- Persistencia local em SQLite
- Frontend desktop em JavaFX
- CLI para preparo, auditoria e backend local
- API REST para instrumentos, cotacoes, analises e historico
- Data modes explicitos: demo, live, mixed e unknown
- Badge visual para DEMO DATA / LIVE DATA / MIXED DATA
- Auditoria de consistencia entre quote, history e analysis
- Dados demo deterministicos e auditaveis por padrao
- Arquitetura preparada para providers live

Escopo honesto: e um dashboard local demonstrativo, nao uma recomendacao financeira nem um feed completo de mercado em tempo real.
