# FX Rates Ingest (SQLite + Power BI)

Pipeline simples para coletar cambio da API Frankfurter, normalizar dados e gravar em SQLite com rastreabilidade de execucao (`ingest_runs`).

## Requisitos

- Windows + PowerShell
- Python 3.10+

## Setup (Windows PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
```

## Rodar backfill (ultimos 90 dias)

Exemplo usando o intervalo sugerido:

```powershell
python -m fx_ingest backfill --start 2025-11-01 --end 2026-02-10 --base USD --symbols BRL,EUR
```

## Rodar carga diaria

```powershell
python -m fx_ingest daily --base USD --symbols BRL,EUR
```

## Verificar se o SQLite tem linhas

```powershell
python -c "import sqlite3; c=sqlite3.connect('data/fx.sqlite'); print(c.execute('select count(*) from fx_rates').fetchone()[0]); c.close()"
```

## Teste rapido

```powershell
$env:PYTHONPATH='src'
python -m unittest discover -s tests -v
```

## Estrutura de tabelas

- `fx_rates(date, base, symbol, rate, source, fetched_at, created_at, updated_at)`
  - PK composta: `(date, base, symbol)`
  - Upsert com `ON CONFLICT ... DO UPDATE`
- `ingest_runs(id, command, args, status, started_at, finished_at, rows_inserted, error_message)`

## Power BI (ODBC + SQLite)

1. Instale um driver ODBC de SQLite no Windows.
2. Crie um DSN apontando para `data/fx.sqlite`.
3. No Power BI Desktop: `Home -> Get Data -> ODBC`.
4. Escolha o DSN e carregue a tabela `fx_rates`.

Visuais recomendados:

- Line chart: `date` (X), `rate` (Y), legenda por `symbol`
- Slicer por `symbol`
- Card para ultima taxa

## Agendamento diario (Task Scheduler)

Acao sugerida:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi
.\.venv\Scripts\python.exe -m fx_ingest daily --base USD --symbols BRL,EUR
```

## Logs

- Arquivo: `logs/app.log`
- Nivel padrao: `INFO`
