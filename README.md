# fx-rates-sqlite-powerbi

Projeto de portfolio para ingestao de taxas de cambio via API Frankfurter, com validacao estrita, persistencia idempotente em SQLite (upsert), logs operacionais, cache local e consumo no Power BI.

## Setup (Windows PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
$env:PYTHONPATH = "src"
```

## Comandos CLI

Novo padrao:

```powershell
python -m fx_rates backfill --start 2025-01-01 --end 2025-12-31 --base USD --symbols BRL,EUR
python -m fx_rates daily --base USD --symbols BRL,EUR
python -m fx_rates status
```

Compatibilidade temporaria (1 ciclo):

```powershell
python -m fx_ingest backfill --start 2025-01-01 --end 2025-12-31 --base USD --symbols BRL,EUR
python -m fx_ingest daily --base USD --symbols BRL,EUR
```

Flags comuns:

- `--db-path`
- `--cache-dir`
- `--no-cache`
- `--log-level` (`DEBUG|INFO|WARNING|ERROR|CRITICAL`)
- `--timeout`

## Validar dados no SQLite

```powershell
python -c "import sqlite3; c=sqlite3.connect('data/fx.sqlite'); print(c.execute('select count(*) from fx_rates').fetchone()[0]); c.close()"
```

## Testes

```powershell
$env:PYTHONPATH = "src"
pytest -q
```

## Power BI via ODBC

1. Instale um driver ODBC SQLite no Windows.
2. Crie um DSN apontando para `data/fx.sqlite`.
3. No Power BI Desktop: `Home -> Get Data -> ODBC`.
4. Selecione o DSN e carregue a tabela `fx_rates`.

Dashboard minimo:

- Line chart: `date` (X), `rate` (Y), legenda por `symbol`
- Slicer por `symbol`
- Card de ultima taxa

Arquivo de evidencia:

- `assets/powerbi_screenshot.png` (substitua o placeholder por screenshot real do dashboard)

## Logs, cache e rastreabilidade

- Log em arquivo: `logs/app.log`
- Cache local: `cache/<hash>.json`
- `ingest_runs` registra: inicio/fim, modo, parametros, row_count, status, erro

## Agendamento diario (Task Scheduler)

Use o script:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_daily.ps1 -Base USD -Symbols BRL,EUR
```

## Troubleshooting

- Timeout/HTTP: aumente `--timeout` e verifique conectividade.
- Payload invalido: execucao fail-fast, run marcado como `FAIL` em `ingest_runs`.
- ODBC/DSN: confirme que o DSN aponta para `data/fx.sqlite` e que a tabela `fx_rates` existe.
