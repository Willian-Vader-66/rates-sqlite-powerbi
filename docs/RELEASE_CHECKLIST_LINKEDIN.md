# Release Checklist - LinkedIn Portfolio

## Backend

- [x] `pytest` passing
- [x] `dashboard prepare-demo --years 1 --demo` passing
- [x] `dashboard audit` without critical alerts
- [x] `dashboard audit-market` confirms demo data is explicit
- [x] API smoke for health, status, summary, instruments, quotes, analysis, and history endpoints

## Frontend

- [x] Maven tests passing
- [x] Maven compile passing
- [x] Visual runner no-frontend passing
- [ ] Manual JavaFX window check
- [ ] Screenshot: Overview
- [ ] Screenshot: Watchlist
- [ ] Screenshot: Settings/data mode

Manual UI points to check before posting screenshots:

- [ ] Window opens
- [ ] Overview appears populated
- [ ] Watchlist loads
- [ ] Settings shows `data_mode`
- [ ] DEMO DATA / LIVE DATA / MIXED DATA badge is visible as applicable
- [ ] No critical blank screen
- [ ] No excessive flicker
- [ ] Filters appear
- [ ] Main charts load

## Data

- [x] Current `data_mode` is clear
- [x] Demo/live/mixed distinction is documented
- [x] SQLite runtime DB is not staged/committed
- [x] API key is not committed
- [x] `.env` is not committed

## Git

- [x] Branch reviewed
- [x] Remote reviewed
- [x] `.gitignore` includes runtime artifacts
- [ ] Run `git diff --cached --check` after staging selected files
- [ ] Confirm `git status --short` before commit

## LinkedIn

- [ ] Screenshot of main dashboard
- [ ] Screenshot of Watchlist
- [ ] Screenshot of Settings/data mode
- [ ] GitHub link added to post text
- [x] Short post draft ready
- [x] Medium post draft ready
- [x] Technical bullet post draft ready
- [x] Scope described honestly as portfolio/demo/live-ready architecture

## Do not commit

- `.env`
- `.venv/`
- `data/*.sqlite`
- `data/*.sqlite-*`
- `logs/`
- `cache/`
- `frontend-java/target/`
- `__pycache__/`
- `.pytest_cache/`
- `*.egg-info/`
- `.tmp/`
