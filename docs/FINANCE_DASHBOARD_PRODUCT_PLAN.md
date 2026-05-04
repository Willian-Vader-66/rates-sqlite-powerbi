# Finance Dashboard Product Plan

## Goal

Build a local financial dashboard that collects, stores, analyzes and visualizes:

- stocks
- fiat currencies
- crypto assets
- macro indicators
- 30-day trends
- latest quotes
- analysis signals

The project should clearly demonstrate the full architecture: API ingestion -> SQLite persistence -> Python backend API -> JavaFX financial dashboard.

## Target user

The primary audience is a technical portfolio viewer, recruiter, implementation/solutions engineering interviewer, and a local user who wants a practical view of market data without connecting a front-end directly to SQLite.

## Dashboard pages / sections

### 1. Overview

- market health cards
- latest USD/BRL
- latest EUR/USD or EUR/BRL
- latest BTC/USD
- latest Selic
- latest successful ingest
- failed runs
- total instruments
- fixed 30-day mini charts
- top 10 company performance summary

### 2. FX & Crypto

- fixed 30-day chart: USD/BRL
- fixed 30-day chart: EUR/USD or EUR/BRL
- fixed 30-day chart: BTC/USD
- fixed 30-day chart: ETH/USD when available
- table of relevant currencies and crypto assets

### 3. Stocks

- top 10 company 30-day performance panel
- top movers
- breakout signals
- drawdown signals
- watchlist table

### 4. Macro

- Selic latest
- Selic 30-day chart
- macro indicator table
- provider/source notes

### 5. Instrument Detail

- selected asset quote
- 30-day chart
- trend
- signal
- volatility
- SMA 20 / SMA 50
- latest update

## Design principles

- clean first
- dark mode with subtle neon accents
- fewer borders
- less visual noise
- cards grouped by meaning
- charts readable
- table not overloaded
- responsive spacing
- clear empty states
- no aggressive animation
- no flickering refresh
- dashboard should be understandable in 5 seconds

## Backend data principles

- no fake data in the Java front-end
- demo mode comes from backend mock providers only
- provider abstraction remains explicit
- provider calls are rate-limit aware
- daily data persists in SQLite
- latest quote data lives in the latest quote table
- historical data remains queryable through HTTP
- macro indicators use a separate daily table

## Release scope for this pass

Implemented in this pass:

- macro daily table and Selic reference seed
- crypto daily table and crypto asset reference seed
- mock macro and crypto providers for local demos
- BCB SGS provider for macro data
- CoinGecko provider for basic live crypto support
- macro and crypto CLI commands
- dashboard endpoints for market overview, fixed charts, and top stocks
- Java models/service methods for the new endpoints
- cleaner Overview tab with cards, fixed mini charts, and top stocks
- first-pass CSS cleanup to reduce visual noise and chart grid density
- API contract updates and QA checklist

Deferred:

- full Stocks, FX & Crypto, Macro, and Settings pages
- richer macro indicators beyond Selic seeds
- live crypto provider hardening for high-volume use
- auth, external deployment, installer, WebSocket/SSE streaming
- screenshot capture and README image embedding
