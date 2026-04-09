# Crypto Batch ELT Project

This is a small practice project for learning a basic end-to-end ELT pipeline.

It gets crypto market data from the CoinGecko API, loads the raw data into DuckDB with `dlt`, transforms it with `dbt`, and uses `Kestra` to run the steps in order.

Everything runs locally so it is easy to start, inspect, and understand.

## Simple Flow

```text
Kestra runs:

CoinGecko API
   ->
Python + dlt
   ->
DuckDB
   ->
dbt
```

## What It Does

- pulls coin market data and global market data from CoinGecko
- loads the raw data into DuckDB
- builds staging and mart models with dbt
- runs the pipeline with Kestra inside Docker

## Main Folders

- `pipelines/`: Python code for the API calls and dlt load
- `dbt_project/`: dbt models and tests
- `kestra/`: Kestra flow file
- `data/warehouse/`: local DuckDB file
- `docker/`: Docker setup

## Tech Used

- Python
- dlt
- DuckDB
- dbt
- Kestra
- Docker

## Config

Edit `.env` before you run the project.

```dotenv
COINGECKO_API_URL=https://api.coingecko.com/api/v3
COINGECKO_API_KEY=
COINGECKO_API_KEY_HEADER=x-cg-demo-api-key
VS_CURRENCY=usd
MARKET_PAGE_SIZE=100
MARKET_PAGES=2
PRICE_CHANGE_WINDOWS=24h,7d,30d
INITIAL_SNAPSHOT_DATE=2026-01-01T00:00:00Z
```

With the default settings, the pipeline usually pulls about 200 assets.

## Run

From the project root:

```bash
docker compose up --build
```

This starts Kestra and the first run is triggered automatically.

Kestra UI:

- URL: `http://localhost:8080`
- Email: `admin@kestra.io`
- Password: `Admin1234`

You can also run:

```bash
docker compose -f docker/docker-compose.yml up --build
```

## Run Parts Manually

Run only the Python load:

```bash
docker compose exec kestra python -m pipelines.dlt_pipeline
```

Run dbt manually:

```bash
docker compose exec kestra sh ./scripts/run_dbt.sh
```

## DuckDB

Database file:

`data/warehouse/crypto_elt.duckdb`

Open it locally:

```powershell
duckdb .\data\warehouse\crypto_elt.duckdb
```

Example query:

```sql
select *
from analytics.mart_latest_asset_prices
order by market_cap_rank
limit 10;
```

## Tables

Schemas used in this project:

- `crypto_raw`: raw tables from dlt
- `analytics`: dbt staging and mart models

Main tables:

- `crypto_raw.raw_coin_markets`
- `crypto_raw.raw_global_metrics`
- `analytics.stg_coin_markets`
- `analytics.stg_global_metrics`
- `analytics.mart_latest_asset_prices`
- `analytics.mart_daily_market_overview`

## Example Queries

Top assets:

```sql
select
  asset_symbol,
  asset_name,
  market_cap_rank,
  current_price_usd
from analytics.mart_latest_asset_prices
order by market_cap_rank
limit 20;
```

Daily overview:

```sql
select
  snapshot_date,
  tracked_assets_count,
  tracked_market_cap_usd,
  global_market_cap_usd
from analytics.mart_daily_market_overview
order by snapshot_date desc;
```

7 day movers:

```sql
select
  asset_symbol,
  asset_name,
  price_change_pct_7d
from analytics.mart_latest_asset_prices
where market_cap_rank <= 50
order by price_change_pct_7d desc
limit 15;
```

## Notes

- This is a local learning project, not a production setup.
- DuckDB keeps the project simple and fast to run.
- Kestra is here to show basic orchestration without adding too much setup.

## Common Issues

If CoinGecko rate limits you:

- add an API key
- lower `MARKET_PAGES`
- lower `MARKET_PAGE_SIZE`

If dbt cannot find the source tables:

- run the dlt pipeline first
- check that `crypto_raw.raw_coin_markets` and `crypto_raw.raw_global_metrics` exist

If the DuckDB file looks empty:

- make sure you opened `data/warehouse/crypto_elt.duckdb`
- make sure it is not a different `crypto_elt.duckdb` in another folder
