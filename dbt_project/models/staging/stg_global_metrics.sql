with source_data as (
    select *
    from {{ source('crypto_raw', 'raw_global_metrics') }}
)

select
    cast(global_snapshot_key as varchar) as global_snapshot_key,
    cast(active_cryptocurrencies as bigint) as active_cryptocurrencies,
    cast(upcoming_icos as bigint) as upcoming_icos,
    cast(ongoing_icos as bigint) as ongoing_icos,
    cast(ended_icos as bigint) as ended_icos,
    cast(markets as bigint) as markets,
    cast(total_market_cap_usd as double) as total_market_cap_usd,
    cast(total_volume_usd as double) as total_volume_usd,
    cast(btc_market_cap_percentage as double) as btc_market_cap_percentage,
    cast(eth_market_cap_percentage as double) as eth_market_cap_percentage,
    cast(market_cap_change_percentage_24h_usd as double) as market_cap_change_pct_24h_usd,
    cast(snapshot_at as timestamp) as snapshot_at,
    cast(cast(snapshot_date as timestamp) as date) as snapshot_date,
    cast(vs_currency as varchar) as vs_currency
from source_data
