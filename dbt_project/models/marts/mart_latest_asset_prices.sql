with latest_rows as (
    select
        *,
        row_number() over (
            partition by asset_id
            order by snapshot_date desc, snapshot_at desc
        ) as snapshot_rank
    from {{ ref('stg_coin_markets') }}
)

select
    asset_id,
    asset_symbol,
    asset_name,
    market_snapshot_key,
    snapshot_date,
    snapshot_at,
    market_cap_rank,
    current_price_usd,
    market_cap_usd,
    total_volume_usd,
    volume_to_market_cap_ratio,
    price_change_pct_24h,
    price_change_pct_7d,
    price_change_pct_30d,
    circulating_supply,
    max_supply,
    ath_price_usd,
    atl_price_usd,
    last_updated_at
from latest_rows
where snapshot_rank = 1
