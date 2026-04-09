with source_data as (
    select *
    from {{ source('crypto_raw', 'raw_coin_markets') }}
),

cleaned as (
    select
        cast(market_snapshot_key as varchar) as market_snapshot_key,
        cast(asset_id as varchar) as asset_id,
        upper(cast(symbol as varchar)) as asset_symbol,
        cast(name as varchar) as asset_name,
        cast(image as varchar) as image_url,
        cast(current_price as double) as current_price_usd,
        cast(market_cap as double) as market_cap_usd,
        cast(market_cap_rank as bigint) as market_cap_rank,
        cast(fully_diluted_valuation as double) as fully_diluted_valuation_usd,
        cast(total_volume as double) as total_volume_usd,
        cast(high_24h as double) as high_24h_usd,
        cast(low_24h as double) as low_24h_usd,
        cast(price_change_24h as double) as price_change_24h_usd,
        cast(price_change_percentage_24h as double) as price_change_pct_24h,
        cast(price_change_percentage_24h_in_currency as double) as price_change_pct_24h_in_currency,
        cast(price_change_percentage_7d_in_currency as double) as price_change_pct_7d,
        cast(price_change_percentage_30d_in_currency as double) as price_change_pct_30d,
        cast(market_cap_change_24h as double) as market_cap_change_24h_usd,
        cast(market_cap_change_percentage_24h as double) as market_cap_change_pct_24h,
        cast(circulating_supply as double) as circulating_supply,
        cast(total_supply as double) as total_supply,
        cast(max_supply as double) as max_supply,
        cast(ath as double) as ath_price_usd,
        cast(ath_change_percentage as double) as ath_change_pct,
        cast(ath_date as timestamp) as ath_date,
        cast(atl as double) as atl_price_usd,
        cast(atl_change_percentage as double) as atl_change_pct,
        cast(atl_date as timestamp) as atl_date,
        cast(last_updated as timestamp) as last_updated_at,
        cast(snapshot_at as timestamp) as snapshot_at,
        cast(cast(snapshot_date as timestamp) as date) as snapshot_date,
        cast(vs_currency as varchar) as vs_currency
    from source_data
)

select
    cleaned.*,
    case
        when market_cap_usd > 0 then total_volume_usd / market_cap_usd
        else null
    end as volume_to_market_cap_ratio
from cleaned
