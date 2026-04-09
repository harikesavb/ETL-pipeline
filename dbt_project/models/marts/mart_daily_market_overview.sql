with daily_summary as (
    select
        snapshot_date,
        count(*) as tracked_assets_count,
        sum(market_cap_usd) as tracked_market_cap_usd,
        sum(total_volume_usd) as tracked_volume_usd,
        avg(price_change_pct_24h) as avg_price_change_pct_24h,
        avg(price_change_pct_7d) as avg_price_change_pct_7d,
        avg(price_change_pct_30d) as avg_price_change_pct_30d
    from {{ ref('stg_coin_markets') }}
    group by 1
)

select
    cast(daily_summary.snapshot_date as varchar) as summary_key,
    daily_summary.snapshot_date,
    daily_summary.tracked_assets_count,
    daily_summary.tracked_market_cap_usd,
    daily_summary.tracked_volume_usd,
    daily_summary.avg_price_change_pct_24h,
    daily_summary.avg_price_change_pct_7d,
    daily_summary.avg_price_change_pct_30d,
    global_metrics.total_market_cap_usd as global_market_cap_usd,
    global_metrics.total_volume_usd as global_volume_usd,
    global_metrics.active_cryptocurrencies,
    global_metrics.markets as exchange_markets_count,
    global_metrics.btc_market_cap_percentage,
    global_metrics.eth_market_cap_percentage,
    global_metrics.market_cap_change_pct_24h_usd,
    case
        when global_metrics.total_market_cap_usd > 0
            then 100.0 * daily_summary.tracked_market_cap_usd / global_metrics.total_market_cap_usd
        else null
    end as tracked_share_of_global_market_pct
from daily_summary
left join {{ ref('stg_global_metrics') }} as global_metrics
    on daily_summary.snapshot_date = global_metrics.snapshot_date
