with hourly_sales_by_region as (
    select
        date_trunc('hour', event_ts) as hour_ts,
        region_id,
        sum(premium_amount) as total_premium,
        count(*) as policy_count,
        avg(premium_amount)::integer as avg_premium
    from {{ ref('stg_policy_sales') }}
    group by 1, 2
)

select * from hourly_sales_by_region
