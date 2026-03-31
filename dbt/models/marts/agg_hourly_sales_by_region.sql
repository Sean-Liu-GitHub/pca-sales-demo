with hourly_sales_by_region as (
    select
        date_trunc('hour', s.event_ts) as hour_ts,
        r.region_name as region,
        sum(s.premium_amount) as total_premium,
        count(s.*) as policy_count,
        avg(s.premium_amount)::integer as avg_premium
    from {{ ref('stg_policy_sales') }} s
    inner join {{ ref('dim_regions') }} r
        on s.region_id = r.region_id
    group by 1, 2
)

select * from hourly_sales_by_region
