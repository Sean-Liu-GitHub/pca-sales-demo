with daily_sales as (
    select
        date_id,
        sum(premium_amount) as total_premium,
        count(*) as total_policies,
        avg(premium_amount)::integer as avg_premium,
        count(distinct agent_id) as active_agents,
        max(event_ts) as last_updated
    from staging.stg_policy_sales
    group by 1
)

select * from daily_sales