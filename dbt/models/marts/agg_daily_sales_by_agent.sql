with daily_sales_by_agent as (
    select
        agent_id,
        date_id,
        sum(premium_amount) as daily_premium,
        count(*) as daily_count
    from {{ ref('stg_policy_sales') }}
    group by 1, 2
)

select * from daily_sales_by_agent
