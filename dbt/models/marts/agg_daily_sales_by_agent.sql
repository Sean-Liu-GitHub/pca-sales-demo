with daily_sales_by_agent as (
    select
        a.agent_id,
        a.agent_name as agent_name,
        s.date_id,
        sum(s.premium_amount) as daily_premium,
        count(s.*) as daily_count
    from {{ ref('stg_policy_sales') }} s
    join {{ ref('dim_agents') }} a
        on s.agent_id = a.agent_id
    group by 1, 2
)

select * from daily_sales_by_agent
