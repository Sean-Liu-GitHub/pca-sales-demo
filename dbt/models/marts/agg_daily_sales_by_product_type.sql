with daily_sales_by_product_type as (
    select
        p.product_type,
        s.date_id,
        count(s.*) as policy_count,
        sum(s.premium_amount) as total_premium
    from {{ ref('stg_policy_sales') }} s
    inner join {{ ref('dim_products') }} p
        on s.product_id = p.product_id
    group by 1, 2
)

select * from daily_sales_by_product_type
