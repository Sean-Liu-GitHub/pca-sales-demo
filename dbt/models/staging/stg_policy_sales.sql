with source as (
    select * from {{ source('raw', 'policy_sales') }}
),
staged as (
    select
        event_id,
        event_ts,
        policy_id,
        agent_id,
        product_id,
        region_id,
        channel,
        premium_amount::integer as premium_amount,
        sum_assured::integer as sum_assured,
        payment_frequency,
        event_ts::date as date_id,
        ingested_at
    from source
)

select * from staged
