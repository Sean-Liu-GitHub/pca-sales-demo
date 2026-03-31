with date_spine as (
    select
        generate_series(
            (select min(event_ts)::date from {{ ref('stg_policy_sales') }}),
            (select max(event_ts)::date from {{ ref('stg_policy_sales') }}) + interval '1 day',
            interval '1 day'
        )::date as date_id
)

select
    date_id,
    to_char(date_id, 'Day')        as day_of_week,
    to_char(date_id, 'Month')      as month,
    'Q' || extract(quarter from date_id)::int as quarter,
    extract(year from date_id)::int as year

from date_spine
