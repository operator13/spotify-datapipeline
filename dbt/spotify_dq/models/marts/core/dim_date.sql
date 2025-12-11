{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

{#
    Dimension: Date

    Purpose:
    - Standard date dimension for time-based analysis
    - Covers years 2015-2025 (dataset range)
    - Enables consistent time-based joins and filtering

    Grain: One row per year-month combination
#}

with date_spine as (
    -- Generate year-month combinations for 2015-2025
    {{ dbt_utils.date_spine(
        datepart="month",
        start_date="cast('2015-01-01' as date)",
        end_date="cast('2026-01-01' as date)"
    )}}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['date_month']) }} as date_sk,

        -- Date components
        date_month as date_month_start,
        extract(year from date_month)::integer as year,
        extract(month from date_month)::integer as month,
        extract(quarter from date_month)::integer as quarter,

        -- Month name
        to_char(date_month, 'Month') as month_name,
        to_char(date_month, 'Mon') as month_name_short,

        -- Quarter name
        'Q' || extract(quarter from date_month)::text as quarter_name,

        -- Year-Month string
        to_char(date_month, 'YYYY-MM') as year_month,

        -- Decade classification
        case
            when extract(year from date_month) >= 2020 then '2020s'
            when extract(year from date_month) >= 2015 then '2015-2019'
            else 'Pre-2015'
        end as decade,

        -- Half year
        case
            when extract(month from date_month) <= 6 then 'H1'
            else 'H2'
        end as half_year,

        -- Season (Northern Hemisphere)
        case
            when extract(month from date_month) in (12, 1, 2) then 'Winter'
            when extract(month from date_month) in (3, 4, 5) then 'Spring'
            when extract(month from date_month) in (6, 7, 8) then 'Summer'
            else 'Fall'
        end as season,

        -- Is current year
        case
            when extract(year from date_month) = extract(year from current_date)
            then true else false
        end as is_current_year,

        -- Days in month
        extract(day from (date_month + interval '1 month' - interval '1 day'))::integer as days_in_month,

        -- Metadata
        current_timestamp as updated_at

    from date_spine
    where extract(year from date_month) between 2015 and 2025
)

select * from final
order by date_month_start
