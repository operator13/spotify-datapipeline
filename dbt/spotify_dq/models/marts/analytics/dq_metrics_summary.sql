{{
    config(
        materialized='table',
        tags=['analytics', 'data_quality']
    )
}}

{#
    Data Quality Metrics Summary

    Purpose:
    - Aggregate all data quality metrics across 6 dimensions
    - Feed Grafana dashboards with current DQ scores
    - Historical tracking via post_hook insert to data_quality.dq_metrics

    Dimensions Covered:
    1. Completeness - % of non-null values
    2. Accuracy - % of values within expected ranges
    3. Consistency - % of valid foreign key references
    4. Timeliness - Hours since last data update
    5. Validity - % of values meeting business rules
    6. Uniqueness - % of unique records (no duplicates)
#}

-- =============================================================================
-- COMPLETENESS METRICS
-- =============================================================================
with completeness_metrics as (
    select
        'Completeness' as dimension,
        'fct_tracks' as table_name,
        'track_name_completeness' as metric_name,
        count(track_name)::float / nullif(count(*), 0) as metric_value,
        0.95 as threshold_value
    from {{ ref('fct_tracks') }}

    union all

    select
        'Completeness',
        'fct_tracks',
        'genre_completeness',
        count(genre)::float / nullif(count(*), 0),
        0.90
    from {{ ref('fct_tracks') }}

    union all

    select
        'Completeness',
        'fct_tracks',
        'country_completeness',
        count(country)::float / nullif(count(*), 0),
        0.85
    from {{ ref('fct_tracks') }}
),

-- =============================================================================
-- ACCURACY METRICS
-- =============================================================================
accuracy_metrics as (
    select
        'Accuracy' as dimension,
        'fct_tracks' as table_name,
        'popularity_in_range' as metric_name,
        count(case when popularity_score between 0 and 100 then 1 end)::float / nullif(count(*), 0) as metric_value,
        0.999 as threshold_value
    from {{ ref('fct_tracks') }}

    union all

    select
        'Accuracy',
        'fct_tracks',
        'audio_features_valid',
        count(case
            when danceability between 0 and 1
             and energy between 0 and 1
             and valence between 0 and 1
            then 1 end)::float / nullif(count(*), 0),
        0.999
    from {{ ref('fct_tracks') }}

    union all

    select
        'Accuracy',
        'fct_tracks',
        'tempo_reasonable',
        count(case when tempo_bpm between 40 and 250 then 1 end)::float / nullif(count(*), 0),
        0.95
    from {{ ref('fct_tracks') }}
),

-- =============================================================================
-- CONSISTENCY METRICS
-- =============================================================================
consistency_metrics as (
    select
        'Consistency' as dimension,
        'fct_tracks' as table_name,
        'genre_fk_valid' as metric_name,
        count(case when g.genre_sk is not null or f.genre_sk is null then 1 end)::float / nullif(count(*), 0) as metric_value,
        0.99 as threshold_value
    from {{ ref('fct_tracks') }} f
    left join {{ ref('dim_genres') }} g on f.genre_sk = g.genre_sk

    union all

    select
        'Consistency',
        'fct_tracks',
        'country_fk_valid',
        count(case when c.country_sk is not null or f.country_sk is null then 1 end)::float / nullif(count(*), 0),
        0.99
    from {{ ref('fct_tracks') }} f
    left join {{ ref('dim_countries') }} c on f.country_sk = c.country_sk
),

-- =============================================================================
-- TIMELINESS METRICS
-- =============================================================================
timeliness_metrics as (
    select
        'Timeliness' as dimension,
        'fct_tracks' as table_name,
        'data_freshness_hours' as metric_name,
        extract(epoch from (current_timestamp - max(dbt_loaded_at))) / 3600 as metric_value,
        48.0 as threshold_value  -- Data should be less than 48 hours old
    from {{ ref('fct_tracks') }}
),

-- =============================================================================
-- VALIDITY METRICS
-- =============================================================================
validity_metrics as (
    select
        'Validity' as dimension,
        'fct_tracks' as table_name,
        'popularity_tier_valid' as metric_name,
        count(case when popularity_tier in ('Very Popular', 'Popular', 'Moderate', 'Low', 'Very Low') then 1 end)::float / nullif(count(*), 0) as metric_value,
        1.0 as threshold_value
    from {{ ref('fct_tracks') }}

    union all

    select
        'Validity',
        'fct_tracks',
        'streaming_counts_positive',
        count(case when streaming_counts >= 0 then 1 end)::float / nullif(count(*), 0),
        1.0
    from {{ ref('fct_tracks') }}

    union all

    select
        'Validity',
        'fct_tracks',
        'release_year_valid',
        count(case when release_year between 2015 and 2025 then 1 end)::float / nullif(count(release_year), 0),
        0.95
    from {{ ref('fct_tracks') }}
),

-- =============================================================================
-- UNIQUENESS METRICS
-- =============================================================================
uniqueness_metrics as (
    select
        'Uniqueness' as dimension,
        'fct_tracks' as table_name,
        'track_id_unique' as metric_name,
        count(distinct track_id)::float / nullif(count(*), 0) as metric_value,
        1.0 as threshold_value
    from {{ ref('fct_tracks') }}
),

-- =============================================================================
-- COMBINED METRICS
-- =============================================================================
all_metrics as (
    select * from completeness_metrics
    union all
    select * from accuracy_metrics
    union all
    select * from consistency_metrics
    union all
    select * from timeliness_metrics
    union all
    select * from validity_metrics
    union all
    select * from uniqueness_metrics
)

select
    dimension,
    table_name,
    metric_name,
    round(metric_value::numeric, 4) as metric_value,
    threshold_value,
    case
        -- For Timeliness, lower is better (fresher data)
        when dimension = 'Timeliness' then metric_value <= threshold_value
        -- For all other metrics, higher is better
        else metric_value >= threshold_value
    end as passed,
    current_timestamp as calculated_at,
    '{{ invocation_id }}' as dbt_run_id
from all_metrics
