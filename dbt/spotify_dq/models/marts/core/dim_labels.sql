{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

{#
    Dimension: Record Labels

    Purpose:
    - Label-level aggregations and performance metrics
    - Analysis of label market share and success rates
    - Label portfolio characteristics

    Grain: One row per unique record label
#}

with tracks as (
    select * from {{ ref('int_tracks__enriched') }}
),

label_aggregates as (
    select
        record_label,

        -- Portfolio metrics
        count(*) as track_count,
        count(distinct artist_names_raw) as artist_count,
        count(distinct genre) as genre_count,
        count(distinct country) as country_count,

        -- Popularity metrics
        round(avg(popularity_score)::numeric, 2) as avg_popularity,
        round(stddev(popularity_score)::numeric, 2) as stddev_popularity,
        max(popularity_score) as max_popularity,

        -- Streaming metrics
        sum(streaming_counts) as total_streams,
        round(avg(streaming_counts)::numeric, 0) as avg_streams_per_track,
        max(streaming_counts) as max_streams_single_track,

        -- Hit rate
        round(
            sum(case when is_genre_hit then 1 else 0 end)::numeric / nullif(count(*), 0),
            4
        ) as hit_rate,

        -- Top tier rate
        round(
            sum(case when popularity_tier = 'Very Popular' then 1 else 0 end)::numeric / nullif(count(*), 0),
            4
        ) as top_tier_rate,

        -- Audio profile
        round(avg(danceability)::numeric, 4) as avg_danceability,
        round(avg(energy)::numeric, 4) as avg_energy,
        round(avg(valence)::numeric, 4) as avg_valence,

        -- Genre focus
        mode() within group (order by genre) as primary_genre,

        -- Time range
        min(release_year) as first_release_year,
        max(release_year) as last_release_year

    from tracks
    where record_label is not null
      and trim(record_label) != ''
    group by record_label
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['record_label']) }} as label_sk,

        -- Label info
        record_label as label_name,

        -- Portfolio
        track_count,
        artist_count,
        genre_count,
        country_count,

        -- Performance
        avg_popularity,
        stddev_popularity,
        max_popularity,
        total_streams,
        avg_streams_per_track,
        max_streams_single_track,
        hit_rate,
        top_tier_rate,

        -- Audio characteristics
        avg_danceability,
        avg_energy,
        avg_valence,

        -- Classification
        primary_genre,
        first_release_year,
        last_release_year,

        -- Derived: Label tier
        case
            when track_count >= 1000 and avg_popularity >= 50 then 'Major Label'
            when track_count >= 100 and avg_popularity >= 40 then 'Mid-Size Label'
            when track_count >= 10 then 'Indie Label'
            else 'Micro Label'
        end as label_tier,

        -- Derived: Success rate classification
        case
            when hit_rate >= 0.2 then 'High Success'
            when hit_rate >= 0.1 then 'Moderate Success'
            when hit_rate >= 0.05 then 'Average Success'
            else 'Developing'
        end as success_classification,

        -- Derived: Portfolio diversity
        case
            when genre_count >= 10 then 'Highly Diverse'
            when genre_count >= 5 then 'Diverse'
            when genre_count >= 2 then 'Moderate'
            else 'Focused'
        end as portfolio_diversity,

        -- Metadata
        current_timestamp as updated_at

    from label_aggregates
)

select * from final
