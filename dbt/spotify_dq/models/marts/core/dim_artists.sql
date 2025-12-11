{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

{#
    Dimension: Artists

    Purpose:
    - Centralized artist dimension with career metrics
    - Aggregated performance statistics
    - Artist tier classification

    Grain: One row per unique artist
#}

with artists as (
    select * from {{ ref('int_artists__parsed') }}
),

final as (
    select
        -- Keys
        artist_id as artist_sk,
        artist_name,
        artist_name_normalized,

        -- Track metrics
        track_count,
        genre_count,
        country_count,

        -- Primary classifications
        primary_genre,
        primary_country,

        -- Popularity metrics
        avg_popularity,
        max_popularity,
        min_popularity,

        -- Streaming metrics
        total_streams,
        avg_streams_per_track,

        -- Career info
        first_release_year,
        last_release_year,
        career_span_years,

        -- Tiers
        artist_tier,
        streaming_tier,

        -- Derived: Activity status
        case
            when last_release_year >= 2023 then 'Active'
            when last_release_year >= 2020 then 'Recent'
            when last_release_year >= 2015 then 'Inactive'
            else 'Legacy'
        end as activity_status,

        -- Derived: Consistency score (stddev of popularity would be ideal but using proxy)
        case
            when max_popularity - min_popularity < 20 then 'Very Consistent'
            when max_popularity - min_popularity < 40 then 'Consistent'
            when max_popularity - min_popularity < 60 then 'Variable'
            else 'Highly Variable'
        end as consistency_level,

        -- Derived: Versatility (based on genre count)
        case
            when genre_count >= 5 then 'Very Versatile'
            when genre_count >= 3 then 'Versatile'
            when genre_count >= 2 then 'Moderate'
            else 'Focused'
        end as versatility_level,

        -- Derived: Geographic reach
        case
            when country_count >= 10 then 'Global'
            when country_count >= 5 then 'International'
            when country_count >= 2 then 'Regional'
            else 'Local'
        end as geographic_reach,

        -- Metadata
        current_timestamp as updated_at

    from artists
)

select * from final
