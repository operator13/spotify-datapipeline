{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

{#
    Dimension: Countries

    Purpose:
    - Country-level aggregations and statistics
    - Geographic analysis of music production
    - Regional music characteristics

    Grain: One row per unique country
#}

with tracks as (
    select * from {{ ref('int_tracks__enriched') }}
),

-- Reference data for country metadata
country_ref as (
    select * from {{ ref('valid_countries') }}
),

country_aggregates as (
    select
        t.country,

        -- Track counts
        count(*) as track_count,
        count(distinct t.artist_names_raw) as artist_count,
        count(distinct t.genre) as genre_count,
        count(distinct t.record_label) as label_count,

        -- Popularity metrics
        round(avg(t.popularity_score)::numeric, 2) as avg_popularity,
        max(t.popularity_score) as max_popularity,

        -- Streaming metrics
        sum(t.streaming_counts) as total_streams,
        round(avg(t.streaming_counts)::numeric, 0) as avg_streams_per_track,

        -- Audio characteristics
        round(avg(t.danceability)::numeric, 4) as avg_danceability,
        round(avg(t.energy)::numeric, 4) as avg_energy,
        round(avg(t.valence)::numeric, 4) as avg_valence,
        round(avg(t.tempo_bpm)::numeric, 2) as avg_tempo,
        round(avg(t.mood_score)::numeric, 4) as avg_mood_score,

        -- Dominant genre
        mode() within group (order by t.genre) as dominant_genre,

        -- Time range
        min(t.release_year) as first_release_year,
        max(t.release_year) as last_release_year

    from tracks t
    where t.country is not null
    group by t.country
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['ca.country']) }} as country_sk,

        -- Country info
        ca.country as country_code,
        coalesce(cr.country_name, ca.country) as country_name,
        cr.region,

        -- Metrics
        ca.track_count,
        ca.artist_count,
        ca.genre_count,
        ca.label_count,

        -- Popularity
        ca.avg_popularity,
        ca.max_popularity,

        -- Streaming
        ca.total_streams,
        ca.avg_streams_per_track,

        -- Audio profile
        ca.avg_danceability,
        ca.avg_energy,
        ca.avg_valence,
        ca.avg_tempo,
        ca.avg_mood_score,

        -- Classification
        ca.dominant_genre,
        ca.first_release_year,
        ca.last_release_year,

        -- Derived: Market size tier
        case
            when ca.track_count >= 5000 then 'Major Market'
            when ca.track_count >= 1000 then 'Established Market'
            when ca.track_count >= 100 then 'Growing Market'
            else 'Emerging Market'
        end as market_tier,

        -- Derived: Music mood profile
        case
            when ca.avg_energy > 0.65 and ca.avg_danceability > 0.6 then 'High Energy'
            when ca.avg_valence > 0.6 then 'Upbeat'
            when ca.avg_valence < 0.4 then 'Melancholic'
            else 'Balanced'
        end as music_mood_profile,

        -- Metadata
        current_timestamp as updated_at

    from country_aggregates ca
    left join country_ref cr
        on ca.country = cr.country_code
)

select * from final
