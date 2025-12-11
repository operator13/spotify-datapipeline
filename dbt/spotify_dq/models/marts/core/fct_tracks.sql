{{
    config(
        materialized='table',
        tags=['marts', 'core', 'fact', 'critical'],
        post_hook=[
            "CREATE INDEX IF NOT EXISTS idx_fct_tracks_genre ON {{ this }} (genre_sk)",
            "CREATE INDEX IF NOT EXISTS idx_fct_tracks_country ON {{ this }} (country_sk)",
            "CREATE INDEX IF NOT EXISTS idx_fct_tracks_popularity ON {{ this }} (popularity_tier)",
            "CREATE INDEX IF NOT EXISTS idx_fct_tracks_release ON {{ this }} (release_year)",
            "ANALYZE {{ this }}"
        ]
    )
}}

{#
    Fact Table: Tracks

    Purpose:
    - Central fact table for track-level analysis
    - Contains all measures and foreign keys to dimensions
    - Optimized for analytical queries

    Grain: One row per unique track
#}

with enriched_tracks as (
    select * from {{ ref('int_tracks__enriched') }}
),

-- Dimension lookups
genres as (
    select genre_sk, genre_name from {{ ref('dim_genres') }}
),

countries as (
    select country_sk, country_code from {{ ref('dim_countries') }}
),

labels as (
    select label_sk, label_name from {{ ref('dim_labels') }}
),

dates as (
    select date_sk, year, month from {{ ref('dim_date') }}
),

final as (
    select
        -- =================================================================
        -- Surrogate Key
        -- =================================================================
        {{ dbt_utils.generate_surrogate_key(['et.track_id']) }} as track_sk,

        -- =================================================================
        -- Natural Key
        -- =================================================================
        et.track_id,

        -- =================================================================
        -- Foreign Keys (Dimension References)
        -- =================================================================
        g.genre_sk,
        c.country_sk,
        l.label_sk,
        d.date_sk as release_date_sk,

        -- =================================================================
        -- Track Attributes
        -- =================================================================
        et.track_name,
        et.album_name,
        et.artist_names_raw as artists,
        et.genre,
        et.country,
        et.record_label,

        -- =================================================================
        -- Release Info
        -- =================================================================
        et.release_date_raw,
        et.release_year,
        et.release_month,
        et.release_decade,

        -- =================================================================
        -- Measures - Popularity & Streaming
        -- =================================================================
        et.popularity_score,
        et.streaming_counts,

        -- =================================================================
        -- Measures - Audio Features
        -- =================================================================
        et.danceability,
        et.energy,
        et.instrumentalness,
        et.valence,
        et.mood_score,
        et.tempo_bpm,
        et.loudness_db,

        -- =================================================================
        -- Derived Categories
        -- =================================================================
        et.popularity_tier,
        et.tempo_category,
        et.energy_category,
        et.streaming_tier,
        et.audio_profile,
        et.loudness_category,
        et.danceability_category,
        et.valence_category,
        et.relative_popularity,

        -- =================================================================
        -- Percentiles & Rankings
        -- =================================================================
        et.popularity_percentile_in_genre,
        et.streaming_percentile,
        et.is_genre_hit,

        -- =================================================================
        -- Genre Context
        -- =================================================================
        et.genre_avg_popularity,
        et.genre_avg_streams,
        et.genre_track_count,

        -- =================================================================
        -- Data Quality Flags
        -- =================================================================
        et.had_duplicates,

        -- =================================================================
        -- Metadata
        -- =================================================================
        et._loaded_at as source_loaded_at,
        et._dbt_loaded_at as dbt_loaded_at,
        et._dbt_run_id as dbt_run_id,
        current_timestamp as updated_at

    from enriched_tracks et

    -- Join dimensions
    left join genres g
        on et.genre = g.genre_name
    left join countries c
        on et.country = c.country_code
    left join labels l
        on et.record_label = l.label_name
    left join dates d
        on et.release_year = d.year
        and et.release_month = d.month
)

select * from final
