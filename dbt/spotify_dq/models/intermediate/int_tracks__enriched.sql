{{
    config(
        materialized='ephemeral',
        tags=['intermediate']
    )
}}

{#
    Intermediate: Track Enrichment

    Purpose:
    - Add derived features and categorizations
    - Calculate additional metrics for analysis
    - Prepare data for marts layer consumption

    Derived Features:
    - Streaming tiers
    - Decade classification
    - Audio profile categorization
    - Relative popularity within genre
#}

with tracks as (
    select * from {{ ref('int_tracks__deduplicated') }}
),

-- Calculate genre-level statistics for relative metrics
genre_stats as (
    select
        genre,
        avg(popularity_score) as genre_avg_popularity,
        avg(streaming_counts) as genre_avg_streams,
        count(*) as genre_track_count
    from tracks
    where genre is not null
    group by genre
),

-- Enrich tracks with derived features
enriched as (
    select
        t.track_id,
        t.track_name,
        t.artist_names_raw,
        t.album_name,
        t.genre,
        t.country,
        t.record_label,
        t.release_date_raw,
        t.release_year,
        t.release_month,

        -- Audio Features
        t.energy,
        t.danceability,
        t.instrumentalness,
        t.valence,
        t.tempo_bpm,
        t.loudness_db,
        t.mood_score,

        -- Metrics
        t.popularity_score,
        t.streaming_counts,

        -- Existing Categories
        t.popularity_tier,
        t.tempo_category,
        t.energy_category,

        -- =================================================================
        -- New Derived Features
        -- =================================================================

        -- Streaming Tier
        case
            when t.streaming_counts >= 1000000000 then 'Billion Club'
            when t.streaming_counts >= 500000000 then '500M+'
            when t.streaming_counts >= 100000000 then '100M+'
            when t.streaming_counts >= 50000000 then '50M+'
            when t.streaming_counts >= 10000000 then '10M+'
            when t.streaming_counts >= 1000000 then '1M+'
            when t.streaming_counts >= 100000 then '100K+'
            else 'Under 100K'
        end as streaming_tier,

        -- Decade Classification
        case
            when t.release_year >= 2020 then '2020s'
            when t.release_year >= 2015 then '2015-2019'
            else 'Pre-2015'
        end as release_decade,

        -- Audio Profile (based on dominant characteristics)
        case
            when t.instrumentalness > 0.5 then 'Instrumental'
            when t.energy > 0.7 and t.danceability > 0.6 then 'High Energy Dance'
            when t.valence > 0.7 and t.energy > 0.5 then 'Upbeat'
            when t.valence < 0.3 and t.energy < 0.4 then 'Melancholic'
            when t.danceability > 0.7 then 'Dance'
            when t.energy < 0.4 then 'Chill'
            else 'Balanced'
        end as audio_profile,

        -- Loudness Category
        case
            when t.loudness_db >= -5 then 'Very Loud'
            when t.loudness_db >= -8 then 'Loud'
            when t.loudness_db >= -12 then 'Moderate'
            when t.loudness_db >= -20 then 'Quiet'
            else 'Very Quiet'
        end as loudness_category,

        -- Danceability Category
        case
            when t.danceability >= 0.8 then 'Very Danceable'
            when t.danceability >= 0.6 then 'Danceable'
            when t.danceability >= 0.4 then 'Moderate'
            else 'Low Danceability'
        end as danceability_category,

        -- Valence Category (Mood)
        case
            when t.valence >= 0.8 then 'Very Positive'
            when t.valence >= 0.6 then 'Positive'
            when t.valence >= 0.4 then 'Neutral'
            when t.valence >= 0.2 then 'Melancholic'
            else 'Very Dark'
        end as valence_category,

        -- Relative Popularity (compared to genre average)
        case
            when gs.genre_avg_popularity is null then 'Unknown'
            when t.popularity_score >= gs.genre_avg_popularity * 1.5 then 'Above Average'
            when t.popularity_score >= gs.genre_avg_popularity * 0.75 then 'Average'
            else 'Below Average'
        end as relative_popularity,

        -- Popularity Percentile within genre
        round(
            (percent_rank() over (
                partition by t.genre
                order by t.popularity_score
            ) * 100)::numeric, 2
        ) as popularity_percentile_in_genre,

        -- Streaming Percentile overall
        round(
            (percent_rank() over (
                order by t.streaming_counts
            ) * 100)::numeric, 2
        ) as streaming_percentile,

        -- Is Hit (top 10% popularity in genre)
        case
            when percent_rank() over (
                partition by t.genre
                order by t.popularity_score
            ) >= 0.9 then true
            else false
        end as is_genre_hit,

        -- Genre Stats
        gs.genre_avg_popularity,
        gs.genre_avg_streams,
        gs.genre_track_count,

        -- Dedup Metadata
        t.had_duplicates,

        -- Original Metadata
        t._loaded_at,
        t._dbt_loaded_at,
        t._dbt_run_id

    from tracks t
    left join genre_stats gs
        on t.genre = gs.genre
)

select * from enriched
