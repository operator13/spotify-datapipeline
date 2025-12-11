{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

{#
    Dimension: Genres

    Purpose:
    - Centralized genre dimension with aggregated audio characteristics
    - Provides genre-level statistics for analysis
    - Enables consistent genre references across the model

    Grain: One row per unique genre
#}

with tracks as (
    select * from {{ ref('int_tracks__enriched') }}
),

genre_aggregates as (
    select
        genre as genre_name,

        -- Track counts
        count(*) as track_count,
        count(distinct artist_names_raw) as artist_count,

        -- Popularity metrics
        round(avg(popularity_score)::numeric, 2) as avg_popularity,
        round(stddev(popularity_score)::numeric, 2) as stddev_popularity,
        max(popularity_score) as max_popularity,
        min(popularity_score) as min_popularity,

        -- Streaming metrics
        sum(streaming_counts) as total_streams,
        round(avg(streaming_counts)::numeric, 0) as avg_streams_per_track,

        -- Audio feature averages
        round(avg(danceability)::numeric, 4) as avg_danceability,
        round(avg(energy)::numeric, 4) as avg_energy,
        round(avg(valence)::numeric, 4) as avg_valence,
        round(avg(instrumentalness)::numeric, 4) as avg_instrumentalness,
        round(avg(tempo_bpm)::numeric, 2) as avg_tempo,
        round(avg(loudness_db)::numeric, 2) as avg_loudness,
        round(avg(mood_score)::numeric, 4) as avg_mood_score,

        -- Distribution percentages
        round(
            sum(case when energy_category = 'High Energy' then 1 else 0 end)::numeric / count(*),
            4
        ) as pct_high_energy,
        round(
            sum(case when danceability_category in ('Very Danceable', 'Danceable') then 1 else 0 end)::numeric / count(*),
            4
        ) as pct_danceable,
        round(
            sum(case when valence_category in ('Very Positive', 'Positive') then 1 else 0 end)::numeric / count(*),
            4
        ) as pct_positive_mood,

        -- Hit rate (top 10% in popularity)
        round(
            sum(case when is_genre_hit then 1 else 0 end)::numeric / count(*),
            4
        ) as hit_rate,

        -- Time distribution
        mode() within group (order by release_decade) as dominant_decade,
        count(distinct release_year) as years_active

    from tracks
    where genre is not null
    group by genre
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['genre_name']) }} as genre_sk,

        -- Attributes
        genre_name,
        track_count,
        artist_count,

        -- Popularity
        avg_popularity,
        stddev_popularity,
        max_popularity,
        min_popularity,

        -- Streaming
        total_streams,
        avg_streams_per_track,

        -- Audio characteristics
        avg_danceability,
        avg_energy,
        avg_valence,
        avg_instrumentalness,
        avg_tempo,
        avg_loudness,
        avg_mood_score,

        -- Distribution metrics
        pct_high_energy,
        pct_danceable,
        pct_positive_mood,
        hit_rate,

        -- Time info
        dominant_decade,
        years_active,

        -- Genre categorization based on characteristics
        case
            when avg_energy > 0.7 and avg_danceability > 0.6 then 'High Energy Dance'
            when avg_instrumentalness > 0.5 then 'Instrumental'
            when avg_valence < 0.3 and avg_energy < 0.4 then 'Melancholic'
            when avg_danceability > 0.7 then 'Dance-Focused'
            when avg_energy < 0.4 then 'Chill/Ambient'
            else 'Balanced'
        end as genre_profile,

        -- Size tier
        case
            when track_count >= 5000 then 'Major Genre'
            when track_count >= 1000 then 'Established Genre'
            when track_count >= 100 then 'Niche Genre'
            else 'Micro Genre'
        end as genre_size_tier,

        -- Metadata
        current_timestamp as updated_at

    from genre_aggregates
)

select * from final
