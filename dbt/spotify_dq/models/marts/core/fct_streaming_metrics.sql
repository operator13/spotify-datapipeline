{{
    config(
        materialized='table',
        tags=['marts', 'core', 'fact', 'aggregated']
    )
}}

{#
    Fact Table: Streaming Metrics (Aggregated)

    Purpose:
    - Pre-aggregated streaming metrics by genre, country, and year
    - Optimized for dashboard queries and trend analysis
    - Reduces query complexity for common analytical patterns

    Grain: One row per genre + country + year combination
#}

with tracks as (
    select * from {{ ref('fct_tracks') }}
),

-- Aggregate by genre, country, year
aggregated_metrics as (
    select
        -- Dimensions
        genre,
        country,
        release_year,

        -- Track counts
        count(*) as track_count,
        count(distinct artists) as artist_count,

        -- Streaming totals
        sum(streaming_counts) as total_streams,
        round(avg(streaming_counts), 0) as avg_streams_per_track,
        max(streaming_counts) as max_streams,
        min(streaming_counts) as min_streams,

        -- Popularity metrics
        round(avg(popularity_score), 2) as avg_popularity,
        round(stddev(popularity_score), 2) as stddev_popularity,
        max(popularity_score) as max_popularity,

        -- Audio feature averages
        round(avg(danceability), 4) as avg_danceability,
        round(avg(energy), 4) as avg_energy,
        round(avg(valence), 4) as avg_valence,
        round(avg(tempo_bpm), 2) as avg_tempo,
        round(avg(mood_score), 4) as avg_mood_score,

        -- Distribution counts
        sum(case when popularity_tier = 'Very Popular' then 1 else 0 end) as very_popular_count,
        sum(case when popularity_tier = 'Popular' then 1 else 0 end) as popular_count,
        sum(case when streaming_tier = 'Billion Club' then 1 else 0 end) as billion_club_count,
        sum(case when is_genre_hit then 1 else 0 end) as hit_count,

        -- Category distributions
        sum(case when energy_category = 'High Energy' then 1 else 0 end) as high_energy_count,
        sum(case when danceability_category in ('Very Danceable', 'Danceable') then 1 else 0 end) as danceable_count,
        sum(case when valence_category in ('Very Positive', 'Positive') then 1 else 0 end) as positive_mood_count

    from tracks
    where genre is not null
      and country is not null
      and release_year is not null
    group by genre, country, release_year
),

-- Add dimension keys
final as (
    select
        -- Surrogate key for this aggregate
        {{ dbt_utils.generate_surrogate_key(['genre', 'country', 'release_year']) }} as metric_sk,

        -- Dimension values
        am.genre,
        am.country,
        am.release_year,

        -- Lookup dimension keys
        g.genre_sk,
        c.country_sk,

        -- Metrics
        am.track_count,
        am.artist_count,
        am.total_streams,
        am.avg_streams_per_track,
        am.max_streams,
        am.min_streams,
        am.avg_popularity,
        am.stddev_popularity,
        am.max_popularity,

        -- Audio averages
        am.avg_danceability,
        am.avg_energy,
        am.avg_valence,
        am.avg_tempo,
        am.avg_mood_score,

        -- Counts
        am.very_popular_count,
        am.popular_count,
        am.billion_club_count,
        am.hit_count,
        am.high_energy_count,
        am.danceable_count,
        am.positive_mood_count,

        -- Calculated rates
        round(am.hit_count::numeric / nullif(am.track_count, 0), 4) as hit_rate,
        round(am.very_popular_count::numeric / nullif(am.track_count, 0), 4) as very_popular_rate,
        round(am.high_energy_count::numeric / nullif(am.track_count, 0), 4) as high_energy_rate,
        round(am.danceable_count::numeric / nullif(am.track_count, 0), 4) as danceable_rate,
        round(am.positive_mood_count::numeric / nullif(am.track_count, 0), 4) as positive_mood_rate,

        -- Metadata
        current_timestamp as updated_at

    from aggregated_metrics am
    left join {{ ref('dim_genres') }} g
        on am.genre = g.genre_name
    left join {{ ref('dim_countries') }} c
        on am.country = c.country_code
)

select * from final
