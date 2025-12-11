{{
    config(
        materialized='ephemeral',
        tags=['intermediate']
    )
}}

{#
    Intermediate: Track Deduplication

    Purpose:
    - Remove duplicate tracks based on name, artist, and album
    - Keep the record with highest popularity score
    - Flag potential duplicates for data quality monitoring

    Business Rule:
    - Duplicate = same track_name + artist_names_raw + album_name (case-insensitive)
    - Retain the most popular version when duplicates exist
#}

with tracks as (
    select * from {{ ref('stg_spotify__tracks') }}
),

-- Identify potential duplicates based on track characteristics
ranked_tracks as (
    select
        *,
        row_number() over (
            partition by
                lower(trim(track_name)),
                lower(trim(artist_names_raw)),
                lower(trim(coalesce(album_name, '')))
            order by
                popularity_score desc nulls last,
                streaming_counts desc nulls last,
                track_id
        ) as duplicate_rank,
        count(*) over (
            partition by
                lower(trim(track_name)),
                lower(trim(artist_names_raw)),
                lower(trim(coalesce(album_name, '')))
        ) as duplicate_count
    from tracks
),

-- Keep only the first occurrence (highest popularity)
deduplicated as (
    select
        track_id,
        track_name,
        artist_names_raw,
        album_name,
        genre,
        country,
        record_label,
        release_date_raw,
        release_year,
        release_month,
        energy,
        danceability,
        instrumentalness,
        valence,
        tempo_bpm,
        loudness_db,
        popularity_score,
        streaming_counts,
        popularity_tier,
        tempo_category,
        energy_category,
        mood_score,
        _loaded_at,
        _dbt_loaded_at,
        _dbt_run_id,

        -- Deduplication metadata
        duplicate_rank,
        duplicate_count,
        case when duplicate_count > 1 then true else false end as had_duplicates

    from ranked_tracks
    where duplicate_rank = 1
)

select * from deduplicated
