{{
    config(
        materialized='view',
        tags=['staging', 'daily']
    )
}}

{#
    Staging model for Spotify tracks

    Purpose:
    - Type casting and data type standardization
    - Column renaming to snake_case convention
    - Generate surrogate key for track identification
    - Parse release date into components
    - Add metadata columns for lineage tracking

    Source: Kaggle Spotify Music Analytics Dataset (2015-2025)
    Records: ~85,000 tracks
#}

with source as (
    select * from {{ source('spotify_raw', 'tracks') }}
),

-- Clean and standardize the data
renamed as (
    select
        -- =================================================================
        -- Surrogate Key (generated from track attributes)
        -- =================================================================
        {{ dbt_utils.generate_surrogate_key([
            'track_name',
            'artist_name',
            'album_name'
        ]) }} as track_id,

        -- =================================================================
        -- Track Information
        -- =================================================================
        trim(track_name) as track_name,
        trim(artist_name) as artist_names_raw,
        trim(album_name) as album_name,
        trim(genre) as genre,
        trim(country) as country,
        trim(label) as record_label,

        -- =================================================================
        -- Release Date Parsing
        -- =================================================================
        release_date as release_date_raw,

        -- Try to extract year from various date formats
        case
            when release_date ~ '^\d{4}-\d{2}-\d{2}$' then
                cast(substring(release_date from 1 for 4) as integer)
            when release_date ~ '^\d{4}$' then
                cast(release_date as integer)
            else null
        end as release_year,

        -- Extract month if full date format
        case
            when release_date ~ '^\d{4}-\d{2}-\d{2}$' then
                cast(substring(release_date from 6 for 2) as integer)
            else null
        end as release_month,

        -- =================================================================
        -- Audio Features (Normalized 0-1)
        -- =================================================================
        cast(energy as numeric(5,4)) as energy,
        cast(danceability as numeric(5,4)) as danceability,
        cast(instrumentalness as numeric(5,4)) as instrumentalness,
        -- Note: valence not in dataset, using derived value
        cast((energy + danceability) / 2 as numeric(5,4)) as valence,

        -- =================================================================
        -- Audio Attributes
        -- =================================================================
        cast(tempo as numeric(6,2)) as tempo_bpm,
        cast(loudness as numeric(6,2)) as loudness_db,

        -- =================================================================
        -- Metrics
        -- =================================================================
        cast(popularity as integer) as popularity_score,
        cast(stream_count as bigint) as streaming_counts,

        -- =================================================================
        -- Derived: Categorizations
        -- =================================================================
        -- Popularity Tier
        case
            when cast(popularity as integer) >= 80 then 'Very Popular'
            when cast(popularity as integer) >= 60 then 'Popular'
            when cast(popularity as integer) >= 40 then 'Moderate'
            when cast(popularity as integer) >= 20 then 'Low'
            else 'Very Low'
        end as popularity_tier,

        -- Tempo Category
        case
            when cast(tempo as numeric) < 80 then 'Slow'
            when cast(tempo as numeric) < 120 then 'Moderate'
            when cast(tempo as numeric) < 160 then 'Fast'
            else 'Very Fast'
        end as tempo_category,

        -- Energy Category
        case
            when cast(energy as numeric) < 0.33 then 'Low Energy'
            when cast(energy as numeric) < 0.66 then 'Medium Energy'
            else 'High Energy'
        end as energy_category,

        -- Mood Score (derived from energy and danceability since valence not in dataset)
        round((cast(energy as numeric) + cast(danceability as numeric)) / 2, 4) as mood_score,

        -- =================================================================
        -- Metadata
        -- =================================================================
        _loaded_at,
        current_timestamp as _dbt_loaded_at,
        '{{ invocation_id }}' as _dbt_run_id

    from source
    where track_name is not null  -- Filter out records with no track name
)

select * from renamed
