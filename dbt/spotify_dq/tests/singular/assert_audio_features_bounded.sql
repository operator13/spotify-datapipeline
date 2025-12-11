{#
    ==========================================================================
    Singular Test: Audio Features Bounded

    DIMENSION: Accuracy / Validity
    PURPOSE: Ensures all normalized audio features are within 0-1 range

    BUSINESS RULE:
    - Audio features (energy, danceability, instrumentalness, valence)
      are normalized scores between 0 and 1
    - Values outside this range indicate data corruption or processing errors

    RETURNS: Tracks with out-of-bounds audio features
    ==========================================================================
#}

with feature_violations as (
    -- Energy violations
    select
        track_id,
        track_name,
        'energy' as feature_name,
        energy as feature_value,
        case
            when energy < 0 then 'BELOW_ZERO'
            when energy > 1 then 'ABOVE_ONE'
        end as violation_type
    from {{ ref('stg_spotify__tracks') }}
    where energy < 0 or energy > 1

    union all

    -- Danceability violations
    select
        track_id,
        track_name,
        'danceability' as feature_name,
        danceability as feature_value,
        case
            when danceability < 0 then 'BELOW_ZERO'
            when danceability > 1 then 'ABOVE_ONE'
        end as violation_type
    from {{ ref('stg_spotify__tracks') }}
    where danceability < 0 or danceability > 1

    union all

    -- Instrumentalness violations
    select
        track_id,
        track_name,
        'instrumentalness' as feature_name,
        instrumentalness as feature_value,
        case
            when instrumentalness < 0 then 'BELOW_ZERO'
            when instrumentalness > 1 then 'ABOVE_ONE'
        end as violation_type
    from {{ ref('stg_spotify__tracks') }}
    where instrumentalness < 0 or instrumentalness > 1

    union all

    -- Valence violations
    select
        track_id,
        track_name,
        'valence' as feature_name,
        valence as feature_value,
        case
            when valence < 0 then 'BELOW_ZERO'
            when valence > 1 then 'ABOVE_ONE'
        end as violation_type
    from {{ ref('stg_spotify__tracks') }}
    where valence < 0 or valence > 1
)

select * from feature_violations
