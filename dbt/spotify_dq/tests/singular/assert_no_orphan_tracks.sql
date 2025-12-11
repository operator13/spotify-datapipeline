{#
    ==========================================================================
    Singular Test: No Orphan Tracks

    DIMENSION: Consistency
    PURPOSE: Ensures all tracks in fact table have valid dimension references

    BUSINESS RULE:
    - Every track with a genre should reference a valid dim_genres record
    - Orphaned records indicate dimension build issues or data inconsistency
    - Null genre_sk is acceptable (for tracks without genre classification)

    RETURNS: Tracks with invalid genre references
    ==========================================================================
#}

select
    f.track_id,
    f.track_name,
    f.genre,
    f.genre_sk,
    'ORPHAN_GENRE_FK' as violation_type,
    'Track references non-existent genre dimension record' as violation_reason
from {{ ref('fct_tracks') }} f
left join {{ ref('dim_genres') }} g
    on f.genre_sk = g.genre_sk
where f.genre_sk is not null
  and g.genre_sk is null
