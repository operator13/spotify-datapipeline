{#
    ==========================================================================
    Singular Test: Streaming Counts Positive

    DIMENSION: Validity / Accuracy
    PURPOSE: Ensures streaming counts are non-negative

    BUSINESS RULE:
    - Streaming counts represent the number of times a track was played
    - Negative values are logically impossible and indicate data errors
    - Zero is valid (new or unpopular tracks)

    RETURNS: Tracks with negative streaming counts
    ==========================================================================
#}

select
    track_id,
    track_name,
    streaming_counts,
    'NEGATIVE_STREAMS' as violation_type,
    'Streaming count cannot be negative' as violation_reason
from {{ ref('stg_spotify__tracks') }}
where streaming_counts < 0
