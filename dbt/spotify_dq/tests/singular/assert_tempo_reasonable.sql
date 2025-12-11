{#
    ==========================================================================
    Singular Test: Tempo Reasonable

    DIMENSION: Accuracy
    PURPOSE: Ensures tempo values are within reasonable BPM range for music

    BUSINESS RULE:
    - Musical tempo typically ranges from 40 BPM (very slow) to 250 BPM (very fast)
    - Values outside this range likely indicate measurement errors
    - Extreme values may be valid for experimental music but should be reviewed

    RETURNS: Tracks with unreasonable tempo values
    ==========================================================================
#}

select
    track_id,
    track_name,
    tempo_bpm,
    case
        when tempo_bpm < 40 then 'TEMPO_TOO_LOW'
        when tempo_bpm > 250 then 'TEMPO_TOO_HIGH'
    end as violation_type,
    case
        when tempo_bpm < 40 then 'Tempo below 40 BPM is unusually slow'
        when tempo_bpm > 250 then 'Tempo above 250 BPM is unusually fast'
    end as violation_reason
from {{ ref('stg_spotify__tracks') }}
where tempo_bpm is not null
  and (tempo_bpm < 40 or tempo_bpm > 250)
