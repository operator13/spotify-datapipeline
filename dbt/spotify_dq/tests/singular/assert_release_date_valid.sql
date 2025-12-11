{#
    ==========================================================================
    Singular Test: Release Date Validation

    DIMENSION: Timeliness / Validity
    PURPOSE: Ensures release dates are within the expected dataset range (2015-2025)

    BUSINESS RULE:
    - The Spotify dataset covers tracks from 2015 to 2025
    - Any release year outside this range indicates data quality issues
    - Null release years are allowed but flagged separately

    RETURNS: Tracks with invalid release years
    ==========================================================================
#}

select
    track_id,
    track_name,
    release_date_raw,
    release_year,
    case
        when release_year is null then 'NULL_YEAR'
        when release_year < 2015 then 'YEAR_TOO_EARLY'
        when release_year > 2025 then 'YEAR_TOO_LATE'
    end as violation_reason
from {{ ref('stg_spotify__tracks') }}
where release_year is not null
  and (release_year < 2015 or release_year > 2025)
