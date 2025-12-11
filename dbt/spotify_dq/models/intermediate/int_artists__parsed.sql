{{
    config(
        materialized='ephemeral',
        tags=['intermediate']
    )
}}

{#
    Intermediate: Artist Parsing and Aggregation

    Purpose:
    - Parse multi-artist strings into individual artists
    - Create unique artist records
    - Calculate artist-level metrics (track count, avg popularity)
    - Assign artist tiers based on performance

    Business Rule:
    - Artists may be separated by various delimiters (;, ,, &, feat., ft., featuring)
    - Each unique artist gets one record with aggregated metrics
#}

with tracks as (
    select * from {{ ref('int_tracks__deduplicated') }}
),

-- Parse artist names from potentially delimited string
-- Handle common separators: semicolon, comma, ampersand, feat variations
artists_unnested as (
    select
        track_id,
        trim(
            regexp_replace(
                unnest(
                    string_to_array(
                        -- Replace common featuring patterns with semicolon
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(artist_names_raw, ' feat\.? ', ';', 'gi'),
                                ' ft\.? ', ';', 'gi'
                            ),
                            ' featuring ', ';', 'gi'
                        ),
                        ';'
                    )
                ),
                '^\s+|\s+$', '', 'g'  -- Trim whitespace
            )
        ) as artist_name,
        popularity_score,
        streaming_counts,
        genre,
        country,
        release_year
    from tracks
),

-- Filter out empty artist names
artists_cleaned as (
    select *
    from artists_unnested
    where artist_name is not null
      and trim(artist_name) != ''
      and length(artist_name) > 0
),

-- Create unique artist records with normalized name
unique_artists as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['lower(trim(artist_name))']) }} as artist_id,
        trim(artist_name) as artist_name,
        lower(trim(artist_name)) as artist_name_normalized
    from artists_cleaned
),

-- Calculate artist-level metrics
artist_metrics as (
    select
        ua.artist_id,
        ua.artist_name,
        ua.artist_name_normalized,

        -- Track counts
        count(distinct ac.track_id) as track_count,

        -- Popularity metrics
        round(avg(ac.popularity_score), 2) as avg_popularity,
        max(ac.popularity_score) as max_popularity,
        min(ac.popularity_score) as min_popularity,

        -- Streaming metrics
        sum(ac.streaming_counts) as total_streams,
        round(avg(ac.streaming_counts), 0) as avg_streams_per_track,

        -- Genre diversity
        count(distinct ac.genre) as genre_count,
        mode() within group (order by ac.genre) as primary_genre,

        -- Geographic presence
        count(distinct ac.country) as country_count,
        mode() within group (order by ac.country) as primary_country,

        -- Career span
        min(ac.release_year) as first_release_year,
        max(ac.release_year) as last_release_year,
        max(ac.release_year) - min(ac.release_year) as career_span_years

    from unique_artists ua
    left join artists_cleaned ac
        on lower(trim(ac.artist_name)) = ua.artist_name_normalized
    group by 1, 2, 3
),

-- Assign artist tiers
final as (
    select
        artist_id,
        artist_name,
        artist_name_normalized,
        track_count,
        avg_popularity,
        max_popularity,
        min_popularity,
        total_streams,
        avg_streams_per_track,
        genre_count,
        primary_genre,
        country_count,
        primary_country,
        first_release_year,
        last_release_year,
        career_span_years,

        -- Artist tier based on track count and popularity
        case
            when track_count >= 50 and avg_popularity >= 60 then 'Top Tier'
            when track_count >= 20 and avg_popularity >= 40 then 'Established'
            when track_count >= 5 and avg_popularity >= 20 then 'Rising'
            else 'Emerging'
        end as artist_tier,

        -- Streaming tier
        case
            when total_streams >= 1000000000 then 'Billion+'
            when total_streams >= 100000000 then '100M+'
            when total_streams >= 10000000 then '10M+'
            when total_streams >= 1000000 then '1M+'
            else 'Under 1M'
        end as streaming_tier

    from artist_metrics
)

select * from final
