{#
    ==========================================================================
    Custom Generic Test: Freshness SLA

    DIMENSION: Timeliness
    PURPOSE: Validates that the most recent record is within the SLA window

    PARAMETERS:
        - model: The model to test
        - column_name: Timestamp column to check (e.g., _dbt_loaded_at)
        - max_hours: Maximum allowed hours since last update (default 24)

    USAGE:
        tests:
          - freshness_sla:
              column_name: _dbt_loaded_at
              max_hours: 24

    RETURNS: Row if data is stale (beyond SLA), empty if fresh
    ==========================================================================
#}

{% test freshness_sla(model, column_name, max_hours=24) %}

with latest_record as (
    select
        max({{ column_name }}) as max_timestamp
    from {{ model }}
),

sla_check as (
    select
        max_timestamp,
        current_timestamp as check_timestamp,
        extract(epoch from (current_timestamp - max_timestamp)) / 3600 as hours_since_update,
        {{ max_hours }} as sla_hours
    from latest_record
)

select
    max_timestamp as last_update,
    check_timestamp,
    round(hours_since_update::numeric, 2) as hours_stale,
    sla_hours as max_allowed_hours,
    'Data freshness SLA violation' as failure_reason
from sla_check
where hours_since_update > {{ max_hours }}

{% endtest %}
