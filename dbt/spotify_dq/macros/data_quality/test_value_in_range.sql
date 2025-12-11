{#
    ==========================================================================
    Custom Generic Test: Value in Range

    DIMENSION: Accuracy
    PURPOSE: Validates that numeric values fall within expected bounds
             with optional null handling

    PARAMETERS:
        - model: The model to test
        - column_name: The column to validate
        - min_value: Minimum allowed value
        - max_value: Maximum allowed value
        - allow_null: Whether to allow null values (default true)

    USAGE:
        tests:
          - value_in_range:
              column_name: popularity_score
              min_value: 0
              max_value: 100
              allow_null: false

    RETURNS: Rows where values are outside the specified range
    ==========================================================================
#}

{% test value_in_range(model, column_name, min_value, max_value, allow_null=true) %}

with validation as (
    select
        {{ column_name }} as tested_value,
        {{ min_value }} as min_bound,
        {{ max_value }} as max_bound,
        case
            when {{ column_name }} is null then 'NULL_VALUE'
            when {{ column_name }} < {{ min_value }} then 'BELOW_MIN'
            when {{ column_name }} > {{ max_value }} then 'ABOVE_MAX'
            else 'VALID'
        end as validation_status
    from {{ model }}
)

select
    tested_value,
    min_bound,
    max_bound,
    validation_status as failure_reason
from validation
where validation_status != 'VALID'
{% if allow_null %}
  and validation_status != 'NULL_VALUE'
{% endif %}

{% endtest %}
