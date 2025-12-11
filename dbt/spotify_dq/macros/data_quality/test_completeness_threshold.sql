{#
    ==========================================================================
    Custom Generic Test: Completeness Threshold

    DIMENSION: Completeness
    PURPOSE: Validates that a column has at least the specified percentage
             of non-null values

    PARAMETERS:
        - model: The model to test
        - column_name: The column to check for nulls
        - threshold: Minimum acceptable completeness ratio (default 0.95 = 95%)

    USAGE:
        tests:
          - completeness_threshold:
              column_name: track_name
              threshold: 0.95

    RETURNS: Rows where completeness is below threshold (test fails if any rows)
    ==========================================================================
#}

{% test completeness_threshold(model, column_name, threshold=0.95) %}

with validation as (
    select
        count(*) as total_rows,
        count({{ column_name }}) as non_null_rows,
        count({{ column_name }})::float / nullif(count(*), 0) as completeness_ratio
    from {{ model }}
),

failed_check as (
    select
        total_rows,
        non_null_rows,
        completeness_ratio,
        {{ threshold }} as threshold_value,
        'Completeness below threshold' as failure_reason
    from validation
    where completeness_ratio < {{ threshold }}
)

select * from failed_check

{% endtest %}
