{#
    ==========================================================================
    Custom Generic Test: Referential Integrity

    DIMENSION: Consistency
    PURPOSE: Validates that foreign key values exist in the parent table

    PARAMETERS:
        - model: The child model to test
        - column_name: The foreign key column in the child table
        - parent_model: Reference to the parent model
        - parent_column: The primary key column in the parent table
        - allow_null: Whether to allow null FK values (default true)

    USAGE:
        tests:
          - referential_integrity:
              column_name: genre_sk
              parent_model: ref('dim_genres')
              parent_column: genre_sk
              allow_null: true

    RETURNS: Rows with orphaned foreign keys (values not in parent)
    ==========================================================================
#}

{% test referential_integrity(model, column_name, parent_model, parent_column, allow_null=true) %}

with child_values as (
    select distinct
        {{ column_name }} as fk_value
    from {{ model }}
    {% if allow_null %}
    where {{ column_name }} is not null
    {% endif %}
),

parent_values as (
    select distinct
        {{ parent_column }} as pk_value
    from {{ parent_model }}
),

orphans as (
    select
        c.fk_value as orphaned_value,
        'Foreign key not found in parent table' as failure_reason
    from child_values c
    left join parent_values p
        on c.fk_value = p.pk_value
    where p.pk_value is null
)

select * from orphans

{% endtest %}
