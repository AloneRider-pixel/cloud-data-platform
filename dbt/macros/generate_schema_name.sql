-- Macros for common patterns

{% macro cents_to_dollars(column_name, precision=2) -%}
    round({{ column_name }} / 100.0, {{ precision }})
{%- endmacro %}

{% macro safe_divide(numerator, denominator, default=0) -%}
    case 
        when {{ denominator }} = 0 or {{ denominator }} is null then {{ default }}
        else {{ numerator }} / {{ denominator }}
    end
{%- endmacro %}

{% macro date_spine(start_date, end_date) -%}
    with days as (
        select generate_series(
            '{{ start_date }}'::date,
            '{{ end_date }}'::date,
            '1 day'::interval
        ) as date_day
    )
    select cast(date_day as date) as date_day from days
{%- endmacro %}

{% macro audit_columns() -%}
    , current_timestamp as _dbt_loaded_at
    , '{{ invocation_id }}' as _dbt_invocation_id
{%- endmacro %}
