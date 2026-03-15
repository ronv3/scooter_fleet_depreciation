{% macro get_start_date() %}
    {%- set v = var('start_date', none) -%}

    {%- if v is not none and (v | trim) != '' -%}
        cast('{{ v }}' as date)

    {%- else -%}
        cast(date_trunc('month', current_date) as date)

    {%- endif -%}
{% endmacro %}