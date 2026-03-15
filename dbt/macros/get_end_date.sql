{% macro get_end_date(default='month_end') %}
    {%- set v = var('end_date', none) -%}

    {%- if v is not none and (v | trim) != '' -%}
        cast('{{ v }}' as date)

    {%- else -%}
        {%- if default == 'today' -%}
            cast(current_date as date)

        {%- else -%}
            cast(last_day(current_date) as date)

        {%- endif -%}

    {%- endif -%}
{% endmacro %}