{% macro delete_period(date_column='ride_date') %}
    {#
        Pre-hook macro: deletes rows from the target table
        where the date_column falls within the current run's
        reporting period (start_date … end_date).

        Used by incremental models (e.g. fct_general_ledger) to
        implement a safe delete+insert pattern:
          1. Pre-hook deletes the period's existing rows
          2. Model SELECT produces the fresh rows for that period
          3. dbt inserts them

        This ensures the period can be safely reprocessed without
        duplicating data, while leaving other periods untouched.

        Usage in a model config:
          pre_hook="{{ delete_period('ride_date') }}"
    #}

    delete from {{ this }}
    where {{ date_column }} >= {{ get_start_date() }}
      and {{ date_column }} <= {{ get_end_date() }}

{% endmacro %}