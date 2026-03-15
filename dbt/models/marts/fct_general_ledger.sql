/*
    Mart: General Ledger
    ====================
    The single source of truth for all financial reporting.

    Materialized as INCREMENTAL with a delete+insert strategy:
      1. Pre-hook deletes existing rows for the current reporting period
      2. Model SELECT produces fresh journal lines for that period
      3. dbt appends them to the table

    This makes the GL append-only across periods: once a period is
    processed and the next period begins, earlier data is untouched.
    Reprocessing the same period (e.g. corrections) safely replaces
    only that period's rows via the pre-hook.

    Built by joining journal entries with:
      1. Journal posting rules  — resolves line_type + country → account_code
      2. Chart of accounts      — resolves account_code → metadata

    INNER JOINs are used deliberately — if a posting rule or
    account is missing, the pipeline should fail loudly rather
    than silently produce NULL-attributed ledger rows.

    On first run (or --full-refresh): the table is created from scratch
    and the pre-hook is skipped (there is no existing table to delete from).
*/

{{
    config(
        materialized='incremental',
        unique_key='journal_entry_id',
        on_schema_change='append_new_columns',
        pre_hook=[
            "{{ delete_period('ride_date') }}"
        ] if is_incremental() else []
    )
}}

with journal as (
    select * from {{ ref('int_journal_entries') }}
),

posting_rules as (
    select * from {{ ref('stg_account_mapping') }}
),

chart_of_accounts as (
    select * from {{ ref('stg_chart_of_accounts') }}
),

ledger as (
    select
        j.journal_entry_id,
        j.order_id,
        j.ride_id,
        j.scooter_id,
        j.ride_date,

        -- Reporting period: first day of the ride's month
        -- Enables easy grouping / filtering by period downstream
        date_trunc('month', j.ride_date) as reporting_period,

        j.start_time,
        j.city,
        j.country,
        j.currency,

        j.line_type,
        j.line_number,
        j.entry_side,
        j.line_amount,
        j.coupon_code,

        -- Account code from posting rules
        pr.account_code,

        -- Account metadata from chart of accounts
        coa.account_name,
        coa.account_category,
        coa.normal_side,

        -- Signed amounts for easy aggregation:
        --   debit  → positive
        --   credit → negative
        case
            when j.entry_side = 'debit' then  j.line_amount
            when j.entry_side = 'credit' then -j.line_amount
        end as signed_amount,

        -- Audit metadata
        current_timestamp as loaded_at

    from journal j
    inner join posting_rules pr
        on  j.line_type = pr.line_type
        and j.country   = pr.country
    inner join chart_of_accounts coa
        on  pr.account_code = coa.account_code
)

select * from ledger
