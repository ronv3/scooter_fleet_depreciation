/*
    Staging: rides quarantine
    =========================
    Captures all rows rejected by the stg_rides guardrails, tagged with
    the reason for rejection.  This table serves as an operational data
    quality monitor: analytics teams can track rejection volumes over
    time, investigate individual records, and identify upstream issues.

    The guardrails in stg_rides apply in a cascade:
      1. NULL financial amounts  →  rejected first
      2. Outlier amounts (> 50 EUR)  →  rejected from non-null rows
      3. Duplicates (row_number > 1)  →  rejected from clean, non-outlier rows

    A row appears here exactly once, tagged with the *first* guardrail
    that would have excluded it.  This mirrors the cascade order in
    stg_rides, so the two tables together account for every source row
    in the reporting period.

    Materialized as an incremental table (delete+insert per period) so
    that historical quarantine data is preserved for trend analysis.
*/

{{
    config(
        materialized='incremental',
        unique_key='quarantine_id',
        on_schema_change='append_new_columns',
        pre_hook=[
            "{{ delete_period('ride_date') }}"
        ] if is_incremental() else []
    )
}}

with source as (
    select * from {{ source('data_lake', 'rides') }}
    where cast(start_time as date) >= {{ get_start_date() }}
      and cast(start_time as date) <= {{ get_end_date() }}
),

-- ── Category 1: NULL financial amounts ──────────────────────────────────
null_rejects as (
    select
        order_id, ride_id, scooter_id, start_time, end_time,
        duration_min, distance_km,
        amount, vat_rate, vat_amount, sum_with_vat_amount, currency,
        coupon_used, coupon_amount, city, country,
        'null_financial_amount' as rejection_reason
    from source
    where amount is null
       or vat_amount is null
       or sum_with_vat_amount is null
),

-- Rows that pass the NULL filter (needed for downstream categories)
not_null_rows as (
    select *
    from source
    where amount is not null
      and vat_amount is not null
      and sum_with_vat_amount is not null
),

-- ── Category 2: Outlier amounts ─────────────────────────────────────────
outlier_rejects as (
    select
        order_id, ride_id, scooter_id, start_time, end_time,
        duration_min, distance_km,
        amount, vat_rate, vat_amount, sum_with_vat_amount, currency,
        coupon_used, coupon_amount, city, country,
        'outlier_amount' as rejection_reason
    from not_null_rows
    where cast(amount as decimal(12,2)) > 50.00
),

-- Rows that pass both NULL and outlier filters
clean_rows as (
    select *
    from not_null_rows
    where cast(amount as decimal(12,2)) <= 50.00
),

-- ── Category 3: Duplicates ──────────────────────────────────────────────
deduplicated as (
    select *,
        row_number() over (
            partition by order_id
            order by start_time, ride_id
        ) as _dedup_rn
    from clean_rows
),

duplicate_rejects as (
    select
        order_id, ride_id, scooter_id, start_time, end_time,
        duration_min, distance_km,
        amount, vat_rate, vat_amount, sum_with_vat_amount, currency,
        coupon_used, coupon_amount, city, country,
        'duplicate_order' as rejection_reason
    from deduplicated
    where _dedup_rn > 1
),

-- ── Combine all rejected rows ───────────────────────────────────────────
all_rejects as (
    select * from null_rejects
    union all
    select * from outlier_rejects
    union all
    select * from duplicate_rejects
),

-- Assign a sequence number within each (order_id, ride_id, reason) group
-- so that duplicated rows with identical attributes get distinct IDs
numbered_rejects as (
    select *,
        row_number() over (
            partition by order_id, ride_id, rejection_reason
            order by start_time
        ) as _reject_rn
    from all_rejects
)

select
    -- Deterministic surrogate key for incremental unique_key
    -- Includes _reject_rn to guarantee uniqueness when the same
    -- order_id+ride_id appears multiple times (e.g. a duplicated
    -- row that also has a NULL amount)
    md5(cast(coalesce(cast(order_id as varchar), '_null_')
        || '-' || coalesce(cast(ride_id as varchar), '_null_')
        || '-' || rejection_reason
        || '-' || cast(_reject_rn as varchar) as varchar))
        as quarantine_id,

    order_id,
    ride_id,
    scooter_id,

    cast(start_time as timestamp) as start_time,
    cast(end_time   as timestamp) as end_time,
    cast(start_time as date)      as ride_date,

    duration_min,
    distance_km,

    -- Preserve original (potentially corrupt) financial values as-is
    amount          as original_amount,
    vat_rate        as original_vat_rate,
    vat_amount      as original_vat_amount,
    sum_with_vat_amount as original_sum_with_vat_amount,
    currency,

    coupon_used     as original_coupon_used,
    coupon_amount   as original_coupon_amount,

    city,
    country,

    rejection_reason,
    current_timestamp as quarantined_at

from all_rejects
