/*
    Staging: rides
    ==============
    Clean types, add ride_date, normalise nulls for coupon fields.

    Materialized as an INCREMENTAL TABLE (delete+insert per period)
    so that processed rides are persisted and auditable.  An auditor
    can query this table to see exactly which rides were included in
    any historical period's processing, with the exact cleaned values.

    Filters to the current reporting period using the start_date / end_date
    vars.  When running via Airflow the DAG passes explicit dates;
    when running ad-hoc the macros default to the current calendar month.

    Guardrails (defensive staging logic):
      1. NULL amount filter — rows with NULL financial amounts are excluded
         (simulates payment gateway timeouts / missing data).
      2. Outlier filter — rows where the amount exceeds 50 EUR are excluded.
         Normal rides cost at most ~8.70 EUR (35 min × 0.22 + 1.00 unlock);
         a 100× decimal-point error produces amounts > 100 EUR.
         The 50 EUR threshold is well above any legitimate ride cost while
         catching extreme outliers.
      3. Deduplication — when multiple rows share the same order_id (duplicate
         event delivery), only one copy is retained via ROW_NUMBER().

    These guardrails have NO effect on clean data: clean data contains no
    NULLs in financial columns, no amounts above 50 EUR, and no duplicate
    order_ids.
*/

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='append_new_columns',
        pre_hook=[
            "{{ delete_period('ride_date') }}"
        ] if is_incremental() else []
    )
}}

with source as (
    select * from {{ source('data_lake', 'rides') }}
),

-- Guardrail 1: filter rows with NULL financial amounts
-- (real-world analogue: payment gateway timeout / missing data)
not_null_filter as (
    select *
    from source
    where amount is not null
      and vat_amount is not null
      and sum_with_vat_amount is not null
),

-- Guardrail 2: filter extreme outlier amounts
-- Normal max ride cost: 35 min × 0.22 + 1.00 = 8.70 EUR
-- A 100× decimal error produces amounts > 100 EUR
-- Threshold of 50 EUR catches outliers with a wide safety margin
-- (real-world analogue: decimal point error in source system)
outlier_filter as (
    select *
    from not_null_filter
    where cast(amount as decimal(12,2)) <= 50.00
),

-- Guardrail 3: deduplicate by order_id
-- If duplicate events arrive (e.g. Kafka redelivery), keep only one copy
-- Deterministic: picks the first row by start_time, ride_id
-- (real-world analogue: duplicate event delivery from messaging systems)
deduplicated as (
    select *,
        row_number() over (
            partition by order_id
            order by start_time, ride_id
        ) as _dedup_rn
    from outlier_filter
),

staged as (
    select
        order_id,
        ride_id,
        scooter_id,

        cast(start_time as timestamp) as start_time,
        cast(end_time   as timestamp) as end_time,
        cast(start_time as date)      as ride_date,

        duration_min,
        distance_km,

        -- financials
        cast(amount             as decimal(12,2)) as amount,
        cast(vat_rate           as decimal(5,4))  as vat_rate,
        cast(vat_amount         as decimal(12,2)) as vat_amount,
        cast(sum_with_vat_amount as decimal(12,2)) as sum_with_vat_amount,
        currency,

        -- coupon (normalise NaN / empty → NULL, amount → 0 when no coupon)
        case
            when coupon_used is not null and coupon_used != '' and coupon_used != 'None'
            then coupon_used
        end as coupon_code,

        coalesce(
            case
                when coupon_used is not null and coupon_used != '' and coupon_used != 'None'
                then cast(coupon_amount as decimal(12,2))
            end,
            0
        ) as coupon_amount,

        city,
        country,

        -- Audit metadata
        current_timestamp as loaded_at

    from deduplicated
    where _dedup_rn = 1
      and cast(start_time as date) >= {{ get_start_date() }}
      and cast(start_time as date) <= {{ get_end_date() }}
)

select
    order_id, ride_id, scooter_id,
    start_time, end_time, ride_date,
    duration_min, distance_km,
    amount, vat_rate, vat_amount, sum_with_vat_amount, currency,
    coupon_code, coupon_amount,
    city, country,
    loaded_at
from staged
