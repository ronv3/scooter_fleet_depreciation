/*
    Staging: rides
    ==============
    Clean types, add ride_date, normalise nulls for coupon fields.

    Filters to the current reporting period using the start_date / end_date
    vars.  When running via Airflow the DAG passes explicit dates;
    when running ad-hoc the macros default to the current calendar month.
*/

with source as (
    select * from {{ source('data_lake', 'rides') }}
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
        country

    from source
    where cast(start_time as date) >= {{ get_start_date() }}
      and cast(start_time as date) <= {{ get_end_date() }}
)

select * from staged
