/*
    Mart: Trial Balance
    ===================
    Aggregate the general ledger into account-level totals,
    broken down by reporting_period (month).

    Rebuilt fully on every run from the entire GL. Since the GL
    is incremental and accumulates data across periods, this
    trial balance always reflects the complete picture.

    The trial balance must satisfy per period:
      total debits = total credits

    Downstream reports filter this table:
      - Income statement: current period only
      - Balance sheet:    cumulative through current period end
*/

with ledger as (
    select * from {{ ref('fct_general_ledger') }}
),

aggregated as (
    select
        reporting_period,
        account_code,
        account_name,
        account_category,
        normal_side,
        country,

        sum(case when entry_side = 'debit'  then line_amount else 0 end) as total_debit,
        sum(case when entry_side = 'credit' then line_amount else 0 end) as total_credit,
        sum(signed_amount) as net_balance,

        count(*) as entry_count

    from ledger
    group by
        reporting_period,
        account_code,
        account_name,
        account_category,
        normal_side,
        country
)

select * from aggregated
order by reporting_period, account_code, country
