/*
    Report: Income Statement (Profit & Loss)
    =========================================
    Revenue minus expenses = net income for the current reporting period.

    This is a PERIOD statement: it shows activity only during the
    period defined by start_date / end_date vars (defaults to
    current calendar month).

    Uses net_balance from the trial balance (which is grouped by
    reporting_period) rather than raw debit/credit columns.  This
    is more robust against contra-entries or corrections.

    Sign convention:
      Revenue:  net_balance is negative (credit-normal) → flip to positive
      Expenses: net_balance is positive (debit-normal)  → show as-is
*/

with trial_balance as (
    select *
    from {{ ref('fct_trial_balance') }}
    where reporting_period >= date_trunc('month', {{ get_start_date() }})
      and reporting_period <= date_trunc('month', {{ get_end_date() }})
),

-- Revenue: credit-normal accounts → net_balance is negative → flip sign
revenue as (
    select
        account_code,
        account_name,
        country,
        -sum(net_balance) as amount
    from trial_balance
    where account_category = 'revenue'
    group by account_code, account_name, country
),

-- Expenses: debit-normal accounts → net_balance is positive → show as-is
expenses as (
    select
        account_code,
        account_name,
        country,
        sum(net_balance) as amount
    from trial_balance
    where account_category = 'expense'
    group by account_code, account_name, country
),

-- Assemble line items
line_items as (
    select
        1 as sort_order,
        'Revenue' as section,
        account_code,
        account_name,
        country,
        amount
    from revenue

    union all

    select
        2 as sort_order,
        'Expenses' as section,
        account_code,
        account_name,
        country,
        amount
    from expenses
)

select
    {{ get_start_date() }} as period_start,
    {{ get_end_date() }}   as period_end,
    sort_order,
    section,
    account_code,
    account_name,
    country,
    amount,
    -- Running context: totals and net income
    sum(case when section = 'Revenue' then amount else 0 end)
        over () as total_revenue,
    sum(case when section = 'Expenses' then amount else 0 end)
        over () as total_expenses,
    sum(case when section = 'Revenue' then amount else -amount end)
        over () as net_income

from line_items
order by sort_order, account_code, country
