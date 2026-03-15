/*
    Report: Balance Sheet
    =====================
    Assets = Liabilities + Equity

    This is a POINT-IN-TIME statement: it shows the cumulative
    financial position as of the period end date.  All ledger
    data from the beginning of time through end_date is included.

    In our simplified model:
      - Assets      = Accounts Receivable (what customers owe / have paid)
      - Liabilities = VAT Payable (tax owed to government)
      - Equity      = Retained Earnings = cumulative Revenue − Expenses

    The balance sheet equation MUST hold:
      Assets = Liabilities + Equity
*/

with trial_balance as (
    -- Cumulative: everything up to and including the current period
    select *
    from {{ ref('fct_trial_balance') }}
    where reporting_period <= date_trunc('month', {{ get_end_date() }})
),

-- Sum across all periods to get cumulative balances
cumulative as (
    select
        account_code,
        account_name,
        account_category,
        normal_side,
        country,
        sum(net_balance) as net_balance
    from trial_balance
    group by
        account_code,
        account_name,
        account_category,
        normal_side,
        country
),

-- Assets: accounts with category = 'asset', normal_side = debit
assets as (
    select
        account_code,
        account_name,
        country,
        net_balance as balance   -- positive for debit-normal accounts
    from cumulative
    where account_category = 'asset'
),

-- Liabilities: accounts with category = 'liability', normal_side = credit
liabilities as (
    select
        account_code,
        account_name,
        country,
        -net_balance as balance  -- flip sign: credit-normal → positive
    from cumulative
    where account_category = 'liability'
),

-- Equity = Retained Earnings = cumulative Revenue - Expenses
-- Revenue: credit-normal (negative signed_amount) → flip to positive
-- Expenses: debit-normal (positive signed_amount) → subtract
retained_earnings as (
    select
        sum(case
            when account_category = 'revenue' then -net_balance
            when account_category = 'expense' then -net_balance
            else 0
        end) as balance
    from cumulative
    where account_category in ('revenue', 'expense')
),

-- Assemble the balance sheet
balance_sheet as (
    select 1 as sort_order, 'Assets' as section,
           account_code, account_name, country, balance
    from assets

    union all

    select 2 as sort_order, 'Liabilities' as section,
           account_code, account_name, country, balance
    from liabilities

    union all

    select 3 as sort_order, 'Equity' as section,
           '3000' as account_code,
           'Retained Earnings' as account_name,
           'ALL' as country,
           balance
    from retained_earnings
),

-- Validation: Assets = Liabilities + Equity
totals as (
    select
        sum(case when section = 'Assets'      then balance else 0 end) as total_assets,
        sum(case when section = 'Liabilities'  then balance else 0 end) as total_liabilities,
        sum(case when section = 'Equity'       then balance else 0 end) as total_equity
    from balance_sheet
)

select
    {{ get_end_date() }} as report_date,
    bs.*,
    t.total_assets,
    t.total_liabilities,
    t.total_equity,
    t.total_liabilities + t.total_equity as liabilities_plus_equity,
    abs(t.total_assets - (t.total_liabilities + t.total_equity)) < 0.01 as equation_balanced
from balance_sheet bs
cross join totals t
order by sort_order, account_code, country
