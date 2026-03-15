/*
    Singular test: Balance sheet equation
    ======================================
    Assets = Liabilities + Equity must hold.
    If any row in the balance sheet has equation_balanced = false,
    the accounting equation is broken and the pipeline should fail.
*/

select
    section,
    account_code,
    account_name,
    balance,
    total_assets,
    total_liabilities,
    total_equity,
    liabilities_plus_equity
from {{ ref('rpt_balance_sheet') }}
where equation_balanced = false
