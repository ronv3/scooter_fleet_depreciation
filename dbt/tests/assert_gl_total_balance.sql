/*
    Singular test: General ledger total balance
    ============================================
    Across the entire general ledger, the sum of all signed_amount
    values must be zero (total debits = total credits globally).

    This validates the ledger as a whole, complementing the
    per-order test in assert_journal_entries_balance.
*/

with totals as (
    select
        sum(signed_amount) as net_signed_amount,
        sum(case when entry_side = 'debit'  then line_amount else 0 end) as total_debit,
        sum(case when entry_side = 'credit' then line_amount else 0 end) as total_credit
    from {{ ref('fct_general_ledger') }}
)

select
    total_debit,
    total_credit,
    net_signed_amount,
    abs(total_debit - total_credit) as imbalance
from totals
where abs(total_debit - total_credit) > 0.01
