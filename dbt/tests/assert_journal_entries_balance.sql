/*
    Singular test: Double-entry balance per order
    ==============================================
    For every order_id, the sum of debits must equal the sum of credits.
    This is the fundamental invariant of double-entry bookkeeping.

    Any row returned by this query represents a broken journal entry
    and should cause the pipeline to fail.
*/

with entry_totals as (
    select
        order_id,
        sum(case when entry_side = 'debit'  then line_amount else 0 end) as total_debit,
        sum(case when entry_side = 'credit' then line_amount else 0 end) as total_credit
    from {{ ref('int_journal_entries') }}
    group by order_id
)

select
    order_id,
    total_debit,
    total_credit,
    abs(total_debit - total_credit) as imbalance
from entry_totals
where abs(total_debit - total_credit) > 0.01
