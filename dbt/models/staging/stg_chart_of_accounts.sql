/*
    Staging: Chart of Accounts
    ==========================
    Master reference of all general ledger accounts.
    Each account has a unique code, a descriptive name, a category
    (asset, liability, equity, revenue, expense) and a normal side
    (debit or credit) that determines sign conventions.

    This is the single source of truth for account metadata.
    Journal posting rules (account_mapping) reference these codes,
    and the general ledger joins here for account attributes.
*/

with source as (
    select * from {{ source('data_lake', 'chart_of_accounts') }}
)

select
    account_code,
    account_name,
    account_category,   -- asset | liability | equity | revenue | expense
    normal_side         -- debit | credit
from source
