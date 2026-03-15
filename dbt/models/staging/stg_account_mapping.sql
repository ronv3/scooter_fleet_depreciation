/*
    Staging: Journal Posting Rules (account_mapping)
    =================================================
    Defines which general-ledger account to hit for each
    combination of journal line type and country.

    For example: a 'revenue' line in Estonia posts to account 4101.

    This table does NOT carry account metadata (name, category,
    normal_side) — that belongs in the chart of accounts.
    The general ledger joins posting rules first (to resolve
    the account_code) and then the chart of accounts (to get
    the account attributes).
*/

with source as (
    select * from {{ source('data_lake', 'account_mapping') }}
)

select
    line_type,      -- receivable | revenue | vat_payable | coupon_expense
    country,        -- Estonia | Finland | Latvia
    account_code    -- the GL account to post to
from source
