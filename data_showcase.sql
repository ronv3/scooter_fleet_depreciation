select * from data_lake.rides where order_id in ('4e271d26-2934-2d81-282a-c9be672c9740','bb325c17-bb5b-2ab3-519b-7c4f26c5e81a');

select * from data_warehouse.stg_rides where order_id in ('4e271d26-2934-2d81-282a-c9be672c9740','bb325c17-bb5b-2ab3-519b-7c4f26c5e81a');

select * from data_warehouse.stg_account_mapping where country = 'Estonia';

select * from data_warehouse.int_journal_entries where order_id in ('4e271d26-2934-2d81-282a-c9be672c9740','bb325c17-bb5b-2ab3-519b-7c4f26c5e81a') order by scooter_id;

select * from data_warehouse.stg_chart_of_accounts;

select * from data_warehouse.fct_general_ledger where order_id in ('4e271d26-2934-2d81-282a-c9be672c9740','bb325c17-bb5b-2ab3-519b-7c4f26c5e81a') order by scooter_id;

select * from data_warehouse.fct_trial_balance where date(reporting_period) = date('2026-02-01') and country = 'Estonia';

select * from data_warehouse.rpt_balance_sheet where report_date = '2026-02-28';

select * from data_warehouse.rpt_income_statement where period_end = '2026-02-28';