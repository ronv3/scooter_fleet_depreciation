"""
Monthly Financial Pipeline DAG
==============================
Runs on the 1st of each month and processes the PREVIOUS month's data.

Steps:
  1. generate_source_data  — Python script creates rides.csv for the period
  2. dbt_seed              — Load rides.csv into DuckDB (data_lake schema)
  3. dbt_run               — Execute all models with period vars:
                             staging → intermediate → GL (incremental) →
                             trial balance → reports
  4. dbt_test              — Validate data quality and accounting invariants

The DAG passes start_date / end_date as dbt vars so that:
  - stg_rides filters to the period
  - fct_general_ledger deletes+inserts only that period's rows
  - Reports scope to the correct period (income statement) or
    cumulative position (balance sheet)

Backfill: trigger with a custom execution_date or use
  `airflow dags backfill` to reprocess historical months.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# ── Paths inside the container (mapped via compose.yml volumes) ──
SCRIPTS_DIR = "/opt/scripts"
DBT_DIR = "/opt/dbt"
DBT_PROFILES_DIR = "/opt/dbt"          # profiles.yml lives here


# ── Jinja templates for the period dates ──
# Airflow's execution_date represents the START of the period being processed.
# For a monthly DAG triggered on Feb 1, execution_date = Jan 1.
PERIOD_START = "{{ ds }}"                                              # YYYY-MM-DD
PERIOD_END = "{{ (execution_date + macros.dateutil.relativedelta.relativedelta(months=1, days=-1)).strftime('%Y-%m-%d') }}"
# end_date exclusive for the Python script (first day of next month)
PERIOD_END_EXCLUSIVE = "{{ (execution_date + macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y-%m-%d') }}"


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="monthly_financial_pipeline",
    default_args=default_args,
    description="Monthly: generate source data → dbt seed → dbt run → dbt test",
    # Run on the 1st of every month at 06:00 UTC
    schedule_interval="0 6 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=True,                       # enable backfill for historical months
    max_active_runs=1,                  # one month at a time to avoid DuckDB locks
    tags=["financial", "dbt", "monthly"],
) as dag:

    # ── Step 1: Generate source data for the period ──
    generate_source_data = BashOperator(
        task_id="generate_source_data",
        bash_command=(
            f"cd /opt && python {SCRIPTS_DIR}/create_source_data.py "
            f"--start-date {PERIOD_START} "
            f"--end-date {PERIOD_END_EXCLUSIVE}"
        ),
    )

    # ── Step 2: Load rides.csv into DuckDB via dbt seed ──
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"cd {DBT_DIR} && dbt seed "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--select rides"               # only re-seed rides; reference data is static
        ),
    )

    # ── Step 3: Run all dbt models with period vars ──
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_DIR} && dbt run "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--vars '{{\"start_date\": \"{PERIOD_START}\", \"end_date\": \"{PERIOD_END}\"}}'"
        ),
    )

    # ── Step 4: Run dbt tests ──
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && dbt test "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--vars '{{\"start_date\": \"{PERIOD_START}\", \"end_date\": \"{PERIOD_END}\"}}'"
        ),
    )

    # ── Task dependencies ──
    generate_source_data >> dbt_seed >> dbt_run >> dbt_test
