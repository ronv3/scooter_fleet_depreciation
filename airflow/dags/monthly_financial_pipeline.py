"""
Monthly Financial Pipeline DAG
==============================
Runs on the 1st of each month and processes the PREVIOUS month's data.

The DAG is organised into TaskGroups that mirror the dbt model layers:

  generate_source_data
    └─► seed
          ├── seed_rides           (dynamic – regenerated every month)
          └── seed_reference_data  (static – chart_of_accounts + account_mapping)
                └─► staging
                      ├── run
                      └── test
                            └─► intermediate
                                  ├── run
                                  └── test   (journal‑entry balance assertion)
                                        └─► marts
                                              ├── run_general_ledger
                                              ├── test_general_ledger  (GL balance assertion)
                                              ├── run_trial_balance    (after GL tests pass)
                                              └── test_trial_balance
                                                    └─► reports
                                                          ├── run
                                                          └── test   (balance‑sheet equation)

Each layer runs its own `dbt run --select` followed by `dbt test --select`
so that failures surface at the earliest possible stage.

Backfill: trigger with a custom execution_date or use
  `airflow dags backfill` to reprocess historical months.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


# ── Paths inside the container (mapped via compose.yml volumes) ──────────────
SCRIPTS_DIR = "/opt/scripts"
DBT_DIR = "/opt/dbt"
DBT_PROFILES_DIR = "/opt/dbt"          # profiles.yml lives here


# ── Jinja templates for the period dates ─────────────────────────────────────
# Airflow's execution_date represents the START of the period being processed.
# For a monthly DAG triggered on Feb 1, execution_date = Jan 1.
PERIOD_START = "{{ ds }}"
PERIOD_END = (
    "{{ (execution_date"
    " + macros.dateutil.relativedelta.relativedelta(months=1, days=-1))"
    ".strftime('%Y-%m-%d') }}"
)
# end_date exclusive for the Python script (first day of next month)
PERIOD_END_EXCLUSIVE = (
    "{{ (execution_date"
    " + macros.dateutil.relativedelta.relativedelta(months=1))"
    ".strftime('%Y-%m-%d') }}"
)

# ── Shared dbt CLI fragments ─────────────────────────────────────────────────
DBT_COMMON = f"cd {DBT_DIR} && dbt"
DBT_PROFILES = f"--profiles-dir {DBT_PROFILES_DIR}"
DBT_VARS = (
    "--vars '{\"start_date\": \""
    + PERIOD_START
    + "\", \"end_date\": \""
    + PERIOD_END
    + "\"}'"
)


# ── Helper factories ─────────────────────────────────────────────────────────

def dbt_run_task(task_id: str, select: str, **kwargs) -> BashOperator:
    """Create a BashOperator that runs `dbt run --select <select>`."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"{DBT_COMMON} run {DBT_PROFILES} --select {select} {DBT_VARS}",
        **kwargs,
    )


def dbt_test_task(task_id: str, select: str, **kwargs) -> BashOperator:
    """Create a BashOperator that runs `dbt test --select <select>`."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"{DBT_COMMON} test {DBT_PROFILES} --select {select} {DBT_VARS}",
        **kwargs,
    )


# ── DAG definition ───────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="monthly_financial_pipeline",
    default_args=default_args,
    description=(
        "Monthly: generate → seed → staging → intermediate → marts → reports "
        "(layer-by-layer with tests)"
    ),
    schedule_interval="0 6 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=True,
    max_active_runs=1,                  # one month at a time (DuckDB lock safety)
    tags=["financial", "dbt", "monthly"],
) as dag:

    # ── Step 1: Generate source data ─────────────────────────────────────────
    generate_source_data = BashOperator(
        task_id="generate_source_data",
        bash_command=(
            f"cd /opt && python {SCRIPTS_DIR}/create_source_data.py "
            f"--start-date {PERIOD_START} "
            f"--end-date {PERIOD_END_EXCLUSIVE}"
        ),
    )

    # ── Step 2: Seed ─────────────────────────────────────────────────────────
    with TaskGroup("seed") as seed_group:
        seed_rides = BashOperator(
            task_id="seed_rides",
            bash_command=(
                f"{DBT_COMMON} seed {DBT_PROFILES} --select rides"
            ),
        )
        seed_reference_data = BashOperator(
            task_id="seed_reference_data",
            bash_command=(
                f"{DBT_COMMON} seed {DBT_PROFILES} "
                "--select chart_of_accounts account_mapping"
            ),
        )

    # ── Step 3: Staging layer ────────────────────────────────────────────────
    with TaskGroup("staging") as staging_group:
        run_staging = dbt_run_task(
            "run",
            select="staging",
        )
        test_staging = dbt_test_task(
            "test",
            select="staging",
        )
        run_staging >> test_staging

    # ── Step 4: Intermediate layer ───────────────────────────────────────────
    with TaskGroup("intermediate") as intermediate_group:
        run_intermediate = dbt_run_task(
            "run",
            select="intermediate",
        )
        test_intermediate = dbt_test_task(
            "test",
            select="intermediate",
        )
        run_intermediate >> test_intermediate

    # ── Step 5: Marts layer (GL → Trial Balance, each tested) ────────────────
    with TaskGroup("marts") as marts_group:
        run_gl = dbt_run_task(
            "run_general_ledger",
            select="fct_general_ledger",
        )
        test_gl = dbt_test_task(
            "test_general_ledger",
            select="fct_general_ledger",
        )
        run_tb = dbt_run_task(
            "run_trial_balance",
            select="fct_trial_balance",
        )
        test_tb = dbt_test_task(
            "test_trial_balance",
            select="fct_trial_balance",
        )
        run_gl >> test_gl >> run_tb >> test_tb

    # ── Step 6: Reports layer ────────────────────────────────────────────────
    with TaskGroup("reports") as reports_group:
        run_reports = dbt_run_task(
            "run",
            select="reports",
        )
        test_reports = dbt_test_task(
            "test",
            select="reports",
        )
        run_reports >> test_reports

    # ── Cross-group dependencies ─────────────────────────────────────────────
    generate_source_data >> seed_group >> staging_group >> intermediate_group >> marts_group >> reports_group
