# Data Pipeline for Scooter Fleet Financial Reporting

This repository contains the practical implementation for a Bachelor's thesis on automating financial reporting using data warehouse models. It takes synthetic scooter ride billing data and transforms it — through a layered dbt pipeline — into a fully auditable double-entry general ledger and standard financial statements (income statement, balance sheet).

The pipeline is designed to mirror real-world financial data warehouse patterns: separation of chart of accounts from journal posting rules, incremental ledger loading with period-based delete+insert, multi-country VAT handling, and automated validation of double-entry invariants at every layer.

---

## Tech Stack

| Component | Role |
|---|---|
| **Python** | Source data generation (`scripts/create_source_data.py`) |
| **DuckDB** | Analytical database (file-based, embedded — no separate server) |
| **dbt (dbt-duckdb)** | Transformation layer, testing, model materialization |
| **Apache Airflow** | Orchestration and monthly scheduling |
| **Docker Compose** | Reproducible local environment |
| **PostgreSQL** | Airflow metadata only (not the analytical warehouse) |

---

## Prerequisites

The only prerequisite is **Docker Desktop** running on your machine. All Python dependencies, dbt, and DuckDB run inside the container — no local installation needed.

---

## Setup

### 1. Clone and enter the project

```bash
cd scooter_financial_reporting
```

### 2. Create environment file

```bash
cp .env.example .env
```

The defaults work as-is for local development. Edit `.env` if you want to change Airflow credentials or ports.

### 3. Build and initialize

```bash
# Build the Docker images
docker compose build

# Initialize Airflow metadata DB and create admin user (wait for it to exit)
docker compose up airflow-init

# Start Airflow webserver and scheduler
docker compose up -d airflow-webserver airflow-scheduler
```

### 4. Verify the setup

```bash
# All three containers should be running
docker compose ps

# dbt should connect to DuckDB successfully
docker compose exec airflow-webserver bash -lc "cd /opt/dbt && dbt debug --profiles-dir /opt/dbt"
```

Expected output: `adapter type: duckdb`, connection test `OK`.

---

## Running the Full-Year Showcase

For the thesis demonstration, the recommended approach is to generate a full year of data (January–December 2026) and process it in one pass. This produces a complete set of financial statements with a meaningful volume of data.

### Step 1 — Generate source data for the full year

```bash
docker compose exec airflow-webserver bash -lc \
  "cd /opt && python scripts/create_source_data.py --start-date 2026-01-01 --end-date 2027-01-01"
```

This produces approximately 134,000 ride records across 12 months, 3 countries, and 90 scooters. The script prints a validation summary — verify all checks pass before continuing.

### Step 2 — Seed into DuckDB

```bash
docker compose exec airflow-webserver bash -lc \
  "cd /opt/dbt && dbt seed --full-refresh --profiles-dir /opt/dbt"
```

### Step 3 — Run the full pipeline

```bash
docker compose exec airflow-webserver bash -lc \
  "cd /opt/dbt && dbt run --profiles-dir /opt/dbt --vars '{start_date: \"2026-01-01\", end_date: \"2026-12-31\"}'"
```

### Step 4 — Run all tests

```bash
docker compose exec airflow-webserver bash -lc \
  "cd /opt/dbt && dbt test --profiles-dir /opt/dbt --vars '{start_date: \"2026-01-01\", end_date: \"2026-12-31\"}'"
```

### Step 5 — Query the results

If DuckDB CLI is installed locally (`brew install duckdb`):

```bash
duckdb duckdb/thesis.duckdb
```

```sql
-- Full-year income statement (account-level detail)
SELECT * FROM data_warehouse.rpt_income_statement_detail ORDER BY sort_order, account_code;

-- Period-level P&L summary
SELECT * FROM data_warehouse.rpt_income_statement_summary;

-- Balance sheet detail as of December 31, 2026
SELECT * FROM data_warehouse.rpt_balance_sheet_detail ORDER BY sort_order, account_code;

-- Balance sheet summary with equation check
SELECT * FROM data_warehouse.rpt_balance_sheet_summary;

-- General ledger row count
SELECT COUNT(*) FROM data_warehouse.fct_general_ledger;

-- Trial balance by period — verify debits = credits per month
SELECT
    reporting_period,
    SUM(total_debit)  AS total_debit,
    SUM(total_credit) AS total_credit,
    ABS(SUM(total_debit) - SUM(total_credit)) < 0.001 AS balanced
FROM data_warehouse.fct_trial_balance
GROUP BY reporting_period
ORDER BY reporting_period;
```

---

## Monthly Workflow (Simulating Production)

In a production setting, the pipeline runs monthly via Airflow. Each run processes the previous month's ride data.

### Manual monthly run

```bash
# Generate February 2026 data
docker compose exec airflow-webserver bash -lc \
  "cd /opt && python scripts/create_source_data.py --start-date 2026-02-01 --end-date 2026-03-01"

# Seed and run
docker compose exec airflow-webserver bash -lc \
  "cd /opt/dbt && dbt seed --select rides --profiles-dir /opt/dbt"

docker compose exec airflow-webserver bash -lc \
  "cd /opt/dbt && dbt run --profiles-dir /opt/dbt --vars '{start_date: \"2026-02-01\", end_date: \"2026-02-28\"}'"

docker compose exec airflow-webserver bash -lc \
  "cd /opt/dbt && dbt test --profiles-dir /opt/dbt --vars '{start_date: \"2026-02-01\", end_date: \"2026-02-28\"}'"
```

### Automated via Airflow

The DAG `monthly_financial_pipeline` runs on the 1st of each month at 06:00 UTC. It processes the previous month by automatically calculating the period dates from Airflow's `execution_date`. Enable it in the Airflow UI at http://localhost:8080.

The DAG has `catchup=True`, meaning Airflow will backfill all months from the `start_date` (January 2026) to the current date when first enabled.

### Reprocessing a period

Because the general ledger uses a delete+insert strategy, rerunning any period is safe and idempotent. The pre-hook deletes that period's rows, then fresh rows are inserted. Other periods remain untouched.

---

## Data Flow: Source to Financial Statements

### Overview

```
create_source_data.py          Generates rides.csv for a given date range
        |
        v
dbt seed                       Loads CSV into DuckDB (data_lake.rides)
        |
        v
stg_rides                      Type casting, null normalization, period filtering
        |
        v
int_journal_entries             Explodes each ride into double-entry journal lines
        |
        v
fct_general_ledger              Joins with posting rules + chart of accounts
        |                       Incremental: delete+insert by period
        v
fct_trial_balance               Aggregates ledger by account + period
        |                       Incremental: delete+insert by period
       / \
      v   v
rpt_income_statement_detail  rpt_balance_sheet_detail
rpt_income_statement_summary rpt_balance_sheet_summary
```

### Layer-by-Layer Detail

**Source data generation** (`scripts/create_source_data.py`): A Python script generates synthetic ride billing records — one row per completed scooter ride with pricing breakdown (net amount, VAT, gross, coupon discount). The script accepts `--start-date` and `--end-date` arguments. Without arguments it defaults to the previous calendar month. All random generation uses a fixed seed (42) so the same date range always produces identical data.

**Staging** (`stg_rides`, `stg_account_mapping`, `stg_chart_of_accounts`): Defensive data cleaning. Types are cast explicitly (amounts to `DECIMAL(12,2)`, timestamps, etc.). Coupon fields are normalized (empty strings and `'None'` become SQL `NULL`, missing coupon amounts default to 0). `stg_rides` is materialized as an **incremental** table (delete+insert per period) so that the exact cleaned rides used in each period are persisted and auditable. The staging layer also applies the period filter — only rides within the `start_date`/`end_date` range flow downstream.

**Intermediate** (`int_journal_entries`): The core accounting transformation, materialized as an **incremental** table (delete+insert per period) for auditability. Each ride is cross-joined with a line-type spine to produce double-entry journal lines. A ride without a coupon produces 3 lines; a ride with a coupon produces 4. Zero-amount lines are filtered out (standard accounting practice — you do not post a EUR 0.00 entry). Each line receives a deterministic `journal_entry_id` (MD5 hash of `order_id` + `line_type`) for traceability.

**General Ledger** (`fct_general_ledger`): The single source of truth for all financial reporting. Journal lines are enriched with account metadata through a two-step join: first to the posting rules (resolving `line_type + country` to an `account_code`), then to the chart of accounts (resolving `account_code` to name, category, and normal side). The GL is materialized as an **incremental** table with a pre-hook that deletes the current period's rows before inserting fresh ones. This makes it append-only across periods while allowing safe reprocessing of any individual period.

**Trial Balance** (`fct_trial_balance`): Aggregates the general ledger by `reporting_period`, `account_code`, and `country`. Produces total debits, total credits, and net balance per account per month. Materialized as **incremental** (delete+insert per reporting period) — only the current period is re-aggregated while historical periods remain untouched, acting as a soft period close.

**Income Statement** (`rpt_income_statement_detail` + `rpt_income_statement_summary`): A period statement showing revenue minus expenses for the reporting period. Both models are materialized as **incremental** tables (delete+insert by `period_end`), so each monthly run adds that period's rows while previous periods remain untouched — building a historical series of income statements. The detail model provides account-level line items; the summary model produces one row per period with total revenue, total expenses, and net income.

**Balance Sheet** (`rpt_balance_sheet_detail` + `rpt_balance_sheet_summary`): A point-in-time statement showing cumulative financial position. Both models are materialized as **incremental** tables (delete+insert by `report_date`), so each monthly run adds a cumulative snapshot while previous snapshots remain untouched. The detail model provides account-level balances; the summary model produces one row per report date with the accounting equation validation (`equation_balanced` flag, threshold 0.001).

---

## Double-Entry Accounting Logic

### The Journal Entry for a Scooter Ride

Every completed ride produces a balanced set of journal entries. For a ride costing EUR 5.18 net + 24% VAT = EUR 6.42 gross, with no coupon:

```
DR  1200  Accounts Receivable       EUR 6.42
    CR  4101  Ride Revenue — Estonia     EUR 5.18
    CR  2101  VAT Payable — Estonia      EUR 1.24

Debits (6.42) = Credits (5.18 + 1.24 = 6.42)  ✓
```

With a EUR 3.00 coupon applied (customer pays EUR 3.42):

```
DR  1200  Accounts Receivable       EUR 3.42
DR  6200  Marketing Expense         EUR 3.00
    CR  4101  Ride Revenue — Estonia     EUR 5.18
    CR  2101  VAT Payable — Estonia      EUR 1.24

Debits (3.42 + 3.00 = 6.42) = Credits (5.18 + 1.24 = 6.42)  ✓
```

The mathematical invariant that guarantees balance: `(gross - coupon) + coupon = net_revenue + vat = gross`.

### The Chart of Accounts

| Code | Account Name | Category | Normal Side |
|------|-------------|----------|------------|
| 1200 | Accounts Receivable | Asset | Debit |
| 2101 | VAT Payable — Estonia | Liability | Credit |
| 2102 | VAT Payable — Finland | Liability | Credit |
| 2103 | VAT Payable — Latvia | Liability | Credit |
| 3000 | Retained Earnings | Equity | Credit |
| 4101 | Ride Revenue — Estonia | Revenue | Credit |
| 4102 | Ride Revenue — Finland | Revenue | Credit |
| 4103 | Ride Revenue — Latvia | Revenue | Credit |
| 6200 | Marketing Expense — Coupons | Expense | Debit |

Revenue and VAT accounts are country-specific because tax rates and reporting obligations differ per jurisdiction (Estonia 24%, Finland 25.5%, Latvia 21%). Accounts Receivable and Marketing Expense are consolidated globally — the same account code regardless of country.

### Separation of Chart of Accounts and Posting Rules

The chart of accounts (`chart_of_accounts.csv`) defines what each account is — its name, category, and normal side. The posting rules (`account_mapping.csv`) define which account a transaction line hits for a given `line_type + country` combination. These are separate concerns: the chart of accounts is a master reference, while posting rules are transaction-routing logic. This separation follows the pattern used in enterprise ERP systems (SAP, Oracle Financials, NetSuite) where account determination rules are maintained independently of the account master.

---

## Traceability: From Reports Back to Source Events

Every row in the general ledger carries the full chain of identifiers needed to trace back to the originating event:

| Column | Purpose |
|--------|---------|
| `journal_entry_id` | Deterministic MD5 hash of `order_id + line_type` — uniquely identifies each journal line |
| `order_id` | The original billing order (UUID) — links directly to the source ride |
| `ride_id` | Deterministic hash of `scooter_id + start_time + end_time` |
| `scooter_id` | The physical scooter that performed the ride |
| `ride_date` | When the ride occurred |
| `line_type` | Which leg of the journal entry this row represents (`receivable`, `revenue`, `vat_payable`, `coupon_expense`) |
| `account_code` | The GL account this line posted to |
| `reporting_period` | Month-level partition for period filtering |
| `loaded_at` | Timestamp of when the row was written to the ledger |

To trace a specific ledger row back to its source:

```sql
-- From a balance sheet line item, drill into the ledger
SELECT * FROM data_warehouse.fct_general_ledger
WHERE account_code = '4101' AND reporting_period = '2026-01-01'
ORDER BY ride_date, order_id;

-- From a ledger row, find the original ride
SELECT * FROM data_lake.rides
WHERE order_id = 'some-uuid-here';
```

The `journal_entry_id` is deterministic: given the same `order_id` and `line_type`, it always produces the same hash. This means reprocessing a period produces identical IDs for unchanged rides, making reconciliation straightforward.

---

## Data Quality and Validations

The pipeline validates data integrity at multiple levels.

### Schema tests (dbt schema.yml)

Every model layer has schema tests defined in `schema.yml` files:

- `unique` and `not_null` on key identifiers (`journal_entry_id`, `order_id`, `account_code`)
- `accepted_values` on categorical fields (`entry_side` must be debit/credit, `account_category` must be asset/liability/equity/revenue/expense, `country` must be Estonia/Finland/Latvia)
- `relationships` between posting rules and chart of accounts (every `account_code` in the posting rules must exist in the chart of accounts)

### Custom singular tests (dbt tests/)

Three custom SQL tests validate the fundamental accounting invariants:

- **`assert_journal_entries_balance`**: For every `order_id`, the sum of debits must equal the sum of credits. Any row returned means a broken journal entry.
- **`assert_gl_total_balance`**: Across the entire general ledger, the total signed amount must be zero (total debits = total credits globally).
- **`assert_balance_sheet_equation`**: The balance sheet must satisfy Assets = Liabilities + Equity. Any row where `equation_balanced = false` fails the test.

### Source-level validations

The Python data generator (`create_source_data.py`) runs its own validation suite before writing CSVs: VAT calculation accuracy, sum_with_vat consistency, coupon rule compliance, double-entry balance, ride-per-day range, and mapping completeness. The script prints a validation summary and flags any failures.

### Defensive joins

The general ledger uses `INNER JOIN` (not `LEFT JOIN`) when joining journal entries to posting rules and chart of accounts. If a posting rule or account is missing, the pipeline produces zero rows for the affected entries rather than silently inserting `NULL`-attributed ledger rows. This is a deliberate design choice for financial data — silent NULLs in a ledger are unacceptable.

---

## Incremental Loading Strategy

The general ledger uses a **delete+insert** pattern implemented via a dbt pre-hook macro (`delete_period`):

1. Before inserting new data, the pre-hook runs: `DELETE FROM fct_general_ledger WHERE ride_date BETWEEN start_date AND end_date`
2. The model's SELECT produces all journal lines for the current period
3. dbt inserts them into the table

This makes the GL **append-only across periods**: once January is processed and February begins, January's data is untouched. But the current period can be safely reprocessed at any time — the pre-hook clears it before re-inserting. On first run (or `--full-refresh`), the pre-hook is skipped because there is no existing table to delete from.

The same incremental pattern extends to every layer: `stg_rides`, `int_journal_entries`, `fct_trial_balance`, and all four report models — each deletes and reinserts only the current period's rows. This means that historical data at every layer is stable and auditable, building up a complete historical series of financial statements across all monthly runs.

---

## Airflow Orchestration

The DAG `monthly_financial_pipeline` (`airflow/dags/monthly_financial_pipeline.py`) runs on the 1st of each month and processes the previous month's data. It is organized into six sequential **TaskGroups** that mirror the dbt model layers, each running its own `dbt run --select` followed by `dbt test --select`:

```
generate_source_data → seed → staging → intermediate → marts → reports
```

1. **generate_source_data** — Python script produces ride records for the target month.
2. **seed** — Two parallel sub-tasks: `seed_rides` (monthly data) and `seed_reference_data` (static reference tables).
3. **staging** — Runs and tests `stg_rides`, `stg_account_mapping`, `stg_chart_of_accounts`.
4. **intermediate** — Runs and tests `int_journal_entries`, including the journal balance assertion.
5. **marts** — Runs and tests the GL first, then (only if GL tests pass) runs and tests the trial balance.
6. **reports** — Runs and tests all report models, including the balance sheet equation assertion.

This layer-by-layer design means a test failure in staging prevents wasted computation on downstream layers. The DAG has `catchup=True` for backfill and `max_active_runs=1` to prevent DuckDB file lock conflicts.

---

## Project Structure

```
.
├── airflow/
│   ├── Dockerfile                        # Airflow image with dbt + Python deps
│   ├── dags/
│   │   └── monthly_financial_pipeline.py # Monthly orchestration DAG
│   ├── logs/
│   └── plugins/
├── benchmarks/                           # Scalability and evaluation scripts
│   ├── baseline_income_statement.py      # Python baseline: CSV → income statement
│   ├── generate_benchmark_data.py       # Pre-generates scaled ride CSVs
│   ├── run_benchmarks.py                 # Multi-scale timing harness
│   └── run_fault_evaluation.sh          # End-to-end fault injection evaluation
├── compose.yml                           # Docker Compose (Airflow + Postgres)
├── .env.example                          # Environment template
├── data/                                 # Generated source CSVs (archive)
│   ├── rides.csv
│   ├── account_mapping.csv
│   └── chart_of_accounts.csv
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/
│   │   ├── get_start_date.sql            # Period start date (var or default)
│   │   ├── get_end_date.sql              # Period end date (var or default)
│   │   └── delete_period.sql             # Pre-hook: delete rows for a period
│   ├── models/
│   │   ├── sources.yml                   # dbt source declarations
│   │   ├── staging/
│   │   │   ├── stg_rides.sql             # Incremental + guardrails (NULL, outlier, dedup)
│   │   │   ├── stg_account_mapping.sql   # Journal posting rules
│   │   │   ├── stg_chart_of_accounts.sql # Account master
│   │   │   └── schema.yml
│   │   ├── intermediate/
│   │   │   ├── int_journal_entries.sql   # Ride → journal line explosion
│   │   │   └── schema.yml
│   │   ├── marts/
│   │   │   ├── fct_general_ledger.sql    # Incremental GL
│   │   │   ├── fct_trial_balance.sql     # Account aggregation by period
│   │   │   └── schema.yml
│   │   └── reports/
│   │       ├── rpt_income_statement_detail.sql   # Account-level P&L
│   │       ├── rpt_income_statement_summary.sql  # Period-level P&L totals
│   │       ├── rpt_balance_sheet_detail.sql      # Account-level balance sheet
│   │       ├── rpt_balance_sheet_summary.sql     # Equation validation
│   │       └── schema.yml
│   ├── seeds/
│   │   ├── rides.csv
│   │   ├── account_mapping.csv
│   │   ├── chart_of_accounts.csv
│   │   └── schema.yml                    # Column type definitions
│   ├── selectors.yml
│   └── tests/
│       ├── assert_journal_entries_balance.sql
│       ├── assert_gl_total_balance.sql
│       └── assert_balance_sheet_equation.sql
├── duckdb/                               # DuckDB database file
├── exports/                              # Exported reports
├── scripts/
│   ├── create_source_data.py             # Source data generator
│   └── inject_faults.py                 # Data quality fault injection
└── README.md
```

---

## Evaluation: Scalability Benchmarks

The `benchmarks/` directory measures execution time at increasing data scales, comparing three approaches:

| # | Approach | What it does |
|---|----------|-------------|
| 1 | **Python baseline** | Reads rides.csv with pandas, computes income statement. No database, no layers — the shortest path from CSV to report. |
| 2 | **dbt full-refresh** | The thesis pipeline processing all data from scratch (rides loaded via DuckDB COPY, then full dbt run). |
| 3 | **dbt incremental** | The thesis pipeline processing only December, with Jan–Nov already loaded. How the pipeline is designed to run in production. |

> **Note on data loading:** The benchmark loads the rides table directly into DuckDB via `COPY` rather than `dbt seed`. `dbt seed` is designed for small reference files and runs out of memory at large scales (6.6M+ rows). The production pipeline uses `dbt seed` normally — this is a benchmark-only adaptation. Reference tables (`account_mapping`, `chart_of_accounts`) are still seeded via dbt in both cases.

Data is scaled by increasing the fleet size. Three scales at roughly 10× increments: 90 scooters (~131K rides), 900 (~1.3M), and 4,500 (~6.6M). Each approach is timed 3 times per scale; the median is reported. A fourth scale (9,000 scooters, ~13.1M rides) was attempted but exceeded the Docker container's 12 GB memory allocation during dbt processing, causing out-of-memory termination.

### Prerequisites

Ensure the Docker containers are running (see [Setup](#setup)).

### Step-by-step: Run the benchmarks

All commands are run from your **host terminal** in the project root directory.

**Step 1 — Pre-generate source data for all scales (run once):**

```bash
docker compose exec airflow-webserver bash -lc \
  "cd /opt && python -u benchmarks/generate_benchmark_data.py --scales 90,900,4500"
```

This generates one `rides_{n}.csv` per scale into `benchmarks/data/` (gitignored). Generation is slow at the largest scales (~10–30 min total) but only needs to be done once. Subsequent benchmark runs load from these files directly — no regeneration.

**Step 2 — Validate the Python baseline produces correct totals (quick sanity check):**

```bash
docker compose exec airflow-webserver bash -lc \
  "cd /opt && python benchmarks/baseline_income_statement.py --validate"
```

Expected output: income statement printed, followed by `VALIDATION PASSED`. The totals must be: Revenue 536,507.46 / Expenses 10,188.00 / Net Income 526,319.46.

**Step 3 — Run the full benchmark suite:**

```bash
docker compose exec airflow-webserver bash -lc \
  "cd /opt && python -u benchmarks/run_benchmarks.py --scales 90,900,4500 --runs 3"
```

This will take a while (potentially 30+ minutes at the largest scales). For a quick test with just the two smallest scales:

```bash
docker compose exec airflow-webserver bash -lc \
  "cd /opt && python -u benchmarks/run_benchmarks.py --scales 90,900 --runs 3"
```

To run only the Python baseline (no dbt, much faster):

```bash
docker compose exec airflow-webserver bash -lc \
  "cd /opt && python benchmarks/run_benchmarks.py --skip-dbt"
```

### Output files

| File | Description |
|------|-------------|
| `benchmarks/data/rides_{n}.csv` | Pre-generated source data per scale (gitignored) |
| `benchmarks/results.csv` | Timing data: fleet_size, ride_count, approach, median_seconds |
| `benchmarks/results.json` | Detailed results with all individual run times |
| `benchmarks/results.specs.json` | Machine specs (CPU, Python/dbt/DuckDB versions) |

### What to expect

At small scale (90 scooters), the Python script is faster — it has no overhead. As scale increases, the dbt full-refresh time grows linearly (it processes the entire year each time), while the dbt incremental time stays roughly flat (it only processes December). The crossover point — where incremental becomes faster than the Python script — is the key finding.

After running, record the machine specs from `results.specs.json` for the thesis.

---

## Evaluation: Fault Injection (Data Quality)

The fault injection evaluation tests whether the pipeline's data quality mechanisms work against imperfect data.

### Fault types

| Fault | Real-world analogue | Handling | Mechanism |
|-------|-------------------|----------|-----------|
| NULL financial amounts | Payment gateway timeout | **Prevention** — staging filters them out | `WHERE amount IS NOT NULL` |
| Extreme outlier (100×) | Decimal point error | **Prevention** — staging filters them out | `WHERE amount <= 50.00` |
| Duplicate rides | Duplicate event delivery (Kafka) | **Prevention** — staging deduplicates | `ROW_NUMBER() OVER (PARTITION BY order_id ...)` |
| Invalid country | Misconfigured source | **Detection** — `accepted_values` test halts pipeline | Existing dbt test on `country` column |

### Staging guardrails (added to `stg_rides.sql`)

Three defensive CTEs were added to the staging model. They have **no effect on clean data** (verified: max clean ride amount is 8.70 EUR, no NULLs, no duplicate order_ids):

1. `not_null_filter` — excludes rows with NULL `amount`, `vat_amount`, or `sum_with_vat_amount`
2. `outlier_filter` — excludes rows where `amount > 50 EUR` (catches 100× decimal errors; threshold is 6× above max legitimate ride)
3. `deduplicated` — keeps one row per `order_id` via `ROW_NUMBER()`, ordered by `start_time, ride_id`

### Step-by-step: Run the fault evaluation

All commands are run from your **host terminal**.

**Option A — Automated end-to-end evaluation (recommended):**

```bash
docker compose exec airflow-webserver bash -lc \
  "cd /opt && bash benchmarks/run_fault_evaluation.sh"
```

This script does everything:
1. Generates clean data and runs the full pipeline (baseline)
2. Injects prevention faults (null, outlier, duplicate)
3. Re-seeds and re-runs the pipeline
4. Verifies all 75 tests still pass (guardrails filter faulted rows; totals reflect clean data only)
5. Restores clean data
6. Injects the detection fault (invalid country "Tartu")
7. Re-seeds and re-runs — expects the `accepted_values` test to **fail**
8. Restores clean data

**Option B — Manual step-by-step:**

```bash
# 1. Generate clean baseline data
docker compose exec airflow-webserver bash -lc \
  "cd /opt && python scripts/create_source_data.py --start-date 2026-01-01 --end-date 2027-01-01"

# 2. Seed and run pipeline on clean data
docker compose exec airflow-webserver bash -lc \
  "cd /opt/dbt && dbt seed --full-refresh --profiles-dir /opt/dbt"
docker compose exec airflow-webserver bash -lc \
  "cd /opt/dbt && dbt run --full-refresh --profiles-dir /opt/dbt \
    --vars '{\"start_date\": \"2026-01-01\", \"end_date\": \"2026-12-31\"}'"
docker compose exec airflow-webserver bash -lc \
  "cd /opt/dbt && dbt test --profiles-dir /opt/dbt \
    --vars '{\"start_date\": \"2026-01-01\", \"end_date\": \"2026-12-31\"}'"

# 3. Inject prevention-mode faults
docker compose exec airflow-webserver bash -lc \
  "cd /opt && python scripts/inject_faults.py --fault-types null,outlier,duplicate --seed 123"

# 4. Re-seed and re-run on faulted data
docker compose exec airflow-webserver bash -lc \
  "cd /opt/dbt && dbt seed --full-refresh --profiles-dir /opt/dbt"
docker compose exec airflow-webserver bash -lc \
  "cd /opt/dbt && dbt run --full-refresh --profiles-dir /opt/dbt \
    --vars '{\"start_date\": \"2026-01-01\", \"end_date\": \"2026-12-31\"}'"

# 5. Verify tests pass (should all pass — guardrails cleaned the faults)
docker compose exec airflow-webserver bash -lc \
  "cd /opt/dbt && dbt test --profiles-dir /opt/dbt \
    --vars '{\"start_date\": \"2026-01-01\", \"end_date\": \"2026-12-31\"}'"

# 6. Restore clean data
docker compose exec airflow-webserver bash -lc \
  "cd /opt && cp data/rides.csv.clean_backup data/rides.csv && \
    cp dbt/seeds/rides.csv.clean_backup dbt/seeds/rides.csv"

# 7. Inject detection-mode fault (invalid country)
docker compose exec airflow-webserver bash -lc \
  "cd /opt && python scripts/inject_faults.py --fault-types country --seed 123"

# 8. Re-seed and re-run — expect test FAILURE
docker compose exec airflow-webserver bash -lc \
  "cd /opt/dbt && dbt seed --full-refresh --profiles-dir /opt/dbt"
docker compose exec airflow-webserver bash -lc \
  "cd /opt/dbt && dbt run --full-refresh --profiles-dir /opt/dbt \
    --vars '{\"start_date\": \"2026-01-01\", \"end_date\": \"2026-12-31\"}'" || true
docker compose exec airflow-webserver bash -lc \
  "cd /opt/dbt && dbt test --profiles-dir /opt/dbt \
    --vars '{\"start_date\": \"2026-01-01\", \"end_date\": \"2026-12-31\"}'" || echo "EXPECTED FAILURE"

# 9. Restore clean data
docker compose exec airflow-webserver bash -lc \
  "cd /opt && cp data/rides.csv.clean_backup data/rides.csv && \
    cp dbt/seeds/rides.csv.clean_backup dbt/seeds/rides.csv"
```

### What to expect

**Prevention faults:** After injecting NULL, outlier, and duplicate faults, the pipeline should run normally. All 75 tests should pass. The staging guardrails silently filter out the faulted rows, so the income statement totals will be slightly lower than the clean baseline (the corrupted rows and their associated revenue are excluded). The reports accurately reflect only the data that passed the quality checks.

**Detection fault:** After injecting the invalid country "Tartu", the `dbt test` step should **fail** with an `accepted_values` error on `stg_rides.country`. This is the expected behaviour — the pipeline refuses to produce reports with invalid data.

Record the fault counts (how many rows of each type were injected) and the row counts (faulted CSV rows vs. staged rows) for the thesis tables.

---

## Troubleshooting

**`SHOW TABLES` returns no rows in DuckDB CLI:** Tables are created in named schemas. Use `SELECT * FROM data_warehouse.rpt_income_statement_detail` or inspect schemas with `SELECT schema_name FROM information_schema.schemata`.

**`dbt seed` fails after editing `seeds/schema.yml`:** Clear the partial parse cache: `dbt clean && dbt seed --profiles-dir /opt/dbt`.

**Why is there no DuckDB container?** DuckDB is embedded and file-based (like SQLite). dbt opens it directly at `/opt/duckdb/thesis.duckdb` inside the container, mapped to `duckdb/thesis.duckdb` on the host via a Docker volume.

**Airflow UI not loading:** Ensure the webserver container is running (`docker compose ps`). The UI is at http://localhost:8080 with credentials from your `.env` file.
