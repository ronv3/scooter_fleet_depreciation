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
cd scooter_fleet_depreciation
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
-- Full-year income statement
SELECT * FROM data_warehouse.rpt_income_statement ORDER BY sort_order, account_code;

-- Balance sheet as of December 31, 2026
SELECT * FROM data_warehouse.rpt_balance_sheet ORDER BY sort_order, account_code;

-- General ledger row count
SELECT COUNT(*) FROM data_warehouse.fct_general_ledger;

-- Trial balance by period — verify debits = credits per month
SELECT
    reporting_period,
    SUM(total_debit)  AS total_debit,
    SUM(total_credit) AS total_credit,
    ABS(SUM(total_debit) - SUM(total_credit)) < 0.01 AS balanced
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
        |                       Full rebuild from entire GL each run
       / \
      v   v
rpt_income_statement         rpt_balance_sheet
(current period only)        (cumulative through period end)
```

### Layer-by-Layer Detail

**Source data generation** (`scripts/create_source_data.py`): A Python script generates synthetic ride billing records — one row per completed scooter ride with pricing breakdown (net amount, VAT, gross, coupon discount). The script accepts `--start-date` and `--end-date` arguments. Without arguments it defaults to the previous calendar month. All random generation uses a fixed seed (42) so the same date range always produces identical data.

**Staging** (`stg_rides`, `stg_account_mapping`, `stg_chart_of_accounts`): Defensive data cleaning. Types are cast explicitly (amounts to `DECIMAL(12,2)`, timestamps, etc.). Coupon fields are normalized (empty strings and `'None'` become SQL `NULL`, missing coupon amounts default to 0). The staging layer also applies the period filter — only rides within the `start_date`/`end_date` range flow downstream.

**Intermediate** (`int_journal_entries`): The core accounting transformation. Each ride is cross-joined with a line-type spine to produce double-entry journal lines. A ride without a coupon produces 3 lines; a ride with a coupon produces 4. Zero-amount lines are filtered out (standard accounting practice — you do not post a EUR 0.00 entry). Each line receives a deterministic `journal_entry_id` (MD5 hash of `order_id` + `line_type`) for traceability.

**General Ledger** (`fct_general_ledger`): The single source of truth for all financial reporting. Journal lines are enriched with account metadata through a two-step join: first to the posting rules (resolving `line_type + country` to an `account_code`), then to the chart of accounts (resolving `account_code` to name, category, and normal side). The GL is materialized as an **incremental** table with a pre-hook that deletes the current period's rows before inserting fresh ones. This makes it append-only across periods while allowing safe reprocessing of any individual period.

**Trial Balance** (`fct_trial_balance`): Aggregates the entire general ledger by `reporting_period`, `account_code`, and `country`. Produces total debits, total credits, and net balance per account per month. Rebuilt fully on every run so it always reflects the complete GL.

**Income Statement** (`rpt_income_statement`): A period statement showing revenue minus expenses for the reporting period. Filters the trial balance to the current month(s) only.

**Balance Sheet** (`rpt_balance_sheet`): A point-in-time statement showing cumulative financial position. Filters the trial balance to everything up to and including the period end date, then aggregates across all historical months. Includes an `equation_balanced` validation column that verifies Assets = Liabilities + Equity.

---

## Double-Entry Accounting Logic

### The Journal Entry for a Scooter Ride

Every completed ride produces a balanced set of journal entries. For a ride costing EUR 5.18 net + 22% VAT = EUR 6.32 gross, with no coupon:

```
DR  1200  Accounts Receivable       EUR 6.32
    CR  4101  Ride Revenue — Estonia     EUR 5.18
    CR  2101  VAT Payable — Estonia      EUR 1.14

Debits (6.32) = Credits (5.18 + 1.14 = 6.32)  ✓
```

With a EUR 3.00 coupon applied (customer pays EUR 3.32):

```
DR  1200  Accounts Receivable       EUR 3.32
DR  6200  Marketing Expense         EUR 3.00
    CR  4101  Ride Revenue — Estonia     EUR 5.18
    CR  2101  VAT Payable — Estonia      EUR 1.14

Debits (3.32 + 3.00 = 6.32) = Credits (5.18 + 1.14 = 6.32)  ✓
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

Revenue and VAT accounts are country-specific because tax rates and reporting obligations differ per jurisdiction (Estonia 22%, Finland 24%, Latvia 21%). Accounts Receivable and Marketing Expense are consolidated globally — the same account code regardless of country.

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

The trial balance and reports are full-rebuild tables that re-aggregate from the entire GL each run. Since the GL accumulates data across periods, these downstream models always reflect the complete picture.

---

## Airflow Orchestration

The DAG `monthly_financial_pipeline` (`airflow/dags/monthly_financial_pipeline.py`) runs on the 1st of each month and processes the previous month's data.

```
generate_source_data  →  dbt_seed  →  dbt_run  →  dbt_test
```

**Task 1 — generate_source_data**: Runs the Python script with the period's start and end dates derived from Airflow's `execution_date`.

**Task 2 — dbt_seed**: Loads the freshly generated `rides.csv` into DuckDB. Only `rides` is re-seeded; the chart of accounts and posting rules are static reference data.

**Task 3 — dbt_run**: Executes all dbt models with `--vars` passing `start_date` and `end_date`. The staging layer filters to the period, the GL deletes+inserts only that period's rows, the trial balance rebuilds from the full GL, and reports scope to the appropriate period.

**Task 4 — dbt_test**: Validates all schema tests and custom accounting invariant tests. A test failure stops the pipeline and prevents downstream consumers from reading inconsistent data.

The DAG has `catchup=True`, so enabling it will backfill all months from January 2026 to the current date. `max_active_runs=1` prevents concurrent runs from conflicting on the DuckDB file lock.

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
│   │   │   ├── stg_rides.sql
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
│   │       ├── rpt_income_statement.sql  # P&L for current period
│   │       ├── rpt_balance_sheet.sql     # Cumulative balance sheet
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
│   └── create_source_data.py             # Source data generator
└── README.md
```

---

## Assumptions and Simplifications

This project is a demonstration for a Bachelor's thesis. Several simplifications have been made to keep the scope manageable while still reflecting real-world data warehouse patterns:

**Mock data generation.** All ride data is synthetically generated with a fixed random seed. In a real system, ride data would arrive from an operational database, a streaming platform (Kafka), or an API. The data generator simulates billing records that would typically come from a payment or invoicing system.

**Immediate revenue recognition.** Revenue is recognized at the moment the ride is completed. In practice, scooter companies may have more complex recognition rules (prepaid wallet balances, subscription plans, refund windows). The model treats each ride as a simple completed transaction.

**No cash/bank account.** The model records the receivable (customer owes money) but does not model the payment settlement (customer pays via Stripe/Adyen). In a complete system, there would be a subsequent journal entry: DR Cash/Bank, CR Accounts Receivable. The AR balance in the balance sheet therefore represents the cumulative invoiced amount, not actual outstanding receivables.

**Coupon treatment.** Coupons are treated as a marketing expense (debit to account 6200) rather than as a contra-revenue. Both treatments are valid under accounting standards; expense treatment is more common for promotional coupons that are funded by a marketing budget.

**Single currency.** All transactions are in EUR. Multi-currency operations would require foreign exchange conversion logic, unrealized gain/loss tracking, and a functional currency designation — complexities beyond the scope of this thesis.

**Retained earnings.** The balance sheet computes equity as "Retained Earnings = cumulative Revenue - Expenses." In a multi-year system, retained earnings would carry forward from prior fiscal years with period-closing entries. Since this model starts from a clean slate and does not include dividends or other equity transactions, the calculation is equivalent.

**DuckDB as warehouse.** DuckDB is an embedded analytical database, chosen for portability (no server setup). A production financial data warehouse would typically use Snowflake, BigQuery, Redshift, or a similar cloud warehouse with role-based access control, audit logging, and concurrent query support.

---

## Future Work

The following extensions would bring the pipeline closer to production readiness. They are outside the scope of this thesis but represent natural next steps:

**Period closing mechanism.** A `closed_periods` reference table that marks processed months as locked. The pipeline would refuse to reprocess closed periods, preventing accidental modification of finalized financial data. This is standard in enterprise accounting systems.

**Cash settlement entries.** Adding a payment/settlement data source that generates DR Cash / CR Accounts Receivable entries when customers pay. This would make the AR balance reflect true outstanding receivables and complete the cash flow picture.

**Reversal and adjustment entries.** A mechanism to post manual adjusting entries (accruals, corrections, reclassifications) that are not derived from ride data. These would be loaded from a separate source table and merged into the journal entry flow.

**Depreciation module.** The project name references scooter fleet depreciation. Source data for scooter master records and heartbeat snapshots already exists (`scooters_master.csv`, `scooter_heartbeat.csv`) but is not yet integrated into the financial pipeline. A depreciation model would calculate monthly asset write-downs based on scooter purchase price and useful life.

**Multi-period financial statements.** The current reports show a single period. Comparative statements (this month vs. last month, this year vs. last year) and year-to-date accumulation would provide more analytical value.

**Data lineage visualization.** Integrating dbt docs (`dbt docs generate && dbt docs serve`) to provide an interactive DAG visualization and column-level lineage for audit purposes.

---

## Troubleshooting

**`SHOW TABLES` returns no rows in DuckDB CLI:** Tables are created in named schemas. Use `SELECT * FROM data_warehouse.rpt_income_statement` or inspect schemas with `SELECT schema_name FROM information_schema.schemata`.

**`dbt seed` fails after editing `seeds/schema.yml`:** Clear the partial parse cache: `dbt clean && dbt seed --profiles-dir /opt/dbt`.

**Why is there no DuckDB container?** DuckDB is embedded and file-based (like SQLite). dbt opens it directly at `/opt/duckdb/thesis.duckdb` inside the container, mapped to `duckdb/thesis.duckdb` on the host via a Docker volume.

**Airflow UI not loading:** Ensure the webserver container is running (`docker compose ps`). The UI is at http://localhost:8080 with credentials from your `.env` file.
