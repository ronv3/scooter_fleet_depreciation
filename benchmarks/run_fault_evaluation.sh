#!/usr/bin/env bash
#
# Run the fault injection evaluation end-to-end.
#
# This script must be run from the project root, inside the Docker container
# (or on a host with dbt-duckdb + Python deps installed).
#
# Steps:
#   1. Run the pipeline on clean data (baseline — should already be done)
#   2. Inject faults (null, outlier, duplicate — prevention mode)
#   3. Re-seed and re-run pipeline
#   4. Verify tests pass and totals unchanged
#   5. Restore clean data
#   6. Inject country fault (detection mode)
#   7. Re-seed and re-run pipeline → expect staging tests to FAIL
#   8. Restore clean data
#
# Usage:
#   # Inside Docker container:
#   cd /opt
#   bash benchmarks/run_fault_evaluation.sh
#
set -euo pipefail

DBT_DIR="${DBT_DIR:-dbt}"
DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-dbt}"
SCRIPTS_DIR="${SCRIPTS_DIR:-scripts}"

VARS='{"start_date": "2026-01-01", "end_date": "2026-12-31"}'

echo "=============================================="
echo "  Fault Injection Evaluation"
echo "=============================================="
echo ""

# ── Step 1: Ensure clean baseline ──────────────────────────────────────────
echo "--- Step 1: Generate clean data and run pipeline ---"
python ${SCRIPTS_DIR}/create_source_data.py --start-date 2026-01-01 --end-date 2027-01-01
cd ${DBT_DIR}
dbt seed --full-refresh --profiles-dir ${DBT_PROFILES_DIR}
dbt run --full-refresh --profiles-dir ${DBT_PROFILES_DIR} --vars "${VARS}"
dbt test --profiles-dir ${DBT_PROFILES_DIR} --vars "${VARS}"
cd ..

echo ""
echo "--- Baseline: query income statement totals ---"
python3 -c "
import duckdb
con = duckdb.connect('${DUCKDB_PATH:-duckdb/thesis.duckdb}', read_only=True)
row = con.execute('SELECT total_revenue, total_expenses, net_income FROM data_warehouse.rpt_income_statement_summary').fetchone()
print(f'  Baseline Revenue:  {row[0]:,.2f}')
print(f'  Baseline Expenses: {row[1]:,.2f}')
print(f'  Baseline Net Inc:  {row[2]:,.2f}')
con.close()
"

# ── Step 2: Inject prevention-mode faults ──────────────────────────────────
echo ""
echo "--- Step 2: Inject faults (null, outlier, duplicate) ---"
python ${SCRIPTS_DIR}/inject_faults.py --fault-types null,outlier,duplicate --seed 123

# ── Step 3: Re-seed and re-run ─────────────────────────────────────────────
echo ""
echo "--- Step 3: Re-seed and re-run pipeline on faulted data ---"
cd ${DBT_DIR}
dbt seed --full-refresh --profiles-dir ${DBT_PROFILES_DIR}
dbt run --full-refresh --profiles-dir ${DBT_PROFILES_DIR} --vars "${VARS}"

# ── Step 4: Verify tests and totals ───────────────────────────────────────
echo ""
echo "--- Step 4: Run tests (should all pass) ---"
dbt test --profiles-dir ${DBT_PROFILES_DIR} --vars "${VARS}"
cd ..

echo ""
echo "--- Step 4b: Verify income statement totals unchanged ---"
python3 -c "
import duckdb
con = duckdb.connect('${DUCKDB_PATH:-duckdb/thesis.duckdb}', read_only=True)
row = con.execute('SELECT total_revenue, total_expenses, net_income FROM data_warehouse.rpt_income_statement_summary').fetchone()
print(f'  Faulted Revenue:  {row[0]:,.2f}')
print(f'  Faulted Expenses: {row[1]:,.2f}')
print(f'  Faulted Net Inc:  {row[2]:,.2f}')

# Validation
rev_ok = abs(row[0] - 536507.46) < 0.01
exp_ok = abs(row[1] - 10188.00) < 0.01
net_ok = abs(row[2] - 526319.46) < 0.01
if rev_ok and exp_ok and net_ok:
    print('  PASS: totals match clean baseline')
else:
    print('  FAIL: totals differ from baseline!')
    exit(1)
con.close()
"

# Count how many rides were filtered
echo ""
echo "--- Fault filtering summary ---"
python3 -c "
import pandas as pd
import duckdb

faulted = pd.read_csv('data/rides.csv')
con = duckdb.connect('${DUCKDB_PATH:-duckdb/thesis.duckdb}', read_only=True)
staged_count = con.execute('SELECT COUNT(*) FROM data_warehouse.stg_rides').fetchone()[0]
print(f'  Faulted CSV rows:  {len(faulted):,}')
print(f'  Staged rows:       {staged_count:,}')
print(f'  Rows filtered out: {len(faulted) - staged_count:,}')
con.close()
"

# ── Step 5: Restore clean data ─────────────────────────────────────────────
echo ""
echo "--- Step 5: Restore clean data ---"
cp data/rides.csv.clean_backup data/rides.csv
cp dbt/seeds/rides.csv.clean_backup dbt/seeds/rides.csv

# ── Step 6: Inject detection-mode fault (invalid country) ──────────────────
echo ""
echo "--- Step 6: Inject country fault (detection mode) ---"
python ${SCRIPTS_DIR}/inject_faults.py --fault-types country --seed 123

# ── Step 7: Re-seed and expect test failure ────────────────────────────────
echo ""
echo "--- Step 7: Re-seed and run --- "
cd ${DBT_DIR}
dbt seed --full-refresh --profiles-dir ${DBT_PROFILES_DIR}
dbt run --full-refresh --profiles-dir ${DBT_PROFILES_DIR} --vars "${VARS}" || true

echo ""
echo "--- Step 7b: Run tests (expect accepted_values FAILURE on country) ---"
if dbt test --profiles-dir ${DBT_PROFILES_DIR} --vars "${VARS}" 2>&1; then
    echo "  WARNING: Tests passed unexpectedly — country fault was not detected!"
else
    echo "  EXPECTED: Tests failed — invalid country 'Atlantis' was detected by accepted_values test"
fi
cd ..

# ── Step 8: Restore clean data ─────────────────────────────────────────────
echo ""
echo "--- Step 8: Restore clean data ---"
cp data/rides.csv.clean_backup data/rides.csv
cp dbt/seeds/rides.csv.clean_backup dbt/seeds/rides.csv
echo ""
echo "=============================================="
echo "  Fault evaluation complete."
echo "=============================================="
