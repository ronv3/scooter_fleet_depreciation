"""
Benchmark harness: measure execution time at increasing data scales.

Compares three approaches:
  1. Python baseline  — read CSV, compute income statement directly
  2. dbt full-refresh — the thesis pipeline processing all data from scratch
  3. dbt incremental  — the thesis pipeline processing only one new month
                        (with 11 months already loaded)

Data is scaled by adjusting the fleet size parameter in the data generator.
Four target scales at roughly 10× increments:
  - 90 scooters   (baseline, ~131K rides)
  - 900 scooters  (~1.3M rides)
  - 4500 scooters (~6.6M rides)
  - 9000 scooters (~13.1M rides)

Each approach is measured 3 times per scale; the median is reported.

This script must be run INSIDE the Docker container where dbt is available,
or on a host with dbt-duckdb installed and configured.

Usage (inside Docker container):
    cd /opt
    python benchmarks/run_benchmarks.py [--scales 90,900,4500,9000]
                                         [--runs 3]
                                         [--output benchmarks/results.csv]
                                         [--skip-dbt]

Usage (host, Python baseline only):
    python benchmarks/run_benchmarks.py --skip-dbt
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from datetime import date
from pathlib import Path
from statistics import median

# Ensure the project root's scripts are importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
sys.path.insert(0, str(PROJECT_ROOT / "benchmarks"))

from baseline_income_statement import compute_income_statement


# ── Configuration ────────────────────────────────────────────────────────────

DEFAULT_SCALES = [90, 900, 4500, 9000]
DEFAULT_RUNS = 3
DEFAULT_OUTPUT = Path("benchmarks/results.csv")

DBT_DIR = Path(os.environ.get("DBT_DIR", "dbt"))
DBT_PROFILES_DIR = Path(os.environ.get("DBT_PROFILES_DIR", "dbt"))
DUCKDB_PATH = Path(os.environ.get("DUCKDB_PATH", "duckdb/thesis.duckdb"))

DATA_DIR = Path("data")
SEED_DIR = Path("dbt/seeds")


# ── Helpers ──────────────────────────────────────────────────────────────────

def get_machine_specs() -> dict:
    """Collect machine specs for reproducibility documentation."""
    specs = {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
        "cpu_count": os.cpu_count(),
    }

    # Try to get dbt version
    try:
        result = subprocess.run(
            ["dbt", "--version"],
            capture_output=True, text=True, timeout=10,
            cwd=str(DBT_DIR),
        )
        for line in result.stdout.splitlines():
            if "dbt-core" in line.lower():
                specs["dbt_core_version"] = line.strip()
            if "duckdb" in line.lower():
                specs["dbt_duckdb_version"] = line.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        specs["dbt_available"] = False

    return specs


def generate_data(fleet_size: int) -> Path:
    """
    Generate a full year of ride data for the given fleet size.

    Temporarily patches the SCOOTER_IDS in the generator to scale the fleet.
    Returns the path to the generated rides.csv.
    """
    # We generate data by calling the generator with the fleet size patched
    # via a wrapper that modifies the config before calling generate_rides_df
    rides_csv = DATA_DIR / "rides.csv"

    print(f"  Generating data for {fleet_size} scooters...")

    # Use a subprocess to avoid module-level side effects
    gen_script = f"""
import sys
sys.path.insert(0, '{PROJECT_ROOT / "scripts"}')
import numpy as np
import create_source_data as gen

gen.SCOOTER_IDS = np.arange(1, {fleet_size} + 1, dtype=int)

# Update city map for scaled fleet (distribute evenly across 3 cities)
third = {fleet_size} // 3
gen.CITY_MAP = {{
    (1, third): ("Tallinn", "Estonia"),
    (third + 1, 2 * third): ("Helsinki", "Finland"),
    (2 * third + 1, {fleet_size}): ("Riga", "Latvia"),
}}

from datetime import date
rides_df = gen.generate_rides_df(
    seed=42,
    ride_start=date(2026, 1, 1),
    ride_end_exclusive=date(2027, 1, 1),
)

mapping_df, accounts_df = gen.generate_account_mapping_df()
gen.save_csvs(rides_df, mapping_df, accounts_df)
print(f"Generated {{len(rides_df)}} rides for {fleet_size} scooters")
"""
    result = subprocess.run(
        [sys.executable, "-c", gen_script],
        capture_output=True, text=True, timeout=600,
        cwd=str(PROJECT_ROOT),
    )
    if result.returncode != 0:
        print(f"  ERROR generating data: {result.stderr}")
        raise RuntimeError(f"Data generation failed for fleet_size={fleet_size}")
    print(f"  {result.stdout.strip()}")

    return rides_csv


def time_python_baseline(rides_csv: Path) -> float:
    """Time the Python baseline income statement computation. Returns seconds."""
    result = compute_income_statement(rides_csv)
    return result["elapsed_seconds"]


def run_dbt_command(args: list[str], timeout: int = 3600) -> tuple[float, str]:
    """
    Run a dbt command and return (elapsed_seconds, stdout).
    Must be run in an environment where dbt is available.
    """
    cmd = [
        "dbt", *args,
        "--profiles-dir", str(DBT_PROFILES_DIR),
    ]

    t0 = time.perf_counter()
    result = subprocess.run(
        cmd,
        capture_output=True, text=True, timeout=timeout,
        cwd=str(DBT_DIR),
    )
    elapsed = time.perf_counter() - t0

    if result.returncode != 0:
        print(f"  dbt command failed: {' '.join(cmd)}")
        print(f"  stderr: {result.stderr[-500:]}")
        raise RuntimeError(f"dbt command failed: {' '.join(cmd)}")

    return elapsed, result.stdout


def time_dbt_full_refresh() -> float:
    """
    Time a full-refresh dbt pipeline run (seed + run + test).
    Returns total wall-clock seconds.
    """
    t0 = time.perf_counter()

    # Remove existing DuckDB to start fresh
    if DUCKDB_PATH.exists():
        DUCKDB_PATH.unlink()

    # Seed
    run_dbt_command(["seed", "--full-refresh"])

    # Run all models (full refresh)
    run_dbt_command([
        "run", "--full-refresh",
        "--vars", '{"start_date": "2026-01-01", "end_date": "2026-12-31"}',
    ])

    elapsed = time.perf_counter() - t0
    return elapsed


def time_dbt_incremental() -> float:
    """
    Time an incremental dbt pipeline run processing only December.

    Pre-condition: months 1-11 must already be loaded.
    This method first loads months 1-11, then times only the December run.
    """
    # Setup: remove DB and do a full run for Jan-Nov
    if DUCKDB_PATH.exists():
        DUCKDB_PATH.unlink()

    run_dbt_command(["seed", "--full-refresh"])

    # Load months 1-11
    run_dbt_command([
        "run", "--full-refresh",
        "--vars", '{"start_date": "2026-01-01", "end_date": "2026-11-30"}',
    ])

    # Now time only the December incremental run
    t0 = time.perf_counter()

    run_dbt_command([
        "run",
        "--vars", '{"start_date": "2026-12-01", "end_date": "2026-12-31"}',
    ])

    elapsed = time.perf_counter() - t0
    return elapsed


# ── Main benchmark loop ─────────────────────────────────────────────────────

def run_benchmarks(
    scales: list[int],
    n_runs: int,
    output_path: Path,
    skip_dbt: bool = False,
) -> None:
    specs = get_machine_specs()
    print("=== Machine Specs ===")
    for k, v in specs.items():
        print(f"  {k}: {v}")

    # Save specs alongside results
    specs_path = output_path.with_suffix(".specs.json")
    with open(specs_path, "w") as f:
        json.dump(specs, f, indent=2)
    print(f"\nSpecs saved to {specs_path}")

    results = []

    for fleet_size in scales:
        print(f"\n{'='*60}")
        print(f"Scale: {fleet_size} scooters")
        print(f"{'='*60}")

        # Generate data at this scale
        rides_csv = generate_data(fleet_size)

        # Count rows
        with open(rides_csv) as f:
            row_count = sum(1 for _ in f) - 1  # subtract header

        print(f"  Ride count: {row_count:,}")

        # --- Python baseline ---
        print(f"\n  Python baseline ({n_runs} runs):")
        py_times = []
        for i in range(n_runs):
            t = time_python_baseline(rides_csv)
            py_times.append(t)
            print(f"    Run {i+1}: {t:.3f}s")
        py_median = median(py_times)
        print(f"    Median: {py_median:.3f}s")

        results.append({
            "fleet_size": fleet_size,
            "ride_count": row_count,
            "approach": "python_baseline",
            "median_seconds": round(py_median, 4),
            "all_times": py_times,
        })

        if not skip_dbt:
            # --- dbt full-refresh ---
            print(f"\n  dbt full-refresh ({n_runs} runs):")
            dbt_full_times = []
            for i in range(n_runs):
                t = time_dbt_full_refresh()
                dbt_full_times.append(t)
                print(f"    Run {i+1}: {t:.3f}s")
            dbt_full_median = median(dbt_full_times)
            print(f"    Median: {dbt_full_median:.3f}s")

            results.append({
                "fleet_size": fleet_size,
                "ride_count": row_count,
                "approach": "dbt_full_refresh",
                "median_seconds": round(dbt_full_median, 4),
                "all_times": dbt_full_times,
            })

            # --- dbt incremental ---
            print(f"\n  dbt incremental ({n_runs} runs):")
            dbt_inc_times = []
            for i in range(n_runs):
                t = time_dbt_incremental()
                dbt_inc_times.append(t)
                print(f"    Run {i+1}: {t:.3f}s")
            dbt_inc_median = median(dbt_inc_times)
            print(f"    Median: {dbt_inc_median:.3f}s")

            results.append({
                "fleet_size": fleet_size,
                "ride_count": row_count,
                "approach": "dbt_incremental",
                "median_seconds": round(dbt_inc_median, 4),
                "all_times": dbt_inc_times,
            })

    # --- Write results CSV ---
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "fleet_size", "ride_count", "approach", "median_seconds",
        ])
        writer.writeheader()
        for r in results:
            writer.writerow({
                "fleet_size": r["fleet_size"],
                "ride_count": r["ride_count"],
                "approach": r["approach"],
                "median_seconds": r["median_seconds"],
            })

    print(f"\n=== Results saved to {output_path} ===")

    # Also save detailed results as JSON
    json_path = output_path.with_suffix(".json")
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Detailed results saved to {json_path}")

    # Restore original data (90 scooters)
    print("\nRestoring original 90-scooter data...")
    generate_data(90)
    print("Done.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run scalability benchmarks at multiple data scales"
    )
    parser.add_argument(
        "--scales",
        type=str,
        default=",".join(str(s) for s in DEFAULT_SCALES),
        help=f"Comma-separated fleet sizes (default: {DEFAULT_SCALES})",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=DEFAULT_RUNS,
        help=f"Number of runs per approach per scale (default: {DEFAULT_RUNS})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--skip-dbt",
        action="store_true",
        help="Skip dbt benchmarks (run Python baseline only)",
    )
    args = parser.parse_args()

    scales = [int(s.strip()) for s in args.scales.split(",")]

    run_benchmarks(
        scales=scales,
        n_runs=args.runs,
        output_path=args.output,
        skip_dbt=args.skip_dbt,
    )


if __name__ == "__main__":
    main()
