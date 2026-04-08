"""
Inject realistic data quality faults into rides.csv for robustness evaluation.

This script reads the clean rides CSV (produced by create_source_data.py),
introduces configurable data quality faults, and writes the faulted data
back to the same location so it can be seeded into the pipeline.

Fault types and their real-world analogues:
  1. NULL financial amounts  — payment gateway timeout / missing data
  2. Extreme outlier amounts — decimal point error in source system (100× multiplier)
  3. Duplicate ride records  — duplicate event delivery (Kafka, webhooks)
  4. Invalid country code    — misconfigured source system

The first three are "clean" faults: the staging layer should filter or
deduplicate them silently. The fourth is a "detect" fault: the existing
accepted_values test on stg_rides.country should halt the pipeline.

Usage:
    python scripts/inject_faults.py [--fault-types null,outlier,duplicate,country]
                                     [--seed 123]
                                     [--null-rate 0.005]
                                     [--outlier-rate 0.003]
                                     [--duplicate-rate 0.005]
                                     [--country-rate 0.002]

    By default, the "country" fault is NOT injected (it halts the pipeline).
    Pass --fault-types explicitly to include it.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import numpy as np
import pandas as pd


RIDES_CSV = Path("data/rides.csv")
SEED_CSV = Path("dbt/seeds/rides.csv")
BACKUP_SUFFIX = ".clean_backup"

DEFAULT_SEED = 123
DEFAULT_NULL_RATE = 0.005       # ~0.5 % of rows
DEFAULT_OUTLIER_RATE = 0.003    # ~0.3 % of rows
DEFAULT_DUPLICATE_RATE = 0.005  # ~0.5 % of rows
DEFAULT_COUNTRY_RATE = 0.002    # ~0.2 % of rows


def inject_null_amounts(
    df: pd.DataFrame, rng: np.random.Generator, rate: float
) -> tuple[pd.DataFrame, int]:
    """Set amount, vat_amount, and sum_with_vat_amount to NaN for a sample of rows."""
    n = max(1, int(len(df) * rate))
    idx = rng.choice(df.index, size=n, replace=False)
    df.loc[idx, "amount"] = np.nan
    df.loc[idx, "vat_amount"] = np.nan
    df.loc[idx, "sum_with_vat_amount"] = np.nan
    return df, n


def inject_outlier_amounts(
    df: pd.DataFrame, rng: np.random.Generator, rate: float
) -> tuple[pd.DataFrame, int]:
    """Multiply amount fields by 100× to simulate decimal-point errors."""
    n = max(1, int(len(df) * rate))
    idx = rng.choice(df.index, size=n, replace=False)
    for col in ["amount", "vat_amount", "sum_with_vat_amount"]:
        df.loc[idx, col] = (df.loc[idx, col] * 100).round(2)
    return df, n


def inject_duplicates(
    df: pd.DataFrame, rng: np.random.Generator, rate: float
) -> tuple[pd.DataFrame, int]:
    """Duplicate a sample of rows (exact copies) to simulate duplicate event delivery."""
    n = max(1, int(len(df) * rate))
    idx = rng.choice(df.index, size=n, replace=False)
    duplicates = df.loc[idx].copy()
    df = pd.concat([df, duplicates], ignore_index=True)
    return df, n


def inject_invalid_country(
    df: pd.DataFrame, rng: np.random.Generator, rate: float
) -> tuple[pd.DataFrame, int]:
    """Replace country with an invalid value to test accepted_values detection."""
    n = max(1, int(len(df) * rate))
    idx = rng.choice(df.index, size=n, replace=False)
    df.loc[idx, "country"] = "Atlantis"
    return df, n


FAULT_HANDLERS = {
    "null": inject_null_amounts,
    "outlier": inject_outlier_amounts,
    "duplicate": inject_duplicates,
    "country": inject_invalid_country,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inject data quality faults into rides.csv"
    )
    parser.add_argument(
        "--fault-types",
        type=str,
        default="null,outlier,duplicate",
        help=(
            "Comma-separated fault types to inject. "
            "Options: null, outlier, duplicate, country. "
            "Default: null,outlier,duplicate (country excluded because it halts the pipeline)."
        ),
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--null-rate", type=float, default=DEFAULT_NULL_RATE)
    parser.add_argument("--outlier-rate", type=float, default=DEFAULT_OUTLIER_RATE)
    parser.add_argument("--duplicate-rate", type=float, default=DEFAULT_DUPLICATE_RATE)
    parser.add_argument("--country-rate", type=float, default=DEFAULT_COUNTRY_RATE)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fault_types = [ft.strip() for ft in args.fault_types.split(",")]

    for ft in fault_types:
        if ft not in FAULT_HANDLERS:
            raise ValueError(
                f"Unknown fault type '{ft}'. Valid: {list(FAULT_HANDLERS.keys())}"
            )

    # Read clean data
    if not RIDES_CSV.exists():
        raise FileNotFoundError(
            f"{RIDES_CSV} not found. Run create_source_data.py first."
        )

    df = pd.read_csv(RIDES_CSV)
    original_count = len(df)
    print(f"Loaded {original_count:,} clean rides from {RIDES_CSV}")

    # Back up clean files
    for path in [RIDES_CSV, SEED_CSV]:
        backup = path.with_suffix(path.suffix + BACKUP_SUFFIX)
        if not backup.exists():
            shutil.copy2(path, backup)
            print(f"  Backed up {path} → {backup}")

    rng = np.random.default_rng(args.seed)
    rate_map = {
        "null": args.null_rate,
        "outlier": args.outlier_rate,
        "duplicate": args.duplicate_rate,
        "country": args.country_rate,
    }

    print(f"\nInjecting faults (seed={args.seed}):")
    summary = {}
    for ft in fault_types:
        handler = FAULT_HANDLERS[ft]
        rate = rate_map[ft]
        df, count = handler(df, rng, rate)
        summary[ft] = count
        print(f"  {ft:>12}: {count:,} rows affected (rate={rate})")

    # Sort to maintain stable ordering
    df = df.sort_values(["scooter_id", "start_time"]).reset_index(drop=True)

    # Write faulted data
    for col in ["start_time", "end_time"]:
        df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d %H:%M:%S")

    df.to_csv(RIDES_CSV, index=False)
    shutil.copy2(RIDES_CSV, SEED_CSV)
    print(f"\nWrote faulted rides to {RIDES_CSV} and {SEED_CSV}")
    print(f"Total rows: {len(df):,} (was {original_count:,})")

    print("\n=== Fault injection summary ===")
    for ft, count in summary.items():
        print(f"  {ft}: {count}")
    print(f"\nTo restore clean data, run:")
    print(f"  cp {RIDES_CSV}{BACKUP_SUFFIX} {RIDES_CSV}")
    print(f"  cp {SEED_CSV}{BACKUP_SUFFIX} {SEED_CSV}")


if __name__ == "__main__":
    main()
