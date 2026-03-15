"""
Generate source data for scooter-ride financial data warehouse.

Outputs:
  - rides.csv          Billing / invoicing records (one row per ride)
  - account_mapping.csv  Chart-of-accounts + journal-line-type mapping

Design notes
------------
rides.csv represents what typically comes from a billing team: every completed
ride with its pricing breakdown (net amount, VAT, gross, coupon discount).
account_mapping.csv is the finance/accounting reference that tells the DWH
which general-ledger accounts to debit/credit when booking each ride.

Together they feed a dbt pipeline:
  staging → intermediate (journal entries) → mart (ledger) → reports (P&L, BS)
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
import uuid
from pathlib import Path
from datetime import date
from dateutil.relativedelta import relativedelta
from typing import Dict, Tuple, List

import numpy as np
import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

RANDOM_SEED = 42

# 90 scooters: 30 per city
SCOOTER_IDS = np.arange(1, 91, dtype=int)

# Ride generation
RIDES_PER_DAY_MIN = 2
RIDES_PER_DAY_MAX = 6

DURATION_MIN = 1
DURATION_MAX = 35
DURATION_MEAN = 14
DURATION_STD = 7

UNLOCK_FEE_EUR = 1.00          # included in the net amount
PER_MINUTE_RATE_EUR = 0.22

# Distance derivation
AVG_SPEED_KM_PER_MIN = 0.23    # ~13.8 km/h
DISTANCE_NOISE_STD = 0.08

# Service window (minutes from midnight)
SERVICE_START_MIN = 6 * 60      # 06:00
SERVICE_END_MIN = 23 * 60       # 23:00
RIDE_BUFFER_MIN = 5

# City mapping  (scooter_id ranges)
CITY_MAP = {
    (1, 30): ("Tallinn", "Estonia"),
    (31, 60): ("Helsinki", "Finland"),
    (61, 90): ("Riga", "Latvia"),
}

# Country-specific VAT rates
VAT_RATES = {
    "Estonia": 0.22,
    "Finland": 0.24,
    "Latvia": 0.21,
}

# Coupon rules
COUPON_PROBABILITY = 0.10       # ~1 in 10 eligible orders
COUPON_MIN_ORDER_AMOUNT = 5.00  # net amount must exceed this
COUPON_DISCOUNT_EUR = 3.00

# Output
OUTPUT_DIR = Path("data")
SEED_DIR = Path("dbt/seeds")


# ============================================================
# HELPERS
# ============================================================

def city_country_for_scooter(scooter_id: int) -> Tuple[str, str]:
    for (lo, hi), (city, country) in CITY_MAP.items():
        if lo <= scooter_id <= hi:
            return city, country
    raise ValueError(f"Unexpected scooter_id: {scooter_id}")


def clipped_normal_int(rng: np.random.Generator, mean: float, std: float, low: int, high: int) -> int:
    x = rng.normal(mean, std)
    return int(np.clip(np.rint(x), low, high))


def deterministic_ride_id(scooter_id: int, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> int:
    """Stable deterministic BIGINT-safe hash."""
    payload = f"{scooter_id}|{start_ts.isoformat()}|{end_ts.isoformat()}".encode()
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=False) & ((1 << 63) - 1)


def deterministic_order_id(scooter_id: int, start_ts: pd.Timestamp) -> str:
    """Deterministic UUID-style order ID from scooter + start time."""
    payload = f"order|{scooter_id}|{start_ts.isoformat()}".encode()
    return str(uuid.UUID(bytes=hashlib.md5(payload).digest()))


def deterministic_coupon_code(rng: np.random.Generator) -> str:
    """Random-looking but reproducible coupon code."""
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "COUP-" + "".join(rng.choice(list(chars)) for _ in range(6))


def generate_non_overlapping_intervals(
    rng: np.random.Generator,
    day_date: date,
    n_rides: int,
) -> List[Tuple[pd.Timestamp, pd.Timestamp, int]]:
    """Generate non-overlapping rides within the daily service window."""
    intervals: List[Tuple[pd.Timestamp, pd.Timestamp, int]] = []

    for _ in range(n_rides):
        accepted = False
        for _attempt in range(500):
            duration_min = clipped_normal_int(rng, DURATION_MEAN, DURATION_STD, DURATION_MIN, DURATION_MAX)
            latest_start = SERVICE_END_MIN - duration_min
            if latest_start < SERVICE_START_MIN:
                continue

            start_minute = int(rng.integers(SERVICE_START_MIN, latest_start + 1))
            end_minute = start_minute + duration_min

            overlaps = any(
                not (end_minute + RIDE_BUFFER_MIN <= (s.hour * 60 + s.minute)
                     or start_minute >= (e.hour * 60 + e.minute) + RIDE_BUFFER_MIN)
                for s, e, _ in intervals
            )
            if overlaps:
                continue

            start_ts = pd.Timestamp(day_date) + pd.Timedelta(minutes=start_minute)
            end_ts = start_ts + pd.Timedelta(minutes=duration_min)
            intervals.append((start_ts, end_ts, duration_min))
            accepted = True
            break

        if not accepted:
            if not intervals:
                fallback_start = SERVICE_START_MIN
            else:
                fallback_start = min(
                    max(e.hour * 60 + e.minute for _, e, _ in intervals) + RIDE_BUFFER_MIN,
                    SERVICE_END_MIN - 1,
                )
            dur = max(1, min(10, SERVICE_END_MIN - fallback_start))
            s = pd.Timestamp(day_date) + pd.Timedelta(minutes=fallback_start)
            intervals.append((s, s + pd.Timedelta(minutes=dur), dur))

    intervals.sort(key=lambda x: x[0])
    return intervals


# ============================================================
# RIDES GENERATION
# ============================================================

def generate_rides_df(
    seed: int = RANDOM_SEED,
    ride_start: date = date(2026, 1, 1),
    ride_end_exclusive: date = date(2026, 2, 1),
) -> pd.DataFrame:
    ride_dates = pd.date_range(
        ride_start,
        pd.Timestamp(ride_end_exclusive) - pd.Timedelta(days=1),
        freq="D",
    ).date

    rng = np.random.default_rng(seed)
    rows = []

    for day in ride_dates:
        for sid in SCOOTER_IDS:
            scooter_id = int(sid)
            n_rides = int(rng.integers(RIDES_PER_DAY_MIN, RIDES_PER_DAY_MAX + 1))
            intervals = generate_non_overlapping_intervals(rng, day, n_rides)
            city, country = city_country_for_scooter(scooter_id)
            vat_rate = VAT_RATES[country]

            for start_ts, end_ts, duration_min in intervals:
                # Distance
                noise = float(np.clip(rng.normal(1.0, DISTANCE_NOISE_STD), 0.65, 1.35))
                distance_km = round(max(0.05, duration_min * AVG_SPEED_KM_PER_MIN * noise), 3)

                # Pricing (net amount includes unlock fee)
                amount = round(UNLOCK_FEE_EUR + duration_min * PER_MINUTE_RATE_EUR, 2)
                vat_amount = round(amount * vat_rate, 2)
                sum_with_vat = round(amount + vat_amount, 2)

                # Coupon logic: ~10% chance if net amount > 5 EUR
                coupon_used = None
                coupon_amount = None
                if amount > COUPON_MIN_ORDER_AMOUNT and rng.random() < COUPON_PROBABILITY:
                    coupon_used = deterministic_coupon_code(rng)
                    coupon_amount = COUPON_DISCOUNT_EUR

                ride_id = deterministic_ride_id(scooter_id, start_ts, end_ts)
                order_id = deterministic_order_id(scooter_id, start_ts)

                rows.append({
                    "order_id": order_id,
                    "ride_id": ride_id,
                    "scooter_id": scooter_id,
                    "start_time": start_ts,
                    "end_time": end_ts,
                    "duration_min": duration_min,
                    "distance_km": distance_km,
                    "amount": amount,
                    "vat_rate": vat_rate,
                    "vat_amount": vat_amount,
                    "sum_with_vat_amount": sum_with_vat,
                    "currency": "EUR",
                    "coupon_used": coupon_used,
                    "coupon_amount": coupon_amount,
                    "city": city,
                    "country": country,
                })

    df = pd.DataFrame(rows)
    df = df.sort_values(["scooter_id", "start_time"]).reset_index(drop=True)
    return df


# ============================================================
# ACCOUNT MAPPING
# ============================================================

def generate_account_mapping_df() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns two dataframes:
      - account_mapping_df: line_type / country / account_code (join key to chart_of_accounts)
      - chart_of_accounts_df: canonical account master (one row per account_code)

    Journal lines per ride:
      1. receivable    (DR)  — Accounts Receivable = sum_with_vat − coupon_amount
      2. coupon_expense (DR) — Marketing Expense   = coupon_amount (0 if none)
      3. revenue        (CR) — Ride Revenue        = amount (net, pre-VAT)
      4. vat_payable    (CR) — VAT Payable         = vat_amount

    Debits always equal credits:
      (sum_with_vat − coupon) + coupon = amount + vat ✓
    """
    countries = ["Estonia", "Finland", "Latvia"]
    vat_account_codes = {"Estonia": "2101", "Finland": "2102", "Latvia": "2103"}
    revenue_account_codes = {"Estonia": "4101", "Finland": "4102", "Latvia": "4103"}

    mapping_rows = []
    account_rows: Dict[str, dict] = {}

    def add_account(code: str, name: str, category: str, normal_side: str) -> None:
        account_rows[code] = {
            "account_code": code,
            "account_name": name,
            "account_category": category,
            "normal_side": normal_side,
        }

    for country in countries:
        mapping_rows.append({"line_type": "receivable",    "country": country, "account_code": "1200"})
        mapping_rows.append({"line_type": "revenue",       "country": country, "account_code": revenue_account_codes[country]})
        mapping_rows.append({"line_type": "vat_payable",   "country": country, "account_code": vat_account_codes[country]})
        mapping_rows.append({"line_type": "coupon_expense","country": country, "account_code": "6200"})

        add_account("1200", "Accounts Receivable",             "asset",     "debit")
        add_account(revenue_account_codes[country], f"Ride Revenue - {country}", "revenue", "credit")
        add_account(vat_account_codes[country],     f"VAT Payable - {country}",  "liability","credit")
        add_account("6200", "Marketing Expense - Coupons",     "expense",   "debit")

    # Retained Earnings is needed for balance sheet but has no journal line type
    add_account("3000", "Retained Earnings", "equity", "credit")

    account_df = pd.DataFrame(sorted(account_rows.values(), key=lambda r: r["account_code"]))
    return pd.DataFrame(mapping_rows), account_df


# ============================================================
# VALIDATIONS
# ============================================================

def run_validations(rides_df: pd.DataFrame, mapping_df: pd.DataFrame) -> Dict[str, object]:
    out: Dict[str, object] = {}

    out["rides_row_count"] = len(rides_df)
    out["mapping_row_count"] = len(mapping_df)
    out["ride_id_unique"] = bool(rides_df["ride_id"].is_unique)
    out["order_id_unique"] = bool(rides_df["order_id"].is_unique)

    # VAT sanity: vat_amount ≈ amount * vat_rate (within rounding tolerance)
    vat_check = (rides_df["vat_amount"] - (rides_df["amount"] * rides_df["vat_rate"]).round(2)).abs()
    out["vat_calculation_max_diff"] = float(vat_check.max())
    out["vat_calculation_ok"] = float(vat_check.max()) < 0.02

    # sum_with_vat = amount + vat_amount
    sum_check = (rides_df["sum_with_vat_amount"] - (rides_df["amount"] + rides_df["vat_amount"]).round(2)).abs()
    out["sum_with_vat_max_diff"] = float(sum_check.max())
    out["sum_with_vat_ok"] = float(sum_check.max()) < 0.02

    # Coupon rules: only applied when amount > 5
    coupon_rows = rides_df[rides_df["coupon_used"].notna()]
    out["coupon_count"] = len(coupon_rows)
    if len(coupon_rows) > 0:
        out["coupon_min_order_amount"] = float(coupon_rows["amount"].min())
        out["coupon_all_above_threshold"] = bool((coupon_rows["amount"] > COUPON_MIN_ORDER_AMOUNT).all())
        out["coupon_amount_all_3"] = bool((coupon_rows["coupon_amount"] == 3.0).all())
    else:
        out["coupon_all_above_threshold"] = True
        out["coupon_amount_all_3"] = True

    # Double-entry balance check (preview):
    # For each ride: sum_with_vat = amount + vat_amount (debits = credits before coupon split)
    # With coupon: (sum_with_vat - coupon) + coupon = amount + vat  ✓
    total_debit = (
        rides_df["sum_with_vat_amount"] - rides_df["coupon_amount"].fillna(0)  # AR
        + rides_df["coupon_amount"].fillna(0)                                   # coupon expense
    ).sum()
    total_credit = (rides_df["amount"] + rides_df["vat_amount"]).sum()
    out["double_entry_balance_diff"] = round(abs(total_debit - total_credit), 4)
    out["double_entry_balanced"] = abs(total_debit - total_credit) < 0.01

    # Rides per day range
    rides_work = rides_df.copy()
    rides_work["ride_date"] = pd.to_datetime(rides_work["start_time"]).dt.date
    daily = rides_work.groupby(["scooter_id", "ride_date"]).size()
    out["rides_per_day_min"] = int(daily.min())
    out["rides_per_day_max"] = int(daily.max())
    out["rides_per_day_in_range"] = bool(daily.between(RIDES_PER_DAY_MIN, RIDES_PER_DAY_MAX).all())

    # City / country distribution
    out["rides_per_country"] = rides_df["country"].value_counts().to_dict()

    # Mapping completeness: every (line_type, country) combo exists
    expected_combos = {("receivable", c) for c in VAT_RATES} | \
                      {("revenue", c) for c in VAT_RATES} | \
                      {("vat_payable", c) for c in VAT_RATES} | \
                      {("coupon_expense", c) for c in VAT_RATES}
    actual_combos = set(zip(mapping_df["line_type"], mapping_df["country"]))
    out["mapping_complete"] = expected_combos == actual_combos

    return out


# ============================================================
# CSV EXPORT
# ============================================================

def save_csvs(
    rides_df: pd.DataFrame,
    mapping_df: pd.DataFrame,
    accounts_df: pd.DataFrame,
    output_dir: Path = OUTPUT_DIR,
    seed_dir: Path = SEED_DIR,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    seed_dir.mkdir(parents=True, exist_ok=True)

    rides_out = rides_df.copy()
    for col in ["start_time", "end_time"]:
        rides_out[col] = pd.to_datetime(rides_out[col]).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Write to data/ (archive / source-of-truth copy)
    rides_out.to_csv(output_dir / "rides.csv", index=False)
    mapping_df.to_csv(output_dir / "account_mapping.csv", index=False)
    accounts_df.to_csv(output_dir / "chart_of_accounts.csv", index=False)

    # Copy rides.csv to dbt/seeds/ so `dbt seed` picks it up.
    # account_mapping and chart_of_accounts are static reference data
    # and only need updating when the account structure changes.
    shutil.copy2(output_dir / "rides.csv", seed_dir / "rides.csv")


# ============================================================
# MAIN
# ============================================================

def _last_month_range() -> Tuple[date, date]:
    today = date.today()
    first_of_this_month = today.replace(day=1)
    first_of_last_month = first_of_this_month - relativedelta(months=1)
    return first_of_last_month, first_of_this_month


def _parse_args() -> Tuple[date, date]:
    parser = argparse.ArgumentParser(description="Generate scooter ride source data.")
    parser.add_argument("--start-date", type=date.fromisoformat, help="Start date (inclusive), YYYY-MM-DD")
    parser.add_argument("--end-date",   type=date.fromisoformat, help="End date (exclusive), YYYY-MM-DD")
    args = parser.parse_args()

    if args.start_date and args.end_date:
        return args.start_date, args.end_date
    if args.start_date or args.end_date:
        parser.error("Provide both --start-date and --end-date, or neither.")
    return _last_month_range()


def main() -> None:
    ride_start, ride_end_exclusive = _parse_args()

    rides_df = generate_rides_df(seed=RANDOM_SEED, ride_start=ride_start, ride_end_exclusive=ride_end_exclusive)
    mapping_df, accounts_df = generate_account_mapping_df()

    validations = run_validations(rides_df, mapping_df)

    save_csvs(rides_df, mapping_df, accounts_df, output_dir=OUTPUT_DIR)

    print(f"Period: {ride_start} → {ride_end_exclusive} (exclusive)")
    print(f"Generated CSVs in: {OUTPUT_DIR.resolve()}")
    print(f"\n=== rides.csv: {len(rides_df):,} rows ===")
    print(rides_df.head(5).to_string(index=False))

    print(f"\n=== account_mapping.csv: {len(mapping_df)} rows ===")
    print(mapping_df.to_string(index=False))

    print(f"\n=== chart_of_accounts.csv: {len(accounts_df)} rows ===")
    print(accounts_df.to_string(index=False))

    print("\n=== Validation summary ===")
    all_ok = True
    for k, v in validations.items():
        status = ""
        if isinstance(v, bool):
            status = " ✓" if v else " ✗"
            if not v:
                all_ok = False
        print(f"  {k}: {v}{status}")

    print(f"\nAll validations passed: {'YES' if all_ok else 'NO'}")


if __name__ == "__main__":
    main()
