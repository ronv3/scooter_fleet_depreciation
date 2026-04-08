"""
Python baseline: compute income statement directly from rides.csv.

This script represents the simplest reasonable approach to producing an
income statement from ride data — the kind of solution a developer would
write if asked "get me the income statement from this CSV by Friday."

It reads the rides CSV, applies the accounting logic (revenue recognition,
VAT, coupon expense), and outputs a summary income statement matching the
pipeline's format.  No layered architecture, no incremental loading, no
database — just pandas.

The output must match the dbt pipeline's income statement totals exactly:
  Total Revenue:  536,507.46  (at 90-scooter baseline scale)
  Total Expenses:  10,188.00
  Net Income:     526,319.46

Usage:
    python benchmarks/baseline_income_statement.py [--rides-csv data/rides.csv] [--validate]
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd


# Known correct totals at baseline (90 scooters, full year)
EXPECTED_REVENUE = 536507.46
EXPECTED_EXPENSES = 10188.00
EXPECTED_NET_INCOME = 526319.46

# Country-specific revenue accounts (mirrors the pipeline's account mapping)
REVENUE_ACCOUNTS = {
    "Estonia": ("4101", "Ride Revenue -- Estonia"),
    "Finland": ("4102", "Ride Revenue -- Finland"),
    "Latvia":  ("4103", "Ride Revenue -- Latvia"),
}


def compute_income_statement(rides_csv: Path) -> dict:
    """
    Read rides CSV and compute income statement.

    Returns a dict with:
      - detail: list of dicts with section, account_code, account_name, country, amount
      - total_revenue: float
      - total_expenses: float
      - net_income: float
      - elapsed_seconds: float (wall-clock time for the computation)
    """
    t0 = time.perf_counter()

    df = pd.read_csv(rides_csv)

    # Revenue: sum of net amounts (pre-VAT) by country
    revenue_by_country = df.groupby("country")["amount"].sum()

    detail = []
    total_revenue = 0.0
    for country, amount in revenue_by_country.items():
        code, name = REVENUE_ACCOUNTS[country]
        rounded = round(float(amount), 2)
        detail.append({
            "section": "Revenue",
            "account_code": code,
            "account_name": name,
            "country": country,
            "amount": rounded,
        })
        total_revenue += rounded

    # Expenses: coupon amounts (marketing expense)
    # Coupons are applied when coupon_used is not NaN
    coupon_expenses = (
        df[df["coupon_used"].notna()]
        .groupby("country")["coupon_amount"]
        .sum()
    )

    total_expenses = 0.0
    for country, amount in coupon_expenses.items():
        rounded = round(float(amount), 2)
        detail.append({
            "section": "Expenses",
            "account_code": "6200",
            "account_name": f"Marketing Expense -- {country}",
            "country": country,
            "amount": rounded,
        })
        total_expenses += rounded

    total_revenue = round(total_revenue, 2)
    total_expenses = round(total_expenses, 2)
    net_income = round(total_revenue - total_expenses, 2)

    elapsed = time.perf_counter() - t0

    return {
        "detail": detail,
        "total_revenue": total_revenue,
        "total_expenses": total_expenses,
        "net_income": net_income,
        "elapsed_seconds": elapsed,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute income statement from rides CSV (Python baseline)"
    )
    parser.add_argument(
        "--rides-csv",
        type=Path,
        default=Path("data/rides.csv"),
        help="Path to rides CSV (default: data/rides.csv)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate against known correct totals and exit with error if mismatch",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Only print totals (for benchmark harness)",
    )
    args = parser.parse_args()

    result = compute_income_statement(args.rides_csv)

    if not args.quiet:
        print("=== Income Statement (Python Baseline) ===\n")

        print("Revenue:")
        for item in result["detail"]:
            if item["section"] == "Revenue":
                print(f"  {item['account_name']:>35s}  {item['amount']:>12,.2f} EUR")
        print(f"  {'Total Revenue':>35s}  {result['total_revenue']:>12,.2f} EUR")

        print("\nExpenses:")
        for item in result["detail"]:
            if item["section"] == "Expenses":
                print(f"  {item['account_name']:>35s}  {item['amount']:>12,.2f} EUR")
        print(f"  {'Total Expenses':>35s}  {result['total_expenses']:>12,.2f} EUR")

        print(f"\n  {'Net Income':>35s}  {result['net_income']:>12,.2f} EUR")
        print(f"\n  Computed in {result['elapsed_seconds']:.3f} seconds")

    else:
        print(
            f"{result['total_revenue']:.2f},"
            f"{result['total_expenses']:.2f},"
            f"{result['net_income']:.2f},"
            f"{result['elapsed_seconds']:.4f}"
        )

    if args.validate:
        errors = []
        if abs(result["total_revenue"] - EXPECTED_REVENUE) > 0.01:
            errors.append(
                f"Revenue mismatch: got {result['total_revenue']:.2f}, "
                f"expected {EXPECTED_REVENUE:.2f}"
            )
        if abs(result["total_expenses"] - EXPECTED_EXPENSES) > 0.01:
            errors.append(
                f"Expenses mismatch: got {result['total_expenses']:.2f}, "
                f"expected {EXPECTED_EXPENSES:.2f}"
            )
        if abs(result["net_income"] - EXPECTED_NET_INCOME) > 0.01:
            errors.append(
                f"Net income mismatch: got {result['net_income']:.2f}, "
                f"expected {EXPECTED_NET_INCOME:.2f}"
            )

        if errors:
            print("\n=== VALIDATION FAILED ===")
            for e in errors:
                print(f"  ERROR: {e}")
            sys.exit(1)
        else:
            print("\n=== VALIDATION PASSED ===")
            print("  All totals match the dbt pipeline output.")


if __name__ == "__main__":
    main()
