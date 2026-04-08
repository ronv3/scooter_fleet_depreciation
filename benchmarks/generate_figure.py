"""
Generate the scalability benchmark figure from results CSV.

Produces a log-log plot with three lines:
  - Python baseline
  - dbt full-refresh
  - dbt incremental

The figure is saved as both PNG and PDF, designed to be readable in
grayscale (uses distinct line styles and markers).

Usage:
    python benchmarks/generate_figure.py [--input benchmarks/results.csv]
                                          [--output benchmarks/scalability]

If the results CSV does not exist, generates a placeholder figure with
example data so the thesis can be compiled before running benchmarks.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd


DEFAULT_INPUT = Path("benchmarks/results.csv")
DEFAULT_OUTPUT = Path("benchmarks/scalability")

# Placeholder data if results.csv doesn't exist yet
PLACEHOLDER_DATA = {
    "fleet_size": [90, 90, 90, 900, 900, 900, 4500, 4500, 4500, 9000, 9000, 9000],
    "ride_count": [
        131412, 131412, 131412,
        1314120, 1314120, 1314120,
        6570600, 6570600, 6570600,
        13141200, 13141200, 13141200,
    ],
    "approach": [
        "python_baseline", "dbt_full_refresh", "dbt_incremental",
        "python_baseline", "dbt_full_refresh", "dbt_incremental",
        "python_baseline", "dbt_full_refresh", "dbt_incremental",
        "python_baseline", "dbt_full_refresh", "dbt_incremental",
    ],
    "median_seconds": [
        # PLACEHOLDER VALUES — replace with actual measurements
        0.2, 3.5, 1.2,
        2.0, 35.0, 2.5,
        10.0, 180.0, 5.0,
        20.0, 400.0, 8.0,
    ],
}

# Plot style config: grayscale-safe, academic
APPROACH_STYLES = {
    "python_baseline": {
        "label": "Python script",
        "marker": "o",
        "linestyle": "-",
        "color": "#333333",
    },
    "dbt_full_refresh": {
        "label": "dbt full-refresh",
        "marker": "s",
        "linestyle": "--",
        "color": "#888888",
    },
    "dbt_incremental": {
        "label": "dbt incremental",
        "marker": "^",
        "linestyle": "-.",
        "color": "#555555",
    },
}


def generate_figure(input_path: Path, output_stem: Path) -> None:
    if input_path.exists():
        df = pd.read_csv(input_path)
        is_placeholder = False
    else:
        print(f"  {input_path} not found — using placeholder data")
        df = pd.DataFrame(PLACEHOLDER_DATA)
        is_placeholder = True

    fig, ax = plt.subplots(figsize=(6.5, 4.5))

    for approach, style in APPROACH_STYLES.items():
        subset = df[df["approach"] == approach].sort_values("ride_count")
        if subset.empty:
            continue

        ax.plot(
            subset["ride_count"],
            subset["median_seconds"],
            marker=style["marker"],
            linestyle=style["linestyle"],
            color=style["color"],
            label=style["label"],
            linewidth=1.5,
            markersize=6,
        )

    ax.set_xscale("log")
    ax.set_yscale("log")

    ax.set_xlabel("Number of rides (full year)", fontsize=10)
    ax.set_ylabel("Execution time (seconds)", fontsize=10)

    title = "Scalability comparison: execution time vs. data volume"
    if is_placeholder:
        title += "\n(placeholder data — replace with actual measurements)"
    ax.set_title(title, fontsize=11)

    ax.legend(fontsize=9, loc="upper left")
    ax.grid(True, which="both", linewidth=0.3, alpha=0.5)

    # Format tick labels nicely
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(
        lambda x, _: f"{x/1e6:.1f}M" if x >= 1e6 else f"{x/1e3:.0f}K"
    ))
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(
        lambda y, _: f"{y:.0f}s" if y >= 1 else f"{y*1000:.0f}ms"
    ))

    plt.tight_layout()

    # Save as PNG and PDF
    output_stem.parent.mkdir(parents=True, exist_ok=True)
    for ext in [".png", ".pdf"]:
        path = output_stem.with_suffix(ext)
        fig.savefig(path, dpi=300, bbox_inches="tight")
        print(f"  Saved: {path}")

    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate scalability benchmark figure"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Input results CSV (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output filename stem, without extension (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    generate_figure(args.input, args.output)
    print("Done.")


if __name__ == "__main__":
    main()
