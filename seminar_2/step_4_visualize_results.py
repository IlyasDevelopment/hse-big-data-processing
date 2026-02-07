#!/usr/bin/env python3
"""
=============================================================================
  Step 4 — Combine Results & Generate Charts
=============================================================================

Merges write results (Step 2) and read results (Step 3) into one table,
prints a summary, and generates comparison bar charts.

Usage:
    python step_4_visualize_results.py
"""

from common import (
    pd, os,
    WRITE_RESULTS_PATH, READ_RESULTS_PATH, FULL_RESULTS_PATH,
    DEFAULT_OUTPUT_DIR, plt,
    ensure_output_dir,
)
from tabulate import tabulate


# ============================================================================
#  Merge Results
# ============================================================================

def load_and_merge_results() -> pd.DataFrame:
    """Load write + read CSVs and merge on Format name."""
    if not os.path.exists(WRITE_RESULTS_PATH):
        print(f"  ERROR: {WRITE_RESULTS_PATH} not found. Run step_2 first.")
        return pd.DataFrame()
    if not os.path.exists(READ_RESULTS_PATH):
        print(f"  ERROR: {READ_RESULTS_PATH} not found. Run step_3 first.")
        return pd.DataFrame()

    write_df = pd.read_csv(WRITE_RESULTS_PATH)
    read_df = pd.read_csv(READ_RESULTS_PATH)

    merged = pd.merge(write_df, read_df, on="Format", how="outer")
    return merged


# ============================================================================
#  Charts
# ============================================================================

def generate_charts(results_df: pd.DataFrame, output_dir: str) -> None:
    if plt is None:
        print("  Skipping chart generation (matplotlib not installed).")
        return

    charts_dir = os.path.join(output_dir, "charts")
    os.makedirs(charts_dir, exist_ok=True)

    plt.style.use("seaborn-v0_8-whitegrid")

    metrics = [
        ("File Size (MB)", "File Size Comparison", "file_size.png", "steelblue"),
        ("Write Time (s)", "Write Time Comparison", "write_time.png", "darkorange"),
        ("Read Full (s)", "Full Read Time Comparison", "read_full.png", "seagreen"),
        ("Read Cols (s)", "Column Selection Read Time", "read_cols.png", "mediumpurple"),
        ("Read Filter (s)", "Filtered Read Time Comparison", "read_filter.png", "indianred"),
        ("Aggregation (s)", "Aggregation Time After Read", "aggregation.png", "goldenrod"),
    ]

    for col, title, filename, color in metrics:
        if col not in results_df.columns:
            continue

        subset = results_df.dropna(subset=[col])
        if subset.empty:
            continue

        fig, ax = plt.subplots(figsize=(12, max(6, len(subset) * 0.45)))
        bars = ax.barh(subset["Format"], subset[col], color=color, edgecolor="white")

        for bar, val in zip(bars, subset[col]):
            if pd.notna(val):
                ax.text(
                    bar.get_width() + ax.get_xlim()[1] * 0.01,
                    bar.get_y() + bar.get_height() / 2,
                    f"{val:.2f}",
                    va="center",
                    fontsize=9,
                )

        ax.set_xlabel(col)
        ax.set_title(title, fontsize=14, fontweight="bold")
        ax.invert_yaxis()
        plt.tight_layout()

        chart_path = os.path.join(charts_dir, filename)
        fig.savefig(chart_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"  Chart saved: {chart_path}")


# ============================================================================
#  Main
# ============================================================================

def main():
    ensure_output_dir()

    print("=" * 70)
    print("  STEP 4 — VISUALIZE & COMPARE RESULTS")
    print("=" * 70)

    results_df = load_and_merge_results()
    if results_df.empty:
        return

    # Print full table
    print("\n" + "=" * 90)
    print("  FULL BENCHMARK RESULTS")
    print("=" * 90)
    print(tabulate(
        results_df,
        headers="keys",
        tablefmt="pretty",
        showindex=False,
        floatfmt=".2f",
    ))

    # Save merged results
    results_df.to_csv(FULL_RESULTS_PATH, index=False)
    print(f"\n  Full results saved to: {FULL_RESULTS_PATH}")

    # Generate charts
    print("\n  Generating charts …")
    generate_charts(results_df, DEFAULT_OUTPUT_DIR)

    # Key takeaways
    print("\n" + "=" * 70)
    print("  KEY TAKEAWAYS")
    print("=" * 70)
    if "File Size (MB)" in results_df.columns:
        smallest = results_df.loc[results_df["File Size (MB)"].idxmin()]
        largest = results_df.loc[results_df["File Size (MB)"].idxmax()]
        print(f"  Smallest file:  {smallest['Format']} ({smallest['File Size (MB)']:.1f} MB)")
        print(f"  Largest file:   {largest['Format']} ({largest['File Size (MB)']:.1f} MB)")
        print(f"  Ratio:          {largest['File Size (MB)'] / smallest['File Size (MB)']:.1f}x")
    if "Read Cols (s)" in results_df.columns and "Read Full (s)" in results_df.columns:
        print("\n  Column projection speedup (Read Cols vs Read Full):")
        for _, row in results_df.iterrows():
            if pd.notna(row.get("Read Full (s)")) and pd.notna(row.get("Read Cols (s)")) and row["Read Cols (s)"] > 0:
                speedup = row["Read Full (s)"] / row["Read Cols (s)"]
                print(f"    {row['Format']:25s}  {speedup:.2f}x")

    print("\n  Done!")


if __name__ == "__main__":
    main()
