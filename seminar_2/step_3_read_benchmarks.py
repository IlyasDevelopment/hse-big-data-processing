#!/usr/bin/env python3
"""
=============================================================================
  Step 3 — Read Benchmarks (Full Scan, Column Select, Filter, Aggregation)
=============================================================================

Reads data files written in Step 2 and benchmarks:
  1. Full-scan read time
  2. Column-selection read time  (only 3 columns)
  3. Filtered read time          (event_type == 'purchase')
  4. Aggregation time            (groupby country + event_type)

Key observations to look for:
  - Columnar formats (Parquet, ORC, Feather) read fewer columns MUCH faster
  - Parquet predicate push-down skips row groups that don't match the filter
  - Row-based formats (CSV, JSON) must read everything regardless

Usage:
    python step_3_read_benchmarks.py
"""

from common import (
    np, pd, pa, pq, os,
    HAS_ORC,
    SELECT_COLUMNS, FILTER_EVENT_TYPE,
    DATA_FILES_DIR, READ_RESULTS_PATH, WRITE_RESULTS_PATH,
    ensure_output_dir, time_it,
)

if HAS_ORC:
    from common import orc


# ============================================================================
#  Reader Functions
# ============================================================================

# ---- CSV -------------------------------------------------------------------

def read_csv(path): return pd.read_csv(path)
def read_csv_cols(path): return pd.read_csv(path, usecols=SELECT_COLUMNS)
def read_csv_filter(path):
    df = pd.read_csv(path)
    return df[df["event_type"] == FILTER_EVENT_TYPE]

def read_csv_gzip(path): return pd.read_csv(path, compression="gzip")
def read_csv_gzip_cols(path): return pd.read_csv(path, usecols=SELECT_COLUMNS, compression="gzip")
def read_csv_gzip_filter(path):
    df = pd.read_csv(path, compression="gzip")
    return df[df["event_type"] == FILTER_EVENT_TYPE]


# ---- JSON Lines ------------------------------------------------------------

def read_jsonl(path): return pd.read_json(path, lines=True)
def read_jsonl_cols(path):
    df = pd.read_json(path, lines=True)
    return df[SELECT_COLUMNS]
def read_jsonl_filter(path):
    df = pd.read_json(path, lines=True)
    return df[df["event_type"] == FILTER_EVENT_TYPE]

def read_jsonl_gzip(path): return pd.read_json(path, lines=True, compression="gzip")
def read_jsonl_gzip_cols(path):
    df = pd.read_json(path, lines=True, compression="gzip")
    return df[SELECT_COLUMNS]
def read_jsonl_gzip_filter(path):
    df = pd.read_json(path, lines=True, compression="gzip")
    return df[df["event_type"] == FILTER_EVENT_TYPE]


# ---- Parquet (columnar — supports push-down!) ------------------------------

def read_parquet(path): return pq.read_table(path).to_pandas()
def read_parquet_cols(path):
    # Only requested columns are read from disk — the big win
    return pq.read_table(path, columns=SELECT_COLUMNS).to_pandas()
def read_parquet_filter(path):
    # Predicate push-down: pyarrow skips row groups that don't match
    filters = [("event_type", "==", FILTER_EVENT_TYPE)]
    return pq.read_table(path, filters=filters).to_pandas()


# ---- ORC (columnar) -------------------------------------------------------

def read_orc(path):
    if not HAS_ORC: raise RuntimeError("pyarrow.orc not available")
    return orc.read_table(path).to_pandas()

def read_orc_cols(path):
    return orc.read_table(path, columns=SELECT_COLUMNS).to_pandas()
def read_orc_filter(path):
    df = orc.read_table(path).to_pandas()
    return df[df["event_type"] == FILTER_EVENT_TYPE]


# ============================================================================
#  Aggregation
# ============================================================================

def run_aggregation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Typical analytical query:
        SELECT country, event_type, COUNT(*), AVG(duration_sec), SUM(revenue)
        GROUP BY country, event_type
    """
    return df.groupby(["country", "event_type"]).agg(
        cnt=("event_id", "count"),
        avg_duration=("duration_sec", "mean"),
        total_revenue=("revenue", "sum"),
    ).reset_index()


# ============================================================================
#  Format Registry (read-only)
# ============================================================================

def get_read_formats():
    return [
        {"name": "CSV",                "ext": ".csv",             "reader": read_csv,         "reader_cols": read_csv_cols,         "reader_filt": read_csv_filter,         "available": True},
        {"name": "CSV + gzip",         "ext": ".csv.gz",          "reader": read_csv_gzip,    "reader_cols": read_csv_gzip_cols,    "reader_filt": read_csv_gzip_filter,    "available": True},
        {"name": "JSON Lines",         "ext": ".jsonl",           "reader": read_jsonl,       "reader_cols": read_jsonl_cols,        "reader_filt": read_jsonl_filter,        "available": True},
        {"name": "JSON Lines + gzip",  "ext": ".jsonl.gz",        "reader": read_jsonl_gzip,  "reader_cols": read_jsonl_gzip_cols,   "reader_filt": read_jsonl_gzip_filter,   "available": True},
        {"name": "Parquet (snappy)",   "ext": ".snappy.parquet",  "reader": read_parquet,     "reader_cols": read_parquet_cols,      "reader_filt": read_parquet_filter,      "available": True},
        {"name": "Parquet (gzip)",     "ext": ".gzip.parquet",    "reader": read_parquet,     "reader_cols": read_parquet_cols,      "reader_filt": read_parquet_filter,      "available": True},
        {"name": "Parquet (zstd)",     "ext": ".zstd.parquet",    "reader": read_parquet,     "reader_cols": read_parquet_cols,      "reader_filt": read_parquet_filter,      "available": True},
        {"name": "Parquet (none)",     "ext": ".none.parquet",    "reader": read_parquet,     "reader_cols": read_parquet_cols,      "reader_filt": read_parquet_filter,      "available": True},
        {"name": "ORC (snappy)",       "ext": ".snappy.orc",      "reader": read_orc,         "reader_cols": read_orc_cols,          "reader_filt": read_orc_filter,          "available": HAS_ORC},
        {"name": "ORC (zlib)",         "ext": ".zlib.orc",        "reader": read_orc,         "reader_cols": read_orc_cols,          "reader_filt": read_orc_filter,          "available": HAS_ORC},
    ]


# ============================================================================
#  Main
# ============================================================================

def main():
    ensure_output_dir()

    print("=" * 70)
    print("  STEP 3 — READ BENCHMARKS")
    print("=" * 70)

    if not os.path.exists(DATA_FILES_DIR):
        print("  ERROR: Data files not found. Run step_2_write_formats.py first.")
        return

    results = []

    for fmt in get_read_formats():
        name = fmt["name"]
        if not fmt["available"]:
            print(f"\n  SKIP  {name} (library not available)")
            continue

        file_path = os.path.join(DATA_FILES_DIR, f"benchmark{fmt['ext']}")
        if not os.path.exists(file_path):
            print(f"\n  SKIP  {name} (file not found: {file_path})")
            continue

        print(f"\n{'='*60}")
        print(f"  {name}")
        print(f"{'='*60}")

        # 1. Full read
        print(f"  Full scan …", end=" ", flush=True)
        try:
            read_sec, df_read = time_it(fmt["reader"], file_path)
            print(f"{read_sec:.2f}s  ({len(df_read):,} rows)")
        except Exception as e:
            print(f"FAILED ({e})")
            continue

        # 2. Column selection
        print(f"  Column select ({SELECT_COLUMNS}) …", end=" ", flush=True)
        try:
            cols_sec, df_cols = time_it(fmt["reader_cols"], file_path)
            print(f"{cols_sec:.2f}s  ({len(df_cols):,} rows, {len(df_cols.columns)} cols)")
        except Exception as e:
            print(f"FAILED ({e})")
            cols_sec = float("nan")

        # 3. Filtered read
        print(f"  Filter (event_type='{FILTER_EVENT_TYPE}') …", end=" ", flush=True)
        try:
            filt_sec, df_filt = time_it(fmt["reader_filt"], file_path)
            print(f"{filt_sec:.2f}s  ({len(df_filt):,} rows)")
        except Exception as e:
            print(f"FAILED ({e})")
            filt_sec = float("nan")

        # 4. Aggregation
        print(f"  Aggregation (groupby) …", end=" ", flush=True)
        try:
            agg_sec, _ = time_it(run_aggregation, df_read)
            print(f"{agg_sec:.2f}s")
        except Exception as e:
            print(f"FAILED ({e})")
            agg_sec = float("nan")

        results.append({
            "Format": name,
            "Read Full (s)": round(read_sec, 2),
            "Read Cols (s)": round(cols_sec, 2),
            "Read Filter (s)": round(filt_sec, 2),
            "Aggregation (s)": round(agg_sec, 2),
        })

        del df_read

    # Save & display results
    from tabulate import tabulate
    results_df = pd.DataFrame(results)

    print("\n" + "=" * 70)
    print("  READ BENCHMARK RESULTS")
    print("=" * 70)
    print(tabulate(results_df, headers="keys", tablefmt="pretty", showindex=False, floatfmt=".2f"))

    results_df.to_csv(READ_RESULTS_PATH, index=False)
    print(f"\n  Results saved to: {READ_RESULTS_PATH}")
    print("\n  Done! Run step_4_visualize_results.py next.")


if __name__ == "__main__":
    main()
