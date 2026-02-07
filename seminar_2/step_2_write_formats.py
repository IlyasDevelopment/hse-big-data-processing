#!/usr/bin/env python3
"""
=============================================================================
  Step 2 — Write Data to All Formats & Compare Sizes
=============================================================================

Loads the generated data from Step 1, writes it to every format, and
measures write time + file size. Results are saved to a CSV.

After running, inspect the data_files/ folder to see the actual files and
compare their sizes.

Usage:
    python step_2_write_formats.py
"""

from common import (
    np, pd, pa, pq, feather, json, os, time,
    HAS_ORC, fastavro,
    GENERATED_DATA_PATH, DATA_FILES_DIR, WRITE_RESULTS_PATH,
    ensure_output_dir, time_it, file_size_mb,
)

# Only import orc if available
if HAS_ORC:
    from common import orc


# ============================================================================
#  Writer Functions
# ============================================================================

def write_csv(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, index=False)


def write_csv_gzip(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, index=False, compression="gzip")


def write_jsonl(df: pd.DataFrame, path: str) -> None:
    df.to_json(path, orient="records", lines=True, date_format="iso")


def write_jsonl_gzip(df: pd.DataFrame, path: str) -> None:
    df.to_json(path, orient="records", lines=True, date_format="iso", compression="gzip")


def write_parquet(df: pd.DataFrame, path: str, compression: str = "snappy") -> None:
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path, compression=compression)


def write_avro(df: pd.DataFrame, path: str, codec: str = "snappy") -> None:
    if fastavro is None:
        raise RuntimeError("fastavro is not installed")

    avro_schema = {
        "type": "record",
        "name": "Event",
        "fields": [
            {"name": "event_id", "type": "long"},
            {"name": "user_id", "type": "long"},
            {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            {"name": "event_type", "type": "string"},
            {"name": "page_url", "type": "string"},
            {"name": "duration_sec", "type": ["null", "double"], "default": None},
            {"name": "revenue", "type": ["null", "double"], "default": None},
            {"name": "device", "type": "string"},
            {"name": "country", "type": "string"},
            {"name": "session_tags", "type": "string"},
        ],
    }
    parsed_schema = fastavro.parse_schema(avro_schema)

    records = []
    for row in df.itertuples(index=False):
        rec = {
            "event_id": int(row.event_id),
            "user_id": int(row.user_id),
            "timestamp": int(row.timestamp.timestamp() * 1000),
            "event_type": row.event_type,
            "page_url": row.page_url,
            "duration_sec": None if pd.isna(row.duration_sec) else float(row.duration_sec),
            "revenue": None if pd.isna(row.revenue) else float(row.revenue),
            "device": row.device,
            "country": row.country,
            "session_tags": row.session_tags,
        }
        records.append(rec)

    with open(path, "wb") as f:
        fastavro.writer(f, parsed_schema, records, codec=codec)


def write_orc(df: pd.DataFrame, path: str, compression: str = "SNAPPY") -> None:
    if not HAS_ORC:
        raise RuntimeError("pyarrow.orc is not available")
    df_copy = df.copy()
    df_copy["duration_sec"] = df_copy["duration_sec"].astype("float64")
    df_copy["revenue"] = df_copy["revenue"].astype("float64")
    table = pa.Table.from_pandas(df_copy)
    orc.write_table(table, path, compression=compression)


def write_feather(df: pd.DataFrame, path: str, compression: str | None = None) -> None:
    feather.write_feather(df, path, compression=compression)


# ============================================================================
#  Format Registry (write-only)
# ============================================================================

def get_write_formats():
    return [
        {"name": "CSV",                "ext": ".csv",             "writer": write_csv,                                              "available": True},
        {"name": "CSV + gzip",         "ext": ".csv.gz",          "writer": write_csv_gzip,                                         "available": True},
        {"name": "JSON Lines",         "ext": ".jsonl",           "writer": write_jsonl,                                             "available": True},
        {"name": "JSON Lines + gzip",  "ext": ".jsonl.gz",        "writer": write_jsonl_gzip,                                        "available": True},
        {"name": "Parquet (snappy)",   "ext": ".snappy.parquet",  "writer": lambda df, p: write_parquet(df, p, compression="snappy"), "available": True},
        {"name": "Parquet (gzip)",     "ext": ".gzip.parquet",    "writer": lambda df, p: write_parquet(df, p, compression="gzip"),   "available": True},
        {"name": "Parquet (zstd)",     "ext": ".zstd.parquet",    "writer": lambda df, p: write_parquet(df, p, compression="zstd"),   "available": True},
        {"name": "Parquet (none)",     "ext": ".none.parquet",    "writer": lambda df, p: write_parquet(df, p, compression="none"),   "available": True},
        {"name": "Avro (snappy)",      "ext": ".snappy.avro",     "writer": lambda df, p: write_avro(df, p, codec="snappy"),          "available": fastavro is not None},
        {"name": "Avro (deflate)",     "ext": ".deflate.avro",    "writer": lambda df, p: write_avro(df, p, codec="deflate"),         "available": fastavro is not None},
        {"name": "ORC (snappy)",       "ext": ".snappy.orc",      "writer": lambda df, p: write_orc(df, p, compression="SNAPPY"),     "available": HAS_ORC},
        {"name": "ORC (zlib)",         "ext": ".zlib.orc",        "writer": lambda df, p: write_orc(df, p, compression="ZLIB"),       "available": HAS_ORC},
    ]


# ============================================================================
#  Main
# ============================================================================

def main():
    ensure_output_dir()

    print("=" * 70)
    print("  STEP 2 — WRITE DATA TO ALL FORMATS")
    print("=" * 70)

    # Load generated data
    print(f"\n  Loading data from: {GENERATED_DATA_PATH}")
    if not os.path.exists(GENERATED_DATA_PATH):
        print("  ERROR: Generated data not found. Run step_1_generate_data.py first.")
        return
    df = pq.read_table(GENERATED_DATA_PATH).to_pandas()
    print(f"  Loaded {len(df):,} rows")

    # Write to each format
    from tabulate import tabulate
    results = []

    for fmt in get_write_formats():
        name = fmt["name"]

        if not fmt["available"]:
            print(f"\n  SKIP  {name} (library not available)")
            continue

        file_path = os.path.join(DATA_FILES_DIR, f"benchmark{fmt['ext']}")

        print(f"\n  Writing {name} …", end=" ", flush=True)
        try:
            write_sec, _ = time_it(fmt["writer"], df, file_path)
        except Exception as e:
            print(f"FAILED ({e})")
            continue
        size_mb = file_size_mb(file_path)
        print(f"{write_sec:.2f}s  ({size_mb:.1f} MB)")

        results.append({
            "Format": name,
            "File Size (MB)": round(size_mb, 2),
            "Write Time (s)": round(write_sec, 2),
        })

    # Save & display results
    results_df = pd.DataFrame(results)

    print("\n" + "=" * 70)
    print("  WRITE BENCHMARK RESULTS")
    print("=" * 70)
    print(tabulate(results_df, headers="keys", tablefmt="pretty", showindex=False, floatfmt=".2f"))

    results_df.to_csv(WRITE_RESULTS_PATH, index=False)
    print(f"\n  Results saved to: {WRITE_RESULTS_PATH}")
    print(f"  Data files in: {DATA_FILES_DIR}/")
    print("\n  Done! Run step_3_read_benchmarks.py next.")


if __name__ == "__main__":
    main()
