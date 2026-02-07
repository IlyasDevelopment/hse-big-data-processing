#!/usr/bin/env python3
"""
=============================================================================
  Big Data File Format Benchmark Script
  Seminar 2 — Practical Session (~20-30 minutes)
=============================================================================

This script generates a synthetic dataset and benchmarks reading/writing
across many popular big data file formats. Students will see first-hand
how format choice affects file size, write speed, read speed, columnar
projection, predicate push-down, and aggregation performance.

DESIGN NOTE — Memory Efficiency
    The script NEVER holds the full dataset in memory at once.  Data is
    generated in chunks and streamed directly to a canonical Parquet file.
    Write benchmarks then read that Parquet file in batches and stream to
    each target format.  This allows benchmarking multi-GB datasets on
    machines with modest RAM (8-16 GB is enough).

Requirements (install via pip):
    pip install -r requirements.txt

    Needed packages:
        pandas >= 2.0
        pyarrow >= 14.0
        fastavro >= 1.9
        matplotlib >= 3.7
        tabulate >= 0.9
        numpy >= 1.24
        cramjam >= 2.6   (needed by fastavro for snappy codec)

Usage examples:
    # Full benchmark (~3 GB of CSV data, takes 15-30 min)
    python benchmark_formats.py

    # Quick classroom demo (~500 MB, takes 5-7 min)
    python benchmark_formats.py --quick

    # Custom size, keep generated files for inspection
    python benchmark_formats.py --num-rows 5000000 --keep

    # Specify output directory
    python benchmark_formats.py --quick --output-dir ./my_results
"""

import argparse
import gc
import gzip
import json
import os
import shutil
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
#  Dependency checks — give students clear messages if something is missing
# ---------------------------------------------------------------------------

try:
    import numpy as np
except ImportError:
    sys.exit(
        "ERROR: numpy is not installed.  Run:  pip install numpy"
    )

try:
    import pandas as pd
except ImportError:
    sys.exit(
        "ERROR: pandas is not installed.  Run:  pip install pandas"
    )

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.feather as feather
except ImportError:
    sys.exit(
        "ERROR: pyarrow is not installed.  Run:  pip install pyarrow"
    )

# ORC support lives inside pyarrow but may not be compiled on every platform.
try:
    import pyarrow.orc as orc

    HAS_ORC = True
except ImportError:
    HAS_ORC = False
    print(
        "WARNING: pyarrow.orc is not available on this platform. "
        "ORC benchmarks will be skipped."
    )

try:
    import fastavro
except ImportError:
    fastavro = None
    print(
        "WARNING: fastavro is not installed. Avro benchmarks will be skipped. "
        "Install with:  pip install fastavro"
    )

try:
    from tabulate import tabulate
except ImportError:
    sys.exit(
        "ERROR: tabulate is not installed.  Run:  pip install tabulate"
    )

try:
    import matplotlib

    matplotlib.use("Agg")  # non-interactive backend — no GUI needed
    import matplotlib.pyplot as plt
except ImportError:
    plt = None
    print(
        "WARNING: matplotlib is not installed. Charts will be skipped. "
        "Install with:  pip install matplotlib"
    )


# ============================================================================
#  SECTION 1 — Synthetic Data Generation
# ============================================================================
#
#  We model *user interaction events* on a web platform.  Each row represents
#  one event (page view, click, purchase, etc.) with realistic column types:
#
#   - event_id        : monotonically increasing INT64
#   - user_id         : INT64  (IDs between 1 and 1 000 000)
#   - timestamp       : DATETIME  (over the last 365 days)
#   - event_type      : STRING categorical  (5 categories)
#   - page_url        : STRING  (simulated URL path)
#   - duration_sec    : FLOAT64  (time on page, may be null)
#   - revenue         : FLOAT64  (purchase amount, mostly null)
#   - device          : STRING categorical  (mobile / desktop / tablet)
#   - country         : STRING categorical  (10 countries)
#   - session_tags    : STRING  (JSON-encoded list — nested-like data)
#
#  The mix of integers, floats, categoricals, timestamps, nullable fields,
#  and a nested-like JSON column makes this realistic for benchmarking how
#  different formats handle diverse data types.
# ============================================================================

# Constants for data generation
EVENT_TYPES = ["page_view", "click", "scroll", "purchase", "logout"]
DEVICES = ["mobile", "desktop", "tablet"]
COUNTRIES = [
    "US", "DE", "GB", "FR", "JP", "BR", "IN", "CA", "AU", "KR",
]
PAGE_PATHS = [
    "/home", "/products", "/products/detail", "/cart", "/checkout",
    "/account", "/search", "/blog", "/about", "/contact",
    "/faq", "/support", "/pricing", "/features", "/docs",
]
TAG_OPTIONS = [
    "organic", "paid", "referral", "returning", "new_user",
    "promo_active", "ab_test_v1", "ab_test_v2", "vip", "bot_suspect",
]

# How many rows per generation chunk.  Keeps peak memory around 200-400 MB.
CHUNK_SIZE = 500_000


def generate_chunk(start_id: int, num_rows: int, rng: np.random.Generator) -> pd.DataFrame:
    """
    Generate one chunk of synthetic event data.

    Parameters
    ----------
    start_id : int
        The first event_id for this chunk.
    num_rows : int
        How many rows to produce.
    rng : numpy Generator
        Seeded random generator for reproducibility.

    Returns
    -------
    pd.DataFrame
    """
    # -- Integer IDs ----------------------------------------------------------
    event_ids = np.arange(start_id, start_id + num_rows, dtype=np.int64)
    user_ids = rng.integers(1, 1_000_001, size=num_rows, dtype=np.int64)

    # -- Timestamps (last 365 days) -------------------------------------------
    base_ts = datetime(2025, 1, 1)
    offsets = rng.integers(0, 365 * 24 * 3600, size=num_rows)
    timestamps = pd.to_datetime(
        [base_ts + timedelta(seconds=int(o)) for o in offsets]
    )

    # -- Categorical strings --------------------------------------------------
    event_type = rng.choice(EVENT_TYPES, size=num_rows)
    device = rng.choice(DEVICES, size=num_rows, p=[0.55, 0.35, 0.10])
    country = rng.choice(COUNTRIES, size=num_rows)
    page_url = rng.choice(PAGE_PATHS, size=num_rows)

    # -- Float columns with nulls ---------------------------------------------
    #   duration_sec: ~10 % null, rest uniform 0.5–300 seconds
    duration_sec = rng.uniform(0.5, 300.0, size=num_rows)
    null_mask_dur = rng.random(num_rows) < 0.10
    duration_sec = duration_sec.astype(object)
    duration_sec[null_mask_dur] = None

    #   revenue: mostly null — only ~5 % of events are purchases
    revenue = np.full(num_rows, None, dtype=object)
    purchase_mask = event_type == "purchase"
    n_purchases = purchase_mask.sum()
    if n_purchases > 0:
        revenue[purchase_mask] = np.round(
            rng.lognormal(mean=3.5, sigma=1.0, size=n_purchases), 2
        )

    # -- Nested-like column (JSON list stored as string) ----------------------
    #   Each event gets 1-4 random tags serialized as a JSON array.
    tag_counts = rng.integers(1, 5, size=num_rows)
    session_tags = [
        json.dumps(rng.choice(TAG_OPTIONS, size=int(tc), replace=False).tolist())
        for tc in tag_counts
    ]

    # -- Assemble DataFrame ---------------------------------------------------
    df = pd.DataFrame(
        {
            "event_id": event_ids,
            "user_id": user_ids,
            "timestamp": timestamps,
            "event_type": event_type,
            "page_url": page_url,
            "duration_sec": pd.array(duration_sec, dtype=pd.Float64Dtype()),
            "revenue": pd.array(revenue, dtype=pd.Float64Dtype()),
            "device": device,
            "country": country,
            "session_tags": session_tags,
        }
    )
    return df


def generate_to_parquet(num_rows: int, parquet_path: str, seed: int = 42) -> int:
    """
    Generate synthetic data in chunks and stream directly to a Parquet file.

    This NEVER holds the full dataset in memory — only one chunk at a time.
    Returns the total number of rows written.
    """
    rng = np.random.default_rng(seed)
    generated = 0
    writer = None

    try:
        while generated < num_rows:
            chunk_rows = min(CHUNK_SIZE, num_rows - generated)
            print(f"  Generating rows {generated + 1:,} – {generated + chunk_rows:,} …")
            chunk_df = generate_chunk(start_id=generated + 1, num_rows=chunk_rows, rng=rng)
            table = pa.Table.from_pandas(chunk_df)

            if writer is None:
                writer = pq.ParquetWriter(parquet_path, table.schema, compression="snappy")
            writer.write_table(table)

            generated += chunk_rows
            del chunk_df, table
            gc.collect()
    finally:
        if writer is not None:
            writer.close()

    size_mb = os.path.getsize(parquet_path) / (1024 ** 2)
    print(f"  Total rows generated: {generated:,}")
    print(f"  Canonical Parquet file: {size_mb:.1f} MB")
    return generated


# ============================================================================
#  SECTION 2 — Writer Functions (Chunked / Streaming)
# ============================================================================
#
#  Each writer reads from the canonical Parquet file in batches and writes
#  to the target format incrementally.  This keeps memory usage low even
#  for multi-GB datasets.
# ============================================================================

def write_csv(parquet_path: str, path: str) -> None:
    """Write Parquet data to plain CSV in chunks."""
    pf = pq.ParquetFile(parquet_path)
    first = True
    for batch in pf.iter_batches(batch_size=CHUNK_SIZE):
        df = batch.to_pandas()
        df.to_csv(path, index=False, mode="a", header=first)
        first = False
        del df


def write_csv_gzip(parquet_path: str, path: str) -> None:
    """Write Parquet data to gzip-compressed CSV in chunks."""
    pf = pq.ParquetFile(parquet_path)
    first = True
    with gzip.open(path, "wt", newline="") as gz:
        for batch in pf.iter_batches(batch_size=CHUNK_SIZE):
            df = batch.to_pandas()
            text = df.to_csv(index=False, header=first)
            gz.write(text)
            first = False
            del df, text


def write_jsonl(parquet_path: str, path: str) -> None:
    """Write Parquet data to JSON Lines in chunks."""
    pf = pq.ParquetFile(parquet_path)
    with open(path, "w") as f:
        for batch in pf.iter_batches(batch_size=CHUNK_SIZE):
            df = batch.to_pandas()
            text = df.to_json(orient="records", lines=True, date_format="iso")
            f.write(text)
            f.write("\n")
            del df, text


def write_jsonl_gzip(parquet_path: str, path: str) -> None:
    """Write Parquet data to gzip-compressed JSON Lines in chunks."""
    pf = pq.ParquetFile(parquet_path)
    with gzip.open(path, "wt") as gz:
        for batch in pf.iter_batches(batch_size=CHUNK_SIZE):
            df = batch.to_pandas()
            text = df.to_json(orient="records", lines=True, date_format="iso")
            gz.write(text)
            gz.write("\n")
            del df, text


def write_parquet(parquet_path: str, path: str, compression: str = "snappy") -> None:
    """Write Parquet data to a new Parquet file with different compression."""
    pf = pq.ParquetFile(parquet_path)
    writer = None
    try:
        for batch in pf.iter_batches(batch_size=CHUNK_SIZE):
            table = pa.Table.from_batches([batch])
            if writer is None:
                writer = pq.ParquetWriter(path, table.schema, compression=compression)
            writer.write_table(table)
            del table
    finally:
        if writer is not None:
            writer.close()


def write_avro(parquet_path: str, path: str, codec: str = "snappy") -> None:
    """Write Parquet data to Avro using fastavro, processing in chunks."""
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

    pf = pq.ParquetFile(parquet_path)
    with open(path, "wb") as f:
        writer = None
        for batch in pf.iter_batches(batch_size=CHUNK_SIZE):
            df = batch.to_pandas()
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

            if writer is None:
                fastavro.writer(f, parsed_schema, records, codec=codec)
                writer = True
            else:
                fastavro.writer(f, parsed_schema, records, codec=codec, header_schema=None)
            del df, records


def write_orc(parquet_path: str, path: str, compression: str = "SNAPPY") -> None:
    """Write Parquet data to ORC using pyarrow.orc in chunks."""
    if not HAS_ORC:
        raise RuntimeError("pyarrow.orc is not available")

    pf = pq.ParquetFile(parquet_path)
    # ORC does not support pandas nullable Float64 well — cast columns.
    # Read the full table via pyarrow and write at once (ORC writer doesn't
    # support incremental writes easily, but pyarrow handles this efficiently
    # without pandas overhead).
    table = pf.read()
    # Cast nullable float columns
    schema = table.schema
    duration_idx = schema.get_field_index("duration_sec")
    revenue_idx = schema.get_field_index("revenue")
    table = table.set_column(duration_idx, "duration_sec",
                             table.column("duration_sec").cast(pa.float64()))
    table = table.set_column(revenue_idx, "revenue",
                             table.column("revenue").cast(pa.float64()))
    orc.write_table(table, path, compression=compression)
    del table


def write_feather(parquet_path: str, path: str, compression: str | None = None) -> None:
    """Write Parquet data to Feather (Arrow IPC) format."""
    # Feather needs the full table, but pyarrow reads Parquet efficiently
    # without pandas overhead.
    table = pq.read_table(parquet_path)
    feather.write_feather(table, path, compression=compression)
    del table


# ============================================================================
#  SECTION 3 — Reader Functions
# ============================================================================
#
#  Each reader returns a pandas DataFrame.  For "column selection" and
#  "filter" benchmarks we pass extra parameters so that columnar formats
#  can push projection / predicates down to the storage layer (this is
#  one of the main selling points of Parquet, ORC, etc.).
#
#  Columns selected for the column-projection benchmark:
#      ["user_id", "event_type", "revenue"]
#
#  Filter predicate for the filter benchmark:
#      event_type == "purchase"
#  (This selects ~20 % of rows, making push-down very beneficial.)
# ============================================================================

SELECT_COLUMNS = ["user_id", "event_type", "revenue"]
FILTER_EVENT_TYPE = "purchase"


# ---- CSV readers -----------------------------------------------------------

def read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


def read_csv_cols(path: str) -> pd.DataFrame:
    return pd.read_csv(path, usecols=SELECT_COLUMNS)


def read_csv_filter(path: str) -> pd.DataFrame:
    # CSV has no predicate push-down; we must read everything then filter.
    df = pd.read_csv(path)
    return df[df["event_type"] == FILTER_EVENT_TYPE]


def read_csv_gzip(path: str) -> pd.DataFrame:
    return pd.read_csv(path, compression="gzip")


def read_csv_gzip_cols(path: str) -> pd.DataFrame:
    return pd.read_csv(path, usecols=SELECT_COLUMNS, compression="gzip")


def read_csv_gzip_filter(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, compression="gzip")
    return df[df["event_type"] == FILTER_EVENT_TYPE]


# ---- JSON Lines readers ----------------------------------------------------

def read_jsonl(path: str) -> pd.DataFrame:
    return pd.read_json(path, lines=True)


def read_jsonl_cols(path: str) -> pd.DataFrame:
    # JSON Lines has no column projection push-down; read all, then select.
    df = pd.read_json(path, lines=True)
    return df[SELECT_COLUMNS]


def read_jsonl_filter(path: str) -> pd.DataFrame:
    df = pd.read_json(path, lines=True)
    return df[df["event_type"] == FILTER_EVENT_TYPE]


def read_jsonl_gzip(path: str) -> pd.DataFrame:
    return pd.read_json(path, lines=True, compression="gzip")


def read_jsonl_gzip_cols(path: str) -> pd.DataFrame:
    df = pd.read_json(path, lines=True, compression="gzip")
    return df[SELECT_COLUMNS]


def read_jsonl_gzip_filter(path: str) -> pd.DataFrame:
    df = pd.read_json(path, lines=True, compression="gzip")
    return df[df["event_type"] == FILTER_EVENT_TYPE]


# ---- Parquet readers -------------------------------------------------------
#  Parquet is *columnar* — pyarrow can read only requested columns and
#  apply row-group-level predicate filters WITHOUT reading the full file.

def read_parquet(path: str) -> pd.DataFrame:
    return pq.read_table(path).to_pandas()


def read_parquet_cols(path: str) -> pd.DataFrame:
    # Only the requested columns are read from disk — this is the big win.
    return pq.read_table(path, columns=SELECT_COLUMNS).to_pandas()


def read_parquet_filter(path: str) -> pd.DataFrame:
    # Predicate push-down: pyarrow skips row groups that don't match.
    filters = [("event_type", "==", FILTER_EVENT_TYPE)]
    return pq.read_table(path, filters=filters).to_pandas()


# ---- Avro readers ----------------------------------------------------------

def read_avro(path: str) -> pd.DataFrame:
    if fastavro is None:
        raise RuntimeError("fastavro is not installed")
    with open(path, "rb") as f:
        records = list(fastavro.reader(f))
    return pd.DataFrame(records)


def read_avro_cols(path: str) -> pd.DataFrame:
    # fastavro does not support native column selection — read all then pick.
    df = read_avro(path)
    return df[SELECT_COLUMNS]


def read_avro_filter(path: str) -> pd.DataFrame:
    df = read_avro(path)
    return df[df["event_type"] == FILTER_EVENT_TYPE]


# ---- ORC readers -----------------------------------------------------------

def read_orc(path: str) -> pd.DataFrame:
    if not HAS_ORC:
        raise RuntimeError("pyarrow.orc is not available")
    return orc.read_table(path).to_pandas()


def read_orc_cols(path: str) -> pd.DataFrame:
    # ORC is columnar — only selected columns are read from disk.
    return orc.read_table(path, columns=SELECT_COLUMNS).to_pandas()


def read_orc_filter(path: str) -> pd.DataFrame:
    # pyarrow.orc does not support predicate push-down via the Python API,
    # so we read then filter in-memory.
    df = orc.read_table(path).to_pandas()
    return df[df["event_type"] == FILTER_EVENT_TYPE]


# ---- Feather / Arrow IPC readers -------------------------------------------

def read_feather(path: str) -> pd.DataFrame:
    return feather.read_feather(path)


def read_feather_cols(path: str) -> pd.DataFrame:
    # Feather supports column projection.
    return feather.read_feather(path, columns=SELECT_COLUMNS)


def read_feather_filter(path: str) -> pd.DataFrame:
    # No native predicate push-down; read then filter.
    df = feather.read_feather(path)
    return df[df["event_type"] == FILTER_EVENT_TYPE]


# ============================================================================
#  SECTION 4 — Benchmark Definitions
# ============================================================================
#
#  Each format is described by a dict with:
#    name        — human-readable label
#    extension   — file extension
#    writer      — callable(parquet_path, target_path)
#    reader      — callable(path) -> DataFrame  (full scan)
#    reader_cols — callable(path) -> DataFrame  (column selection)
#    reader_filt — callable(path) -> DataFrame  (filtered read)
#    available   — whether the required library is present
# ============================================================================

def build_format_registry() -> list[dict[str, Any]]:
    """Return a list of format descriptors for all formats we can test."""

    formats: list[dict[str, Any]] = [
        # ---- CSV -----------------------------------------------------------
        {
            "name": "CSV",
            "extension": ".csv",
            "writer": write_csv,
            "reader": read_csv,
            "reader_cols": read_csv_cols,
            "reader_filt": read_csv_filter,
            "available": True,
        },
        {
            "name": "CSV + gzip",
            "extension": ".csv.gz",
            "writer": write_csv_gzip,
            "reader": read_csv_gzip,
            "reader_cols": read_csv_gzip_cols,
            "reader_filt": read_csv_gzip_filter,
            "available": True,
        },
        # ---- JSON Lines ----------------------------------------------------
        {
            "name": "JSON Lines",
            "extension": ".jsonl",
            "writer": write_jsonl,
            "reader": read_jsonl,
            "reader_cols": read_jsonl_cols,
            "reader_filt": read_jsonl_filter,
            "available": True,
        },
        {
            "name": "JSON Lines + gzip",
            "extension": ".jsonl.gz",
            "writer": write_jsonl_gzip,
            "reader": read_jsonl_gzip,
            "reader_cols": read_jsonl_gzip_cols,
            "reader_filt": read_jsonl_gzip_filter,
            "available": True,
        },
        # ---- Parquet -------------------------------------------------------
        {
            "name": "Parquet (snappy)",
            "extension": ".snappy.parquet",
            "writer": lambda src, p: write_parquet(src, p, compression="snappy"),
            "reader": read_parquet,
            "reader_cols": read_parquet_cols,
            "reader_filt": read_parquet_filter,
            "available": True,
        },
        {
            "name": "Parquet (gzip)",
            "extension": ".gzip.parquet",
            "writer": lambda src, p: write_parquet(src, p, compression="gzip"),
            "reader": read_parquet,
            "reader_cols": read_parquet_cols,
            "reader_filt": read_parquet_filter,
            "available": True,
        },
        {
            "name": "Parquet (zstd)",
            "extension": ".zstd.parquet",
            "writer": lambda src, p: write_parquet(src, p, compression="zstd"),
            "reader": read_parquet,
            "reader_cols": read_parquet_cols,
            "reader_filt": read_parquet_filter,
            "available": True,
        },
        {
            "name": "Parquet (none)",
            "extension": ".none.parquet",
            "writer": lambda src, p: write_parquet(src, p, compression="none"),
            "reader": read_parquet,
            "reader_cols": read_parquet_cols,
            "reader_filt": read_parquet_filter,
            "available": True,
        },
        # ---- Avro ----------------------------------------------------------
        {
            "name": "Avro (snappy)",
            "extension": ".snappy.avro",
            "writer": lambda src, p: write_avro(src, p, codec="snappy"),
            "reader": read_avro,
            "reader_cols": read_avro_cols,
            "reader_filt": read_avro_filter,
            "available": fastavro is not None,
        },
        {
            "name": "Avro (deflate)",
            "extension": ".deflate.avro",
            "writer": lambda src, p: write_avro(src, p, codec="deflate"),
            "reader": read_avro,
            "reader_cols": read_avro_cols,
            "reader_filt": read_avro_filter,
            "available": fastavro is not None,
        },
        # ---- ORC -----------------------------------------------------------
        {
            "name": "ORC (snappy)",
            "extension": ".snappy.orc",
            "writer": lambda src, p: write_orc(src, p, compression="SNAPPY"),
            "reader": read_orc,
            "reader_cols": read_orc_cols,
            "reader_filt": read_orc_filter,
            "available": HAS_ORC,
        },
        {
            "name": "ORC (zlib)",
            "extension": ".zlib.orc",
            "writer": lambda src, p: write_orc(src, p, compression="ZLIB"),
            "reader": read_orc,
            "reader_cols": read_orc_cols,
            "reader_filt": read_orc_filter,
            "available": HAS_ORC,
        },
        # ---- Feather / Arrow IPC -------------------------------------------
        {
            "name": "Feather (none)",
            "extension": ".none.feather",
            "writer": lambda src, p: write_feather(src, p, compression=None),
            "reader": read_feather,
            "reader_cols": read_feather_cols,
            "reader_filt": read_feather_filter,
            "available": True,
        },
        {
            "name": "Feather (lz4)",
            "extension": ".lz4.feather",
            "writer": lambda src, p: write_feather(src, p, compression="lz4"),
            "reader": read_feather,
            "reader_cols": read_feather_cols,
            "reader_filt": read_feather_filter,
            "available": True,
        },
        {
            "name": "Feather (zstd)",
            "extension": ".zstd.feather",
            "writer": lambda src, p: write_feather(src, p, compression="zstd"),
            "reader": read_feather,
            "reader_cols": read_feather_cols,
            "reader_filt": read_feather_filter,
            "available": True,
        },
    ]
    return formats


# ============================================================================
#  SECTION 5 — Timing Helpers
# ============================================================================

def time_it(func, *args, **kwargs) -> tuple[float, Any]:
    """
    Run *func* with the given arguments, measure wall-clock time.

    Returns
    -------
    (elapsed_seconds, return_value)
    """
    start = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - start
    return elapsed, result


def file_size_mb(path: str) -> float:
    """Return the file size in megabytes."""
    return os.path.getsize(path) / (1024 ** 2)


# ============================================================================
#  SECTION 6 — Aggregation Benchmark
# ============================================================================
#
#  After reading the data we perform a typical analytical query:
#      SELECT country, event_type,
#             COUNT(*) AS cnt,
#             AVG(duration_sec) AS avg_duration,
#             SUM(revenue) AS total_revenue
#      FROM events
#      GROUP BY country, event_type
#
#  This measures how fast pandas can aggregate the data once it's in memory.
#  The aggregation time is measured separately from the read time.
# ============================================================================

def run_aggregation(df: pd.DataFrame) -> pd.DataFrame:
    """Perform a groupby aggregation on the DataFrame."""
    return df.groupby(["country", "event_type"]).agg(
        cnt=("event_id", "count"),
        avg_duration=("duration_sec", "mean"),
        total_revenue=("revenue", "sum"),
    ).reset_index()


# ============================================================================
#  SECTION 7 — Main Benchmark Loop
# ============================================================================

def run_benchmarks(
    parquet_path: str,
    output_dir: str,
    formats: list[dict[str, Any]],
) -> pd.DataFrame:
    """
    Run all benchmarks and collect results into a DataFrame.

    Writers read from the canonical Parquet file and write to target format.
    Readers read from the written file and optionally do column selection,
    filtering, and aggregation.
    """
    data_dir = os.path.join(output_dir, "data_files")
    os.makedirs(data_dir, exist_ok=True)

    results: list[dict[str, Any]] = []

    for fmt in formats:
        name = fmt["name"]

        if not fmt["available"]:
            print(f"\n  SKIP  {name} (library not available)")
            continue

        print(f"\n{'='*60}")
        print(f"  Benchmarking: {name}")
        print(f"{'='*60}")

        file_path = os.path.join(data_dir, f"benchmark{fmt['extension']}")

        # ---- 1. Write -------------------------------------------------------
        print(f"  Writing …", end=" ", flush=True)
        try:
            write_sec, _ = time_it(fmt["writer"], parquet_path, file_path)
        except Exception as e:
            print(f"FAILED ({e})")
            continue
        size_mb = file_size_mb(file_path)
        print(f"{write_sec:.2f}s  ({size_mb:.1f} MB)")

        # Force garbage collection between heavy operations
        gc.collect()

        # ---- 2. Full read ----------------------------------------------------
        print(f"  Reading (full scan) …", end=" ", flush=True)
        try:
            read_sec, df_read = time_it(fmt["reader"], file_path)
        except Exception as e:
            print(f"FAILED ({e})")
            continue
        print(f"{read_sec:.2f}s  ({len(df_read):,} rows)")

        # ---- 3. Aggregation --------------------------------------------------
        #  We aggregate the full-scan DataFrame while it's still in memory.
        print(f"  Aggregation (groupby) …", end=" ", flush=True)
        try:
            agg_sec, _ = time_it(run_aggregation, df_read)
        except Exception as e:
            print(f"FAILED ({e})")
            agg_sec = float("nan")
        else:
            print(f"{agg_sec:.2f}s")

        # Free the full-scan DataFrame before column/filter reads
        del df_read
        gc.collect()

        # ---- 4. Column selection ---------------------------------------------
        print(f"  Reading (column select) …", end=" ", flush=True)
        try:
            read_cols_sec, df_cols = time_it(fmt["reader_cols"], file_path)
        except Exception as e:
            print(f"FAILED ({e})")
            read_cols_sec = float("nan")
        else:
            print(f"{read_cols_sec:.2f}s  ({len(df_cols):,} rows, {len(df_cols.columns)} cols)")
            del df_cols
        gc.collect()

        # ---- 5. Filtered read ------------------------------------------------
        print(f"  Reading (filter: event_type='purchase') …", end=" ", flush=True)
        try:
            read_filt_sec, df_filt = time_it(fmt["reader_filt"], file_path)
        except Exception as e:
            print(f"FAILED ({e})")
            read_filt_sec = float("nan")
        else:
            print(f"{read_filt_sec:.2f}s  ({len(df_filt):,} rows)")
            del df_filt
        gc.collect()

        # ---- Collect results -------------------------------------------------
        results.append(
            {
                "Format": name,
                "File Size (MB)": round(size_mb, 2),
                "Write Time (s)": round(write_sec, 2),
                "Read Full (s)": round(read_sec, 2),
                "Read Cols (s)": round(read_cols_sec, 2),
                "Read Filter (s)": round(read_filt_sec, 2),
                "Aggregation (s)": round(agg_sec, 2),
            }
        )

    return pd.DataFrame(results)


# ============================================================================
#  SECTION 8 — Result Reporting & Visualization
# ============================================================================

def print_results_table(results_df: pd.DataFrame) -> None:
    """Pretty-print the results table to the console."""
    print("\n")
    print("=" * 90)
    print("  BENCHMARK RESULTS")
    print("=" * 90)
    print(
        tabulate(
            results_df,
            headers="keys",
            tablefmt="pretty",
            showindex=False,
            numalign="right",
            floatfmt=".2f",
        )
    )
    print()


def save_results_csv(results_df: pd.DataFrame, output_dir: str) -> str:
    """Save results to benchmark_results.csv and return the path."""
    path = os.path.join(output_dir, "benchmark_results.csv")
    results_df.to_csv(path, index=False)
    print(f"  Results saved to: {path}")
    return path


def generate_charts(results_df: pd.DataFrame, output_dir: str) -> None:
    """
    Generate comparison bar charts for key metrics.

    Charts are saved as PNG files in the output directory.
    """
    if plt is None:
        print("  Skipping chart generation (matplotlib not installed).")
        return

    charts_dir = os.path.join(output_dir, "charts")
    os.makedirs(charts_dir, exist_ok=True)

    # Use a readable style
    plt.style.use("seaborn-v0_8-whitegrid")

    metrics = [
        ("File Size (MB)", "File Size Comparison", "file_size.png", "steelblue"),
        ("Write Time (s)", "Write Time Comparison", "write_time.png", "darkorange"),
        ("Read Full (s)", "Full Read Time Comparison", "read_full.png", "seagreen"),
        ("Read Cols (s)", "Column Selection Read Time", "read_cols.png", "mediumpurple"),
        ("Read Filter (s)", "Filtered Read Time Comparison", "read_filter.png", "indianred"),
    ]

    for col, title, filename, color in metrics:
        if col not in results_df.columns:
            continue

        fig, ax = plt.subplots(figsize=(12, max(6, len(results_df) * 0.45)))
        bars = ax.barh(results_df["Format"], results_df[col], color=color, edgecolor="white")

        # Add value labels on bars
        for bar, val in zip(bars, results_df[col]):
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
        ax.invert_yaxis()  # highest bar at top
        plt.tight_layout()

        chart_path = os.path.join(charts_dir, filename)
        fig.savefig(chart_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"  Chart saved: {chart_path}")


# ============================================================================
#  SECTION 9 — Cleanup
# ============================================================================

def cleanup_data_files(output_dir: str) -> None:
    """Remove the generated data files to free disk space."""
    data_dir = os.path.join(output_dir, "data_files")
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
        print(f"  Cleaned up data files in: {data_dir}")


# ============================================================================
#  SECTION 10 — CLI Entry Point
# ============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark big data file formats (CSV, JSON, Parquet, Avro, ORC, Feather).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python benchmark_formats.py                  # Full benchmark (~3 GB)
  python benchmark_formats.py --quick          # Quick demo   (~500 MB)
  python benchmark_formats.py --num-rows 1000000 --keep
        """,
    )
    parser.add_argument(
        "--num-rows",
        type=int,
        default=None,
        help="Number of rows to generate. Overrides --quick. "
             "Default: 30_000_000 (full) or 5_000_000 (quick).",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Quick mode: generate ~500 MB of data (~5M rows) for a faster classroom demo.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./benchmark_output",
        help="Directory for output files (default: ./benchmark_output).",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Keep generated data files after the benchmark (default: delete them).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible data generation (default: 42).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Determine number of rows
    if args.num_rows is not None:
        num_rows = args.num_rows
    elif args.quick:
        num_rows = 5_000_000
    else:
        num_rows = 30_000_000  # ~3+ GB as CSV

    output_dir = os.path.abspath(args.output_dir)
    data_dir = os.path.join(output_dir, "data_files")
    os.makedirs(data_dir, exist_ok=True)

    # The canonical Parquet file acts as the data source for all write benchmarks.
    canonical_parquet = os.path.join(data_dir, "_canonical_source.snappy.parquet")

    print("=" * 70)
    print("  BIG DATA FILE FORMAT BENCHMARK")
    print("=" * 70)
    print(f"  Rows to generate : {num_rows:,}")
    print(f"  Output directory  : {output_dir}")
    print(f"  Quick mode        : {args.quick}")
    print(f"  Keep data files   : {args.keep}")
    print(f"  Random seed       : {args.seed}")
    print(f"  ORC available     : {HAS_ORC}")
    print(f"  Avro available    : {fastavro is not None}")
    print(f"  matplotlib avail. : {plt is not None}")
    print("=" * 70)

    # ---- Step 1: Generate data → stream to canonical Parquet ----------------
    print("\n[Step 1/4] Generating synthetic data → Parquet …")
    total_start = time.perf_counter()
    gen_start = time.perf_counter()
    total_rows = generate_to_parquet(
        num_rows=num_rows, parquet_path=canonical_parquet, seed=args.seed
    )
    gen_elapsed = time.perf_counter() - gen_start
    print(f"  Data generation took {gen_elapsed:.1f}s")

    # ---- Step 2: Run benchmarks ---------------------------------------------
    print("\n[Step 2/4] Running benchmarks …")
    formats = build_format_registry()
    results_df = run_benchmarks(canonical_parquet, output_dir, formats)

    if results_df.empty:
        print("\nNo formats were benchmarked successfully. Exiting.")
        sys.exit(1)

    # ---- Step 3: Report results ---------------------------------------------
    print("\n[Step 3/4] Reporting results …")
    print_results_table(results_df)
    save_results_csv(results_df, output_dir)
    generate_charts(results_df, output_dir)

    # ---- Step 4: Cleanup ----------------------------------------------------
    if not args.keep:
        print("\n[Step 4/4] Cleaning up data files …")
        cleanup_data_files(output_dir)
    else:
        print("\n[Step 4/4] Keeping data files (--keep flag set).")

    total_elapsed = time.perf_counter() - total_start
    print(f"\n  Total benchmark time: {total_elapsed:.1f}s")
    print("  Done!")


if __name__ == "__main__":
    main()
