#!/usr/bin/env python3
"""
=============================================================================
  Step 1 — Generate Synthetic Data & Review It
=============================================================================

This script generates a synthetic dataset of user interaction events and
saves it as a Parquet file for the next steps.

After running, inspect the output to understand the data:
  - Column types (int, float, string, datetime, nullable)
  - Distribution of values
  - Null patterns
  - In-memory vs on-disk size

Usage:
    python step_1_generate_data.py                        # 30M rows (~3 GB CSV)
    python step_1_generate_data.py --num-rows 5000000     # 5M rows (quick demo)
"""

import argparse

from common import (
    np, pd, pa, pq, json,
    EVENT_TYPES, DEVICES, COUNTRIES, PAGE_PATHS, TAG_OPTIONS,
    CHUNK_SIZE, GENERATED_DATA_PATH, ensure_output_dir, time_it,
)


# ============================================================================
#  Data Generation
# ============================================================================

def generate_chunk(start_id: int, num_rows: int, rng: np.random.Generator) -> pd.DataFrame:
    """
    Generate one chunk of synthetic event data.

    We model *user interaction events* on a web platform. Each row has:
      - event_id     : monotonically increasing INT64
      - user_id      : INT64 (1 to 1,000,000)
      - timestamp    : DATETIME (over the last 365 days)
      - event_type   : STRING categorical (5 categories)
      - page_url     : STRING (simulated URL path)
      - duration_sec : FLOAT64 (time on page, ~10% null)
      - revenue      : FLOAT64 (purchase amount, ~95% null)
      - device       : STRING categorical (mobile/desktop/tablet)
      - country      : STRING categorical (10 countries)
      - session_tags : STRING (JSON-encoded list — nested-like data)
    """
    # Integer IDs
    event_ids = np.arange(start_id, start_id + num_rows, dtype=np.int64)
    user_ids = rng.integers(1, 1_000_001, size=num_rows, dtype=np.int64)

    # Timestamps (last 365 days)
    from datetime import datetime, timedelta
    base_ts = datetime(2025, 1, 1)
    offsets = rng.integers(0, 365 * 24 * 3600, size=num_rows)
    timestamps = pd.to_datetime(
        [base_ts + timedelta(seconds=int(o)) for o in offsets]
    )

    # Categorical strings
    event_type = rng.choice(EVENT_TYPES, size=num_rows)
    device = rng.choice(DEVICES, size=num_rows, p=[0.55, 0.35, 0.10])
    country = rng.choice(COUNTRIES, size=num_rows)
    page_url = rng.choice(PAGE_PATHS, size=num_rows)

    # Float columns with nulls
    duration_sec = rng.uniform(0.5, 300.0, size=num_rows)
    null_mask_dur = rng.random(num_rows) < 0.10
    duration_sec = duration_sec.astype(object)
    duration_sec[null_mask_dur] = None

    revenue = np.full(num_rows, None, dtype=object)
    purchase_mask = event_type == "purchase"
    n_purchases = purchase_mask.sum()
    if n_purchases > 0:
        revenue[purchase_mask] = np.round(
            rng.lognormal(mean=3.5, sigma=1.0, size=n_purchases), 2
        )

    # Nested-like column (JSON list stored as string)
    tag_counts = rng.integers(1, 5, size=num_rows)
    session_tags = [
        json.dumps(rng.choice(TAG_OPTIONS, size=int(tc), replace=False).tolist())
        for tc in tag_counts
    ]

    df = pd.DataFrame({
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
    })
    return df


def generate_to_parquet(num_rows: int, parquet_path: str, seed: int = 42) -> int:
    """
    Generate synthetic data in chunks and stream directly to a Parquet file.

    This NEVER holds the full dataset in memory — only one chunk at a time.
    Returns the total number of rows written.
    """
    import gc
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

    return generated


# ============================================================================
#  Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Step 1: Generate synthetic data")
    parser.add_argument("--num-rows", type=int, default=30_000_000,
                        help="Number of rows to generate (default: 30,000,000 → ~3 GB CSV)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    args = parser.parse_args()

    ensure_output_dir()

    print("=" * 70)
    print("  STEP 1 — GENERATE SYNTHETIC DATA")
    print("=" * 70)
    print(f"  Rows: {args.num_rows:,}")
    print(f"  Seed: {args.seed}")
    print(f"  Output: {GENERATED_DATA_PATH}")

    # Generate — streams directly to Parquet, never holds full dataset in memory
    gen_sec, total_rows = time_it(generate_to_parquet, args.num_rows, GENERATED_DATA_PATH, args.seed)
    size_mb = os.path.getsize(GENERATED_DATA_PATH) / (1024 ** 2)
    print(f"\n  Generation took {gen_sec:.1f}s")
    print(f"  Parquet file: {size_mb:.1f} MB on disk")

    # Quick review of a sample (first chunk only to save memory)
    print("\n  Quick data review (first 5 rows):")
    sample = pq.ParquetFile(GENERATED_DATA_PATH).read_row_group(0).to_pandas().head()
    print(sample.to_string(index=False))

    print(f"\n  Total rows: {total_rows:,}")
    print("\n  Done! Run step_2_write_formats.py next.")


if __name__ == "__main__":
    import os
    main()
