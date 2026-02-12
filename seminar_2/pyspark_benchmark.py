#!/usr/bin/env python3
"""
=============================================================================
  PySpark File Format Benchmark
  Seminar 2 -- Avro, Parquet, ORC, CSV, JSON in Apache Spark
=============================================================================

This script benchmarks reading and writing of various file formats using
PySpark (Apache Spark). The key point is to demonstrate that Avro, which is
painfully slow in pure Python (fastavro), becomes highly efficient when
processed by Spark's JVM-based engine.

Formats benchmarked:
  - CSV (plain, gzip)
  - JSON Lines (plain, gzip)
  - Parquet (snappy, gzip, zstd, none)
  - Avro (snappy, deflate) -- the star of this benchmark
  - ORC (snappy, zlib)

For each format, the script measures:
  1. Write time + file size on disk
  2. Full read time
  3. Column selection read (user_id, event_type, revenue)
  4. Filtered read (event_type == 'purchase')
  5. Aggregation (groupBy country, event_type -> count, avg duration, sum revenue)

Prerequisites:
  - Java 8 or 11+ installed and JAVA_HOME set
  - PySpark installed: pip install pyspark
  - tabulate installed: pip install tabulate
  - matplotlib installed (optional): pip install matplotlib

Usage:
  # Quick demo (~1M rows, fast)
  python pyspark_benchmark.py --quick

  # Default (5M rows)
  python pyspark_benchmark.py

  # Custom size, keep output files
  python pyspark_benchmark.py --num-rows 10000000 --keep

  # With specific Spark master
  python pyspark_benchmark.py --num-rows 2000000
"""

import argparse
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
#  Dependency checks -- give clear error messages
# ---------------------------------------------------------------------------

def check_java():
    """Check if Java is available (required for PySpark)."""
    java_home = os.environ.get("JAVA_HOME", "")
    try:
        import subprocess
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode != 0:
            return False, "Java is installed but returned an error."
        return True, ""
    except FileNotFoundError:
        return False, (
            "Java is not installed or not in PATH.\n"
            "  PySpark requires Java 8, 11, or 17.\n"
            "  Install Java: https://adoptium.net/\n"
            f"  JAVA_HOME: {java_home or '(not set)'}"
        )
    except Exception as e:
        return False, f"Error checking Java: {e}"


try:
    from tabulate import tabulate
except ImportError:
    sys.exit("ERROR: tabulate is not installed.  Run:  pip install tabulate")

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    plt = None
    print("WARNING: matplotlib is not installed. Charts will be skipped.")

# Check Java before importing PySpark
java_ok, java_msg = check_java()
if not java_ok:
    print(f"ERROR: {java_msg}")
    print("PySpark benchmarks cannot run without Java. Exiting.")
    sys.exit(1)

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, LongType, StringType,
        DoubleType, TimestampType
    )
except ImportError:
    sys.exit(
        "ERROR: pyspark is not installed.\n"
        "  Run:  pip install pyspark\n"
        "  PySpark also requires Java 8, 11, or 17."
    )


# ============================================================================
#  Constants
# ============================================================================

SPARK_AVRO_PACKAGE = "org.apache.spark:spark-avro_2.13:4.0.0"

# Columns for column-projection benchmark
SELECT_COLUMNS = ["user_id", "event_type", "revenue"]

# Filter value for predicate pushdown benchmark
FILTER_EVENT_TYPE = "purchase"

# Data generation constants
EVENT_TYPES = ["page_view", "click", "scroll", "purchase", "logout"]
DEVICES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["US", "DE", "GB", "FR", "JP", "BR", "IN", "CA", "AU", "KR"]
PAGE_PATHS = [
    "/home", "/products", "/products/detail", "/cart", "/checkout",
    "/account", "/search", "/blog", "/about", "/contact",
    "/faq", "/support", "/pricing", "/features", "/docs",
]


# ============================================================================
#  SparkSession Creation
# ============================================================================

def create_spark_session(app_name: str = "PySpark Format Benchmark") -> SparkSession:
    """
    Create a SparkSession with local[*] mode and spark-avro support.

    The spark-avro package will be downloaded automatically by Spark's
    dependency resolver on first run (requires internet).
    """
    print("  Creating SparkSession ...")
    print(f"  Avro package: {SPARK_AVRO_PACKAGE}")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", SPARK_AVRO_PACKAGE)
        .config("spark.sql.avro.compression.codec", "snappy")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        # Reduce logging noise
        .config("spark.log.level", "WARN")
        .getOrCreate()
    )

    # Suppress Spark's verbose INFO logging
    spark.sparkContext.setLogLevel("WARN")
    print(f"  Spark version: {spark.version}")
    print(f"  Spark master:  {spark.sparkContext.master}")
    print(f"  Parallelism:   {spark.sparkContext.defaultParallelism} cores")
    return spark


# ============================================================================
#  Data Generation
# ============================================================================

def load_or_generate_data(
    spark: SparkSession,
    num_rows: int,
    source_parquet: str,
) -> DataFrame:
    """
    Load data from the step_1 generated Parquet file if it exists,
    or generate it directly using PySpark.

    Parameters
    ----------
    spark : SparkSession
    num_rows : int
        Number of rows to generate (only used if source_parquet doesn't exist).
    source_parquet : str
        Path to the step_1 generated Parquet file.

    Returns
    -------
    DataFrame
        Spark DataFrame with the benchmark data.
    """
    # Always generate data using PySpark directly.
    # Note: pyarrow-generated Parquet files use nanosecond timestamps which
    # PySpark 4.x doesn't support, so we generate our own data.
    return generate_data_spark(spark, num_rows)


def generate_data_spark(spark: SparkSession, num_rows: int) -> DataFrame:
    """
    Generate synthetic event data directly using PySpark.

    Creates data matching the schema from step_1_generate_data.py:
      event_id, user_id, timestamp, event_type, page_url,
      duration_sec, revenue, device, country, session_tags
    """
    print(f"\n  Generating {num_rows:,} rows using PySpark ...")
    start = time.perf_counter()

    # Generate a range DataFrame as the base
    df_base = spark.range(1, num_rows + 1).withColumnRenamed("id", "event_id")

    # Add columns using Spark SQL functions
    import json as json_mod

    df = (
        df_base
        # user_id: random integer 1 to 1,000,000
        .withColumn("user_id", (F.rand() * 1_000_000 + 1).cast(LongType()))

        # timestamp: random datetime in the last 365 days
        .withColumn(
            "timestamp",
            F.from_unixtime(
                F.lit(1704067200) + (F.rand() * 365 * 24 * 3600).cast(LongType())
            ).cast(TimestampType())
        )

        # event_type: random choice from EVENT_TYPES
        .withColumn(
            "event_type",
            F.array(*[F.lit(e) for e in EVENT_TYPES])
                .getItem((F.rand() * len(EVENT_TYPES)).cast("int"))
        )

        # page_url: random choice from PAGE_PATHS
        .withColumn(
            "page_url",
            F.array(*[F.lit(p) for p in PAGE_PATHS])
                .getItem((F.rand() * len(PAGE_PATHS)).cast("int"))
        )

        # duration_sec: uniform 0.5 to 300, ~10% null
        .withColumn(
            "duration_sec",
            F.when(F.rand() < 0.10, F.lit(None).cast(DoubleType()))
                .otherwise(F.rand() * 299.5 + 0.5)
        )

        # revenue: null for non-purchases, lognormal-ish for purchases
        .withColumn(
            "revenue",
            F.when(
                F.col("event_type") == "purchase",
                F.round(F.exp(F.randn() * 1.0 + 3.5), 2)
            ).otherwise(F.lit(None).cast(DoubleType()))
        )

        # device: weighted random choice
        .withColumn(
            "_device_rand", F.rand()
        )
        .withColumn(
            "device",
            F.when(F.col("_device_rand") < 0.55, F.lit("mobile"))
                .when(F.col("_device_rand") < 0.90, F.lit("desktop"))
                .otherwise(F.lit("tablet"))
        )
        .drop("_device_rand")

        # country: random choice from COUNTRIES
        .withColumn(
            "country",
            F.array(*[F.lit(c) for c in COUNTRIES])
                .getItem((F.rand() * len(COUNTRIES)).cast("int"))
        )

        # session_tags: simplified JSON array of tags
        .withColumn(
            "session_tags",
            F.lit('["organic","returning"]')
        )
    )

    # Force materialization to measure generation time
    df = df.cache()
    count = df.count()
    elapsed = time.perf_counter() - start
    print(f"  Generated {count:,} rows in {elapsed:.1f}s")

    return df


# ============================================================================
#  Benchmark Helpers
# ============================================================================

def dir_size_mb(path: str) -> float:
    """
    Calculate total size of a file or directory in MB.

    Spark writes output as directories with part-files inside.
    """
    total = 0
    if os.path.isfile(path):
        total = os.path.getsize(path)
    elif os.path.isdir(path):
        for dirpath, _dirnames, filenames in os.walk(path):
            for f in filenames:
                # Skip Spark metadata files for size calculation
                if not f.startswith("_") and not f.startswith("."):
                    total += os.path.getsize(os.path.join(dirpath, f))
    return total / (1024 ** 2)


def time_write(df: DataFrame, format_name: str, path: str, **options) -> tuple[float, float]:
    """
    Write a DataFrame to the given format, measuring time and file size.

    Returns (elapsed_seconds, size_mb).
    """
    # Clean up any previous output
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)

    writer = df.write.format(format_name).mode("overwrite")
    for key, value in options.items():
        writer = writer.option(key, value)

    start = time.perf_counter()
    writer.save(path)
    elapsed = time.perf_counter() - start

    size_mb = dir_size_mb(path)
    return elapsed, size_mb


def time_read_full(spark: SparkSession, format_name: str, path: str) -> tuple[float, int]:
    """
    Read the full dataset, forcing Spark to execute. Returns (elapsed, row_count).
    """
    start = time.perf_counter()
    df = spark.read.format(format_name).load(path)
    count = df.count()
    elapsed = time.perf_counter() - start
    return elapsed, count


def time_read_cols(spark: SparkSession, format_name: str, path: str) -> tuple[float, int]:
    """
    Read only selected columns. Returns (elapsed, row_count).
    """
    start = time.perf_counter()
    df = spark.read.format(format_name).load(path).select(*SELECT_COLUMNS)
    count = df.count()
    elapsed = time.perf_counter() - start
    return elapsed, count


def time_read_filter(spark: SparkSession, format_name: str, path: str) -> tuple[float, int]:
    """
    Read with a filter predicate. Returns (elapsed, row_count).
    """
    start = time.perf_counter()
    df = (
        spark.read.format(format_name).load(path)
        .filter(F.col("event_type") == FILTER_EVENT_TYPE)
    )
    count = df.count()
    elapsed = time.perf_counter() - start
    return elapsed, count


def time_aggregation(spark: SparkSession, format_name: str, path: str) -> tuple[float, int]:
    """
    Read and perform aggregation. Returns (elapsed, result_row_count).

    Aggregation:
        GROUP BY country, event_type
        -> COUNT(*), AVG(duration_sec), SUM(revenue)
    """
    start = time.perf_counter()
    df = spark.read.format(format_name).load(path)
    agg_df = df.groupBy("country", "event_type").agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration"),
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
    )
    count = agg_df.count()
    elapsed = time.perf_counter() - start
    return elapsed, count


# ============================================================================
#  Format Definitions
# ============================================================================

def build_format_registry(output_dir: str) -> list[dict[str, Any]]:
    """
    Build the list of format configurations to benchmark.

    Each entry has:
      name         : human-readable label
      spark_format : the format string for Spark (csv, json, parquet, avro, orc)
      dir_name     : output directory name
      write_opts   : dict of options for df.write
      read_opts    : dict of options for spark.read (if any)
      available    : whether this format can be tested
    """
    data_dir = os.path.join(output_dir, "spark_data_files")

    formats = [
        # ---- CSV ----
        {
            "name": "CSV",
            "spark_format": "csv",
            "dir_name": os.path.join(data_dir, "csv_plain"),
            "write_opts": {"header": "true"},
            "read_opts": {"header": "true", "inferSchema": "true"},
            "available": True,
        },
        {
            "name": "CSV + gzip",
            "spark_format": "csv",
            "dir_name": os.path.join(data_dir, "csv_gzip"),
            "write_opts": {"header": "true", "compression": "gzip"},
            "read_opts": {"header": "true", "inferSchema": "true"},
            "available": True,
        },
        # ---- JSON Lines ----
        {
            "name": "JSON Lines",
            "spark_format": "json",
            "dir_name": os.path.join(data_dir, "json_plain"),
            "write_opts": {},
            "read_opts": {},
            "available": True,
        },
        {
            "name": "JSON Lines + gzip",
            "spark_format": "json",
            "dir_name": os.path.join(data_dir, "json_gzip"),
            "write_opts": {"compression": "gzip"},
            "read_opts": {},
            "available": True,
        },
        # ---- Parquet ----
        {
            "name": "Parquet (snappy)",
            "spark_format": "parquet",
            "dir_name": os.path.join(data_dir, "parquet_snappy"),
            "write_opts": {"compression": "snappy"},
            "read_opts": {},
            "available": True,
        },
        {
            "name": "Parquet (gzip)",
            "spark_format": "parquet",
            "dir_name": os.path.join(data_dir, "parquet_gzip"),
            "write_opts": {"compression": "gzip"},
            "read_opts": {},
            "available": True,
        },
        {
            "name": "Parquet (zstd)",
            "spark_format": "parquet",
            "dir_name": os.path.join(data_dir, "parquet_zstd"),
            "write_opts": {"compression": "zstd"},
            "read_opts": {},
            "available": True,
        },
        {
            "name": "Parquet (none)",
            "spark_format": "parquet",
            "dir_name": os.path.join(data_dir, "parquet_none"),
            "write_opts": {"compression": "none"},
            "read_opts": {},
            "available": True,
        },
        # ---- Avro (KEY formats for this benchmark!) ----
        {
            "name": "Avro (snappy)",
            "spark_format": "avro",
            "dir_name": os.path.join(data_dir, "avro_snappy"),
            "write_opts": {"compression": "snappy"},
            "read_opts": {},
            "available": True,
        },
        {
            "name": "Avro (deflate)",
            "spark_format": "avro",
            "dir_name": os.path.join(data_dir, "avro_deflate"),
            "write_opts": {"compression": "deflate"},
            "read_opts": {},
            "available": True,
        },
        # ---- ORC ----
        {
            "name": "ORC (snappy)",
            "spark_format": "orc",
            "dir_name": os.path.join(data_dir, "orc_snappy"),
            "write_opts": {"compression": "snappy"},
            "read_opts": {},
            "available": True,
        },
        {
            "name": "ORC (zlib)",
            "spark_format": "orc",
            "dir_name": os.path.join(data_dir, "orc_zlib"),
            "write_opts": {"compression": "zlib"},
            "read_opts": {},
            "available": True,
        },
    ]

    return formats


# ============================================================================
#  Main Benchmark Loop
# ============================================================================

def run_benchmarks(
    spark: SparkSession,
    df: DataFrame,
    output_dir: str,
) -> list[dict[str, Any]]:
    """
    Run all format benchmarks: write, full read, column select, filter, aggregation.

    Returns a list of result dicts.
    """
    formats = build_format_registry(output_dir)
    results: list[dict[str, Any]] = []

    for fmt in formats:
        name = fmt["name"]

        if not fmt["available"]:
            print(f"\n  SKIP  {name} (not available)")
            continue

        print(f"\n{'=' * 60}")
        print(f"  Benchmarking: {name}")
        print(f"{'=' * 60}")

        spark_format = fmt["spark_format"]
        dir_path = fmt["dir_name"]
        write_opts = fmt["write_opts"]
        read_opts = fmt.get("read_opts", {})

        # ---- 1. Write ----
        print(f"  Writing ...", end=" ", flush=True)
        try:
            write_sec, size_mb = time_write(df, spark_format, dir_path, **write_opts)
            print(f"{write_sec:.2f}s  ({size_mb:.1f} MB)")
        except Exception as e:
            print(f"FAILED ({e})")
            continue

        # ---- 2. Full read ----
        print(f"  Reading (full scan) ...", end=" ", flush=True)
        try:
            # For CSV/JSON we need to pass read options
            if read_opts:
                start = time.perf_counter()
                reader = spark.read.format(spark_format)
                for k, v in read_opts.items():
                    reader = reader.option(k, v)
                read_df = reader.load(dir_path)
                row_count = read_df.count()
                read_sec = time.perf_counter() - start
            else:
                read_sec, row_count = time_read_full(spark, spark_format, dir_path)
            print(f"{read_sec:.2f}s  ({row_count:,} rows)")
        except Exception as e:
            print(f"FAILED ({e})")
            read_sec = float("nan")
            row_count = 0

        # ---- 3. Column selection ----
        print(f"  Reading (column select: {SELECT_COLUMNS}) ...", end=" ", flush=True)
        try:
            if read_opts:
                start = time.perf_counter()
                reader = spark.read.format(spark_format)
                for k, v in read_opts.items():
                    reader = reader.option(k, v)
                cols_df = reader.load(dir_path).select(*SELECT_COLUMNS)
                cols_count = cols_df.count()
                read_cols_sec = time.perf_counter() - start
            else:
                read_cols_sec, cols_count = time_read_cols(spark, spark_format, dir_path)
            print(f"{read_cols_sec:.2f}s  ({cols_count:,} rows)")
        except Exception as e:
            print(f"FAILED ({e})")
            read_cols_sec = float("nan")

        # ---- 4. Filtered read ----
        print(f"  Reading (filter: event_type='{FILTER_EVENT_TYPE}') ...", end=" ", flush=True)
        try:
            if read_opts:
                start = time.perf_counter()
                reader = spark.read.format(spark_format)
                for k, v in read_opts.items():
                    reader = reader.option(k, v)
                filt_df = reader.load(dir_path).filter(
                    F.col("event_type") == FILTER_EVENT_TYPE
                )
                filt_count = filt_df.count()
                read_filt_sec = time.perf_counter() - start
            else:
                read_filt_sec, filt_count = time_read_filter(spark, spark_format, dir_path)
            print(f"{read_filt_sec:.2f}s  ({filt_count:,} rows)")
        except Exception as e:
            print(f"FAILED ({e})")
            read_filt_sec = float("nan")

        # ---- 5. Aggregation ----
        print(f"  Aggregation (groupBy country, event_type) ...", end=" ", flush=True)
        try:
            if read_opts:
                start = time.perf_counter()
                reader = spark.read.format(spark_format)
                for k, v in read_opts.items():
                    reader = reader.option(k, v)
                agg_base = reader.load(dir_path)
                agg_df = agg_base.groupBy("country", "event_type").agg(
                    F.count("*").alias("cnt"),
                    F.round(F.avg("duration_sec"), 2).alias("avg_duration"),
                    F.round(F.sum("revenue"), 2).alias("total_revenue"),
                )
                agg_count = agg_df.count()
                agg_sec = time.perf_counter() - start
            else:
                agg_sec, agg_count = time_aggregation(spark, spark_format, dir_path)
            print(f"{agg_sec:.2f}s  ({agg_count} groups)")
        except Exception as e:
            print(f"FAILED ({e})")
            agg_sec = float("nan")

        # ---- Collect result ----
        results.append({
            "Format": name,
            "File Size (MB)": round(size_mb, 2),
            "Write (s)": round(write_sec, 2),
            "Read Full (s)": round(read_sec, 2),
            "Read Cols (s)": round(read_cols_sec, 2),
            "Read Filter (s)": round(read_filt_sec, 2),
            "Aggregation (s)": round(agg_sec, 2),
        })

    return results


# ============================================================================
#  Result Reporting
# ============================================================================

def print_results_table(results: list[dict[str, Any]]) -> None:
    """Pretty-print the results table."""
    print("\n")
    print("=" * 95)
    print("  PYSPARK BENCHMARK RESULTS")
    print("=" * 95)
    print(tabulate(
        results,
        headers="keys",
        tablefmt="pretty",
        numalign="right",
        floatfmt=".2f",
    ))
    print()


def save_results_csv(results: list[dict[str, Any]], output_dir: str) -> str:
    """Save results as a CSV file. Returns the path."""
    import csv
    path = os.path.join(output_dir, "pyspark_benchmark_results.csv")
    if not results:
        return path
    fieldnames = results[0].keys()
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"  Results saved to: {path}")
    return path


def generate_charts(results: list[dict[str, Any]], output_dir: str) -> None:
    """Generate comparison bar charts for key metrics."""
    if plt is None:
        print("  Skipping chart generation (matplotlib not installed).")
        return

    if not results:
        return

    charts_dir = os.path.join(output_dir, "pyspark_charts")
    os.makedirs(charts_dir, exist_ok=True)

    try:
        plt.style.use("seaborn-v0_8-whitegrid")
    except OSError:
        pass  # Style not available, use default

    formats = [r["Format"] for r in results]

    metrics = [
        ("File Size (MB)", "File Size Comparison (PySpark)", "file_size.png", "steelblue"),
        ("Write (s)", "Write Time Comparison (PySpark)", "write_time.png", "darkorange"),
        ("Read Full (s)", "Full Read Time (PySpark)", "read_full.png", "seagreen"),
        ("Read Cols (s)", "Column Selection Read Time (PySpark)", "read_cols.png", "mediumpurple"),
        ("Read Filter (s)", "Filtered Read Time (PySpark)", "read_filter.png", "indianred"),
        ("Aggregation (s)", "Aggregation Time (PySpark)", "aggregation.png", "goldenrod"),
    ]

    for col_key, title, filename, color in metrics:
        values = [r.get(col_key, 0) for r in results]

        # Skip if all values are NaN or zero
        valid_values = [v for v in values if v and v == v]  # NaN != NaN
        if not valid_values:
            continue

        fig, ax = plt.subplots(figsize=(12, max(6, len(formats) * 0.5)))
        bars = ax.barh(formats, values, color=color, edgecolor="white")

        # Add value labels on bars
        for bar, val in zip(bars, values):
            if val and val == val:  # not NaN
                ax.text(
                    bar.get_width() + ax.get_xlim()[1] * 0.01,
                    bar.get_y() + bar.get_height() / 2,
                    f"{val:.2f}",
                    va="center",
                    fontsize=9,
                )

        ax.set_xlabel(col_key)
        ax.set_title(title, fontsize=14, fontweight="bold")
        ax.invert_yaxis()
        plt.tight_layout()

        chart_path = os.path.join(charts_dir, filename)
        fig.savefig(chart_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"  Chart saved: {chart_path}")


def print_key_takeaways(results: list[dict[str, Any]]) -> None:
    """Print key observations from the benchmark results."""
    if not results:
        return

    print("\n" + "=" * 70)
    print("  KEY TAKEAWAYS")
    print("=" * 70)

    # Find smallest / largest file
    by_size = sorted(results, key=lambda r: r.get("File Size (MB)", float("inf")))
    if by_size:
        smallest = by_size[0]
        largest = by_size[-1]
        print(f"  Smallest file: {smallest['Format']} ({smallest['File Size (MB)']:.1f} MB)")
        print(f"  Largest file:  {largest['Format']} ({largest['File Size (MB)']:.1f} MB)")
        if smallest["File Size (MB)"] > 0:
            ratio = largest["File Size (MB)"] / smallest["File Size (MB)"]
            print(f"  Size ratio:    {ratio:.1f}x")

    # Fastest / slowest write
    by_write = sorted(results, key=lambda r: r.get("Write (s)", float("inf")))
    if by_write:
        print(f"\n  Fastest write: {by_write[0]['Format']} ({by_write[0]['Write (s)']:.2f}s)")
        print(f"  Slowest write: {by_write[-1]['Format']} ({by_write[-1]['Write (s)']:.2f}s)")

    # Column projection speedup
    print("\n  Column projection speedup (Read Cols vs Read Full):")
    for r in results:
        full = r.get("Read Full (s)", 0)
        cols = r.get("Read Cols (s)", 0)
        if full and cols and full == full and cols == cols and cols > 0:
            speedup = full / cols
            print(f"    {r['Format']:25s}  {speedup:.2f}x")

    # Avro comparison
    avro_results = [r for r in results if "Avro" in r["Format"]]
    parquet_results = [r for r in results if "Parquet" in r["Format"]]
    if avro_results and parquet_results:
        avg_avro_write = sum(r["Write (s)"] for r in avro_results) / len(avro_results)
        avg_parquet_write = sum(r["Write (s)"] for r in parquet_results) / len(parquet_results)
        print(f"\n  Avro vs Parquet (average write time):")
        print(f"    Avro:    {avg_avro_write:.2f}s")
        print(f"    Parquet: {avg_parquet_write:.2f}s")

        avg_avro_read = sum(r["Read Full (s)"] for r in avro_results) / len(avro_results)
        avg_parquet_read = sum(r["Read Full (s)"] for r in parquet_results) / len(parquet_results)
        print(f"  Avro vs Parquet (average full read time):")
        print(f"    Avro:    {avg_avro_read:.2f}s")
        print(f"    Parquet: {avg_parquet_read:.2f}s")

    print()
    print("  NOTE: In PySpark, Avro is handled by the JVM-based spark-avro library,")
    print("  which is MUCH faster than Python's fastavro. Compare these results with")
    print("  benchmark_formats.py (which uses fastavro) to see the dramatic difference!")
    print()


# ============================================================================
#  Cleanup
# ============================================================================

def cleanup_spark_files(output_dir: str) -> None:
    """Remove Spark output directories to free disk space."""
    data_dir = os.path.join(output_dir, "spark_data_files")
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
        print(f"  Cleaned up Spark data files: {data_dir}")


# ============================================================================
#  CLI Entry Point
# ============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark file formats using PySpark. "
            "Demonstrates that Avro is efficient in Spark (vs slow in pure Python)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pyspark_benchmark.py                       # 5M rows (default)
  python pyspark_benchmark.py --quick               # 1M rows (quick demo)
  python pyspark_benchmark.py --num-rows 10000000   # 10M rows
  python pyspark_benchmark.py --quick --keep        # Keep data files after benchmark
        """,
    )
    parser.add_argument(
        "--num-rows",
        type=int,
        default=None,
        help="Number of rows to benchmark. Overrides --quick. Default: 5,000,000.",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Quick mode: use 1M rows for a fast demo.",
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
        help="Keep generated data files after the benchmark.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Determine number of rows
    if args.num_rows is not None:
        num_rows = args.num_rows
    elif args.quick:
        num_rows = 1_000_000
    else:
        num_rows = 5_000_000

    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # Path to step_1 generated data (if it exists)
    source_parquet = os.path.join(output_dir, "generated_data.parquet")

    print("=" * 70)
    print("  PYSPARK FILE FORMAT BENCHMARK")
    print("=" * 70)
    print(f"  Rows             : {num_rows:,}")
    print(f"  Output directory : {output_dir}")
    print(f"  Quick mode       : {args.quick}")
    print(f"  Keep data files  : {args.keep}")
    print(f"  Source Parquet    : {source_parquet}")
    print(f"  matplotlib       : {'available' if plt else 'not available'}")
    print("=" * 70)

    total_start = time.perf_counter()

    # ---- Step 1: Create SparkSession ----
    print("\n[Step 1/5] Creating SparkSession ...")
    try:
        spark = create_spark_session()
    except Exception as e:
        print(f"\nERROR: Failed to create SparkSession: {e}")
        print("Make sure Java is installed and JAVA_HOME is set correctly.")
        sys.exit(1)

    try:
        # ---- Step 2: Load or generate data ----
        print("\n[Step 2/5] Loading / generating data ...")
        df = load_or_generate_data(spark, num_rows, source_parquet)

        # ---- Step 3: Run benchmarks ----
        print("\n[Step 3/5] Running benchmarks ...")
        results = run_benchmarks(spark, df, output_dir)

        if not results:
            print("\nNo formats were benchmarked successfully. Exiting.")
            spark.stop()
            sys.exit(1)

        # Unpersist cached data
        df.unpersist()

        # ---- Step 4: Report results ----
        print("\n[Step 4/5] Reporting results ...")
        print_results_table(results)
        save_results_csv(results, output_dir)
        generate_charts(results, output_dir)
        print_key_takeaways(results)

        # ---- Step 5: Cleanup ----
        if not args.keep:
            print("[Step 5/5] Cleaning up data files ...")
            cleanup_spark_files(output_dir)
        else:
            print("[Step 5/5] Keeping data files (--keep flag set).")
            data_dir = os.path.join(output_dir, "spark_data_files")
            print(f"  Data files in: {data_dir}")

    finally:
        # Always stop SparkSession to clean up JVM resources
        print("\n  Stopping SparkSession ...")
        spark.stop()

    total_elapsed = time.perf_counter() - total_start
    print(f"\n  Total benchmark time: {total_elapsed:.1f}s")
    print("  Done!")


if __name__ == "__main__":
    main()
