"""
Shared imports, constants, and helpers for the benchmark steps.
Run this first to verify all dependencies are installed.
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
#  Dependency checks
# ---------------------------------------------------------------------------

try:
    import numpy as np
except ImportError:
    sys.exit("ERROR: numpy is not installed.  Run:  pip install numpy")

try:
    import pandas as pd
except ImportError:
    sys.exit("ERROR: pandas is not installed.  Run:  pip install pandas")

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.feather as feather
except ImportError:
    sys.exit("ERROR: pyarrow is not installed.  Run:  pip install pyarrow")

try:
    import pyarrow.orc as orc
    HAS_ORC = True
except ImportError:
    HAS_ORC = False
    print("WARNING: pyarrow.orc is not available. ORC benchmarks will be skipped.")

try:
    import fastavro
except ImportError:
    fastavro = None
    print("WARNING: fastavro is not installed. Avro benchmarks will be skipped.")

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

# ---------------------------------------------------------------------------
#  Constants for data generation
# ---------------------------------------------------------------------------

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

CHUNK_SIZE = 500_000

# Columns used for column-projection benchmark
SELECT_COLUMNS = ["user_id", "event_type", "revenue"]

# Filter value for predicate push-down benchmark
FILTER_EVENT_TYPE = "purchase"

# Default paths
DEFAULT_OUTPUT_DIR = "./benchmark_output"
GENERATED_DATA_PATH = os.path.join(DEFAULT_OUTPUT_DIR, "generated_data.parquet")
DATA_FILES_DIR = os.path.join(DEFAULT_OUTPUT_DIR, "data_files")
WRITE_RESULTS_PATH = os.path.join(DEFAULT_OUTPUT_DIR, "write_results.csv")
READ_RESULTS_PATH = os.path.join(DEFAULT_OUTPUT_DIR, "read_results.csv")
FULL_RESULTS_PATH = os.path.join(DEFAULT_OUTPUT_DIR, "benchmark_results.csv")

# ---------------------------------------------------------------------------
#  Helpers
# ---------------------------------------------------------------------------

def time_it(func, *args, **kwargs) -> tuple[float, Any]:
    """Run *func*, measure wall-clock time. Returns (elapsed_seconds, result)."""
    start = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - start
    return elapsed, result


def file_size_mb(path: str) -> float:
    """Return the file size in megabytes."""
    return os.path.getsize(path) / (1024 ** 2)


def ensure_output_dir():
    """Create the output directory tree."""
    os.makedirs(DEFAULT_OUTPUT_DIR, exist_ok=True)
    os.makedirs(DATA_FILES_DIR, exist_ok=True)
