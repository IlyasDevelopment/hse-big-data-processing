# Seminar 2 — Compression & File Formats for Big Data / ETL

## Seminar Structure (~80 min)

| # | Part | Duration | Material |
|---|------|----------|----------|
| 1 | Parquet format deep-dive | ~30 min | `parquet_format_presentation.pptx` |
| 2 | Theory: Avro, ORC, CSV, JSON, Arrow | ~20 min | `columnar_and_row_formats_theory.md` |
| 3 | Hands-on benchmark | ~20-30 min | Python scripts (see below) |

## Files

### Presentation & Theory

- **`parquet_format_presentation.pptx`** — Slides covering Parquet internals: row-groups, column chunks, pages, encoding (RLE_DICTIONARY, PLAIN), compression codecs, predicate pushdown, partitioning, Delta Lake.
- **`columnar_and_row_formats_theory.md`** — Lecture notes on Avro, ORC, CSV, JSON, Arrow/Feather formats. Includes comparison tables, compression algorithm overview (Snappy, GZIP, LZ4, ZSTD), and format selection guidelines.

### Practical Benchmark — All-in-One

- **`benchmark_formats.py`** — Single-file benchmark that generates ~3 GB of synthetic data (30M rows) and compares 15 format/compression variants (CSV, JSON Lines, Parquet, Avro, ORC, Feather). Measures write time, file size, full read, column selection, filtered read, and aggregation. Uses streaming writes to keep memory usage low.
  ```bash
  pip install -r requirements.txt
  python benchmark_formats.py          # full run (~3 GB CSV, 15-30 min)
  python benchmark_formats.py --quick  # smaller run (~500 MB, 5-7 min)
  ```

### Practical Benchmark — Step-by-Step (for classroom walkthrough)

Run these in order. Each step is independent and saves results for the next.

- **`common.py`** — Shared imports, constants, and helper functions used by all steps.
- **`step_1_generate_data.py`** — Generates 30M synthetic event rows and streams them to a Parquet file. Never holds the full dataset in memory.
  ```bash
  python step_1_generate_data.py                     # 30M rows (default)
  python step_1_generate_data.py --num-rows 5000000  # 5M rows (quick)
  ```
- **`step_2_write_formats.py`** — Reads the generated Parquet data and writes it to all 15 format variants. Measures write time and file size.
  ```bash
  python step_2_write_formats.py
  ```
- **`step_3_read_benchmarks.py`** — Reads each written file and benchmarks: full scan, column selection, filtered read, and groupby aggregation.
  ```bash
  python step_3_read_benchmarks.py
  ```
- **`step_4_visualize_results.py`** — Merges write + read results, prints a combined table, generates comparison bar charts (PNG), and highlights key takeaways.
  ```bash
  python step_4_visualize_results.py
  ```

### Other

- **`requirements.txt`** — Python dependencies.

## Requirements

```
Python 3.10+
pip install -r requirements.txt
```
