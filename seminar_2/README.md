# Семинар 2 — Сжатие и форматы файлов для больших данных / ETL

## Структура семинара (~80 мин)

| # | Часть | Продолжительность | Материал |
|---|-------|-------------------|----------|
| 1 | Глубокий разбор формата Parquet | ~30 мин | `parquet_format_presentation.pptx` |
| 2 | Теория: Avro, ORC, CSV, JSON, Arrow | ~20 мин | `columnar_and_row_formats_theory.md` |
| 3 | Практический бенчмарк | ~20-30 мин | Python-скрипты (см. ниже) |

## Файлы

### Презентация и теория

- **`parquet_format_presentation.pptx`** — Слайды, охватывающие внутреннее устройство Parquet: группы строк, фрагменты столбцов, страницы, кодирование (RLE_DICTIONARY, PLAIN), кодеки сжатия, предикатный pushdown, партиционирование, Delta Lake.
- **`columnar_and_row_formats_theory.md`** — Конспект лекции по форматам Avro, ORC, CSV, JSON, Arrow/Feather. Включает сравнительные таблицы, обзор алгоритмов сжатия (Snappy, GZIP, LZ4, ZSTD) и рекомендации по выбору формата.

### Практический бенчмарк — всё в одном файле

- **`benchmark_formats.py`** — Бенчмарк в одном файле, который генерирует ~3 ГБ синтетических данных (30 млн строк) и сравнивает 15 вариантов формат/сжатие (CSV, JSON Lines, Parquet, Avro, ORC, Feather). Измеряет время записи, размер файла, полное чтение, выбор столбцов, чтение с фильтрацией и агрегацию. Использует потоковую запись для экономии памяти.
  ```bash
  pip install -r requirements.txt
  python benchmark_formats.py          # полный запуск (~3 ГБ CSV, 15-30 мин)
  python benchmark_formats.py --quick  # сокращённый запуск (~500 МБ, 5-7 мин)
  ```

### Практический бенчмарк — пошагово (для разбора в аудитории)

Запускайте скрипты по порядку. Каждый шаг независим и сохраняет результаты для следующего.

- **`common.py`** — Общие импорты, константы и вспомогательные функции, используемые всеми шагами.
- **`step_1_generate_data.py`** — Генерирует 30 млн синтетических строк событий и записывает их в Parquet-файл потоково. Никогда не держит весь набор данных в памяти.
  ```bash
  python step_1_generate_data.py                     # 30 млн строк (по умолчанию)
  python step_1_generate_data.py --num-rows 5000000  # 5 млн строк (быстрый режим)
  ```
- **`step_2_write_formats.py`** — Считывает сгенерированные данные Parquet и записывает их во все 15 вариантов форматов. Измеряет время записи и размер файла.
  ```bash
  python step_2_write_formats.py
  ```
- **`step_3_read_benchmarks.py`** — Считывает каждый записанный файл и измеряет: полное сканирование, выбор столбцов, чтение с фильтрацией и агрегацию с группировкой.
  ```bash
  python step_3_read_benchmarks.py
  ```
- **`step_4_visualize_results.py`** — Объединяет результаты записи и чтения, выводит сводную таблицу, генерирует сравнительные столбчатые диаграммы (PNG) и выделяет ключевые выводы.
  ```bash
  python step_4_visualize_results.py
  ```

### Прочее

- **`requirements.txt`** — Зависимости Python.

## Требования

```
Python 3.10+
pip install -r requirements.txt
```

## Результаты:

step_1_generate_data.py:
```
======================================================================
  STEP 1 — GENERATE SYNTHETIC DATA
======================================================================
  Rows: 30,000,000
  Seed: 42
  Output: ./benchmark_output/generated_data.parquet
  Generating rows 1 – 500,000 …
  Generating rows 500,001 – 1,000,000 …
  Generating rows 1,000,001 – 1,500,000 …
  Generating rows 1,500,001 – 2,000,000 …
  Generating rows 2,000,001 – 2,500,000 …
  Generating rows 2,500,001 – 3,000,000 …
  Generating rows 3,000,001 – 3,500,000 …
  Generating rows 3,500,001 – 4,000,000 …
  Generating rows 4,000,001 – 4,500,000 …
  Generating rows 4,500,001 – 5,000,000 …
  Generating rows 5,000,001 – 5,500,000 …
  Generating rows 5,500,001 – 6,000,000 …
  Generating rows 6,000,001 – 6,500,000 …
  Generating rows 6,500,001 – 7,000,000 …
  Generating rows 7,000,001 – 7,500,000 …
  Generating rows 7,500,001 – 8,000,000 …
  Generating rows 8,000,001 – 8,500,000 …
  Generating rows 8,500,001 – 9,000,000 …
  Generating rows 9,000,001 – 9,500,000 …
  Generating rows 9,500,001 – 10,000,000 …
  Generating rows 10,000,001 – 10,500,000 …
  Generating rows 10,500,001 – 11,000,000 …
  Generating rows 11,000,001 – 11,500,000 …
  Generating rows 11,500,001 – 12,000,000 …
  Generating rows 12,000,001 – 12,500,000 …
  Generating rows 12,500,001 – 13,000,000 …
  Generating rows 13,000,001 – 13,500,000 …
  Generating rows 13,500,001 – 14,000,000 …
  Generating rows 14,000,001 – 14,500,000 …
  Generating rows 14,500,001 – 15,000,000 …
  Generating rows 15,000,001 – 15,500,000 …
  Generating rows 15,500,001 – 16,000,000 …
  Generating rows 16,000,001 – 16,500,000 …
  Generating rows 16,500,001 – 17,000,000 …
  Generating rows 17,000,001 – 17,500,000 …
  Generating rows 17,500,001 – 18,000,000 …
  Generating rows 18,000,001 – 18,500,000 …
  Generating rows 18,500,001 – 19,000,000 …
  Generating rows 19,000,001 – 19,500,000 …
  Generating rows 19,500,001 – 20,000,000 …
  Generating rows 20,000,001 – 20,500,000 …
  Generating rows 20,500,001 – 21,000,000 …
  Generating rows 21,000,001 – 21,500,000 …
  Generating rows 21,500,001 – 22,000,000 …
  Generating rows 22,000,001 – 22,500,000 …
  Generating rows 22,500,001 – 23,000,000 …
  Generating rows 23,000,001 – 23,500,000 …
  Generating rows 23,500,001 – 24,000,000 …
  Generating rows 24,000,001 – 24,500,000 …
  Generating rows 24,500,001 – 25,000,000 …
  Generating rows 25,000,001 – 25,500,000 …
  Generating rows 25,500,001 – 26,000,000 …
  Generating rows 26,000,001 – 26,500,000 …
  Generating rows 26,500,001 – 27,000,000 …
  Generating rows 27,000,001 – 27,500,000 …
  Generating rows 27,500,001 – 28,000,000 …
  Generating rows 28,000,001 – 28,500,000 …
  Generating rows 28,500,001 – 29,000,000 …
  Generating rows 29,000,001 – 29,500,000 …
  Generating rows 29,500,001 – 30,000,000 …

  Generation took 205.6s
  Parquet file: 872.8 MB on disk

  Quick data review (first 5 rows):
 event_id  user_id           timestamp event_type page_url  duration_sec  revenue  device country                                    session_tags
        1    89251 2025-05-05 19:46:38     logout  /search     18.681455     <NA>  mobile      AU          ["ab_test_v1", "referral", "new_user"]
        2   773957 2025-02-22 13:19:49     scroll    /docs    270.448029     <NA> desktop      FR                           ["returning", "paid"]
        3   654572 2025-01-01 04:09:45     scroll   /about     15.650785     <NA> desktop      GB ["bot_suspect", "ab_test_v1", "organic", "vip"]
        4   438879 2025-09-13 10:51:12  page_view   /about     80.916347     <NA> desktop      DE             ["promo_active", "paid", "organic"]
        5   433016 2025-03-26 20:33:18      click  /search    138.164906     <NA>  mobile      US                                ["promo_active"]

  Total rows: 30,000,000

  Done! Run step_2_write_formats.py next.
```

step_2_write_formats.py:
```
| Format | File Size \(MB\) | Write Time \(s\) |
| :--- | :--- | :--- |
| CSV | 3414.61 | 71.13 |
| CSV + gzip | 893.61 | 188.37 |
| JSON Lines | 7222.76 | 72.69 |
| JSON Lines + gzip | 935.95 | 185.04 |
| Parquet \(snappy\) | 837.84 | 9.33 |
| Parquet \(gzip\) | 664.53 | 277.14 |
| Parquet \(zstd\) | 660.34 | 9.54 |
| Parquet \(none\) | 1047.95 | 8.3 |
| ORC \(snappy\) | 886.48 | 12.93 |
| ORC \(zlib\) | 672.12 | 29.29 |
```

step_3_read_benchmarks.py:
```
======================================================================
  STEP 3 — READ BENCHMARKS
======================================================================

============================================================
  CSV
============================================================
  Full scan … 21.39s  (30,000,000 rows)
  Column select (['user_id', 'event_type', 'revenue']) … 9.50s  (30,000,000 rows, 3 cols)
  Filter (event_type='purchase') … 24.26s  (5,999,364 rows)
  Aggregation (groupby) … 1.70s

============================================================
  CSV + gzip
============================================================
  Full scan … 25.45s  (30,000,000 rows)
  Column select (['user_id', 'event_type', 'revenue']) … 13.52s  (30,000,000 rows, 3 cols)
  Filter (event_type='purchase') … 27.47s  (5,999,364 rows)
  Aggregation (groupby) … 1.54s

============================================================
  Parquet (snappy)
============================================================
  Full scan … 2.88s  (30,000,000 rows)
  Column select (['user_id', 'event_type', 'revenue']) … 0.58s  (30,000,000 rows, 3 cols)
  Filter (event_type='purchase') … 0.92s  (5,999,364 rows)
  Aggregation (groupby) … 1.61s

============================================================
  Parquet (gzip)
============================================================
  Full scan … 2.83s  (30,000,000 rows)
  Column select (['user_id', 'event_type', 'revenue']) … 0.71s  (30,000,000 rows, 3 cols)
  Filter (event_type='purchase') … 1.11s  (5,999,364 rows)
  Aggregation (groupby) … 1.51s

============================================================
  Parquet (zstd)
============================================================
  Full scan … 2.64s  (30,000,000 rows)
  Column select (['user_id', 'event_type', 'revenue']) … 0.65s  (30,000,000 rows, 3 cols)
  Filter (event_type='purchase') … 0.96s  (5,999,364 rows)
  Aggregation (groupby) … 1.49s

============================================================
  Parquet (none)
============================================================
  Full scan … 2.74s  (30,000,000 rows)
  Column select (['user_id', 'event_type', 'revenue']) … 0.59s  (30,000,000 rows, 3 cols)
  Filter (event_type='purchase') … 0.92s  (5,999,364 rows)
  Aggregation (groupby) … 1.51s

============================================================
  ORC (snappy)
============================================================
  Full scan … 6.18s  (30,000,000 rows)
  Column select (['user_id', 'event_type', 'revenue']) … 1.02s  (30,000,000 rows, 3 cols)
  Filter (event_type='purchase') … 6.27s  (5,999,364 rows)
  Aggregation (groupby) … 1.52s

============================================================
  ORC (zlib)
============================================================
  Full scan … 9.28s  (30,000,000 rows)
  Column select (['user_id', 'event_type', 'revenue']) … 1.65s  (30,000,000 rows, 3 cols)
  Filter (event_type='purchase') … 9.38s  (5,999,364 rows)
  Aggregation (groupby) … 1.54s

======================================================================
  READ BENCHMARK RESULTS
======================================================================
+------------------+---------------+---------------+-----------------+-----------------+
|      Format      | Read Full (s) | Read Cols (s) | Read Filter (s) | Aggregation (s) |
+------------------+---------------+---------------+-----------------+-----------------+
|       CSV        |     21.39     |      9.5      |      24.26      |       1.7       |
|    CSV + gzip    |     25.45     |     13.52     |      27.47      |      1.54       |
| Parquet (snappy) |     2.88      |     0.58      |      0.92       |      1.61       |
|  Parquet (gzip)  |     2.83      |     0.71      |      1.11       |      1.51       |
|  Parquet (zstd)  |     2.64      |     0.65      |      0.96       |      1.49       |
|  Parquet (none)  |     2.74      |     0.59      |      0.92       |      1.51       |
|   ORC (snappy)   |     6.18      |     1.02      |      6.27       |      1.52       |
|    ORC (zlib)    |     9.28      |     1.65      |      9.38       |      1.54       |
+------------------+---------------+---------------+-----------------+-----------------+

  Results saved to: ./benchmark_output/read_results.csv

  Done! Run step_4_visualize_results.py next.
```

step_4_visualize_results.py:
```
charts/file_size.png
charts/write_time.png
charts/read_full.png
charts/read_cols.png
charts/read_filter.png
charts/aggregation.png
```

pyspark_benchmark.py
```
| Format | File Size \(MB\) | Write \(s\) | Read Full \(s\) | Read Cols \(s\) | Read Filter \(s\) | Aggregation \(s\) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| CSV | 572.23 | 1.71 | 2.38 | 1.7 | 2.27 | 2.37 |
| CSV + gzip | 137.02 | 2.59 | 1.66 | 1.59 | 2.22 | 2.33 |
| JSON Lines | 1133.0 | 1.3 | 2.01 | 1.52 | 1.45 | 1.87 |
| JSON Lines + gzip | 155.69 | 2.28 | 1.56 | 1.61 | 1.45 | 1.98 |
| Parquet \(snappy\) | 135.68 | 1.84 | 0.2 | 0.08 | 0.22 | 0.27 |
| Parquet \(gzip\) | 103.27 | 2.05 | 0.06 | 0.08 | 0.08 | 0.28 |
| Parquet \(zstd\) | 98.85 | 1.05 | 0.07 | 0.06 | 0.08 | 0.27 |
| Parquet \(none\) | 186.35 | 0.9 | 0.06 | 0.06 | 0.17 | 0.14 |
| Avro \(snappy\) | 161.92 | 0.81 | 0.46 | 0.22 | 0.28 | 0.29 |
| Avro \(deflate\) | 126.42 | 1.39 | 0.28 | 0.29 | 0.33 | 0.5 |
| ORC \(snappy\) | 83.13 | 1.22 | 0.21 | 0.1 | 0.2 | 0.36 |
| ORC \(zlib\) | 75.41 | 1.02 | 0.07 | 0.04 | 0.06 | 0.26 |
```