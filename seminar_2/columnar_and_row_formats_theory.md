# Big Data File Formats for ETL Processing

> **Seminar 2 -- Supplementary Theory Material**
> Duration: ~20 minutes of lecture
> Prerequisites: Familiarity with the Parquet format (covered in the separate 30-minute presentation)

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Apache Avro](#2-apache-avro)
3. [Apache ORC (Optimized Row Columnar)](#3-apache-orc-optimized-row-columnar)
4. [CSV (Comma-Separated Values)](#4-csv-comma-separated-values)
5. [JSON / JSONL (JSON Lines)](#5-json--jsonl-json-lines)
6. [Apache Arrow (In-Memory Columnar Format)](#6-apache-arrow-in-memory-columnar-format)
7. [Compression Algorithms Overview](#7-compression-algorithms-overview)
8. [Comprehensive Format Comparison](#8-comprehensive-format-comparison)
9. [Key Takeaways](#9-key-takeaways)
10. [References and Further Reading](#10-references-and-further-reading)

---

## 1. Introduction

When building ETL (Extract, Transform, Load) pipelines in big data environments, the choice of file format has a profound impact on performance, storage cost, and system interoperability. There is no single "best" format -- the right choice depends on the workload characteristics: whether the system is read-heavy or write-heavy, whether the data is structured or semi-structured, whether schema evolution is required, and what query engine sits downstream.

This material covers the most widely used file formats in the big data ecosystem **other than Apache Parquet**, which is discussed in the separate 30-minute presentation. We will examine each format's internal architecture, strengths, weaknesses, and ideal use cases. At the end, we bring everything together in a comprehensive comparison table that includes Parquet for completeness.

### The Fundamental Storage Model Decision

Before diving into individual formats, it is worth recalling the two fundamental approaches to organizing data on disk:

- **Row-oriented storage** writes all fields of a single record contiguously. This is optimal for workloads that read or write entire rows at a time -- transactional systems, streaming ingestion, and record-level lookups.

- **Column-oriented (columnar) storage** writes all values of a single column contiguously. This is optimal for analytical queries that touch only a subset of columns, because the engine can skip irrelevant columns entirely and benefit from much better compression ratios within homogeneous data.

Most modern big data formats fall clearly into one camp or the other, though some (like ORC and Parquet) use a hybrid approach with row-groups containing columnar data.

---

## 2. Apache Avro

### 2.1 History and Origin

Apache Avro was created in 2009 by **Doug Cutting**, the same engineer who created both Apache Hadoop and Apache Lucene. Cutting designed Avro specifically to address the shortcomings of Hadoop's existing serialization framework (Writables), which was tightly coupled to Java and made cross-language data exchange painful.

Avro became a top-level Apache project in 2010 and has since become the de facto standard for **data serialization** in streaming architectures, particularly in the Apache Kafka ecosystem. Its design philosophy prioritizes schema evolution, compact binary encoding, and language neutrality.

### 2.2 Schema System

One of Avro's most distinctive features is its **rich, JSON-based schema definition language**. Every Avro file embeds its schema directly in the file header, which means the data is always self-describing -- a reader never needs an external schema registry to interpret the bytes (though schema registries like Confluent Schema Registry are commonly used in Kafka deployments for efficiency).

#### Primitive Types

Avro supports the following primitive types:

| Type      | Description                        |
|-----------|------------------------------------|
| `null`    | No value                           |
| `boolean` | Binary true/false                  |
| `int`     | 32-bit signed integer              |
| `long`    | 64-bit signed integer              |
| `float`   | 32-bit IEEE 754 floating point     |
| `double`  | 64-bit IEEE 754 floating point     |
| `bytes`   | Sequence of 8-bit unsigned bytes   |
| `string`  | Unicode character sequence (UTF-8) |

#### Complex Types

Avro also supports complex types: `record`, `enum`, `array`, `map`, `union`, and `fixed`. These can be nested arbitrarily.

#### Schema Definition Example

Below is an example of an Avro schema defining a user event record:

```json
{
  "type": "record",
  "name": "UserEvent",
  "namespace": "com.example.analytics",
  "doc": "Represents a single user interaction event",
  "fields": [
    {
      "name": "event_id",
      "type": "string",
      "doc": "Unique identifier for the event"
    },
    {
      "name": "user_id",
      "type": "long"
    },
    {
      "name": "event_type",
      "type": {
        "type": "enum",
        "name": "EventType",
        "symbols": ["CLICK", "VIEW", "PURCHASE", "LOGOUT"]
      }
    },
    {
      "name": "timestamp",
      "type": "long",
      "logicalType": "timestamp-millis"
    },
    {
      "name": "metadata",
      "type": ["null", {
        "type": "map",
        "values": "string"
      }],
      "default": null,
      "doc": "Optional key-value metadata"
    },
    {
      "name": "tags",
      "type": {
        "type": "array",
        "items": "string"
      },
      "default": []
    }
  ]
}
```

Several things to note in this example:

- The `"namespace"` field provides a logical grouping (similar to a Java package).
- The `"doc"` fields serve as inline documentation.
- The `metadata` field uses a **union type** (`["null", {"type": "map", ...}]`) to express that the field is optional. The `default: null` means it can be omitted when writing.
- The `"logicalType"` annotation on the `timestamp` field tells consumers how to interpret the underlying `long` value -- in this case, as milliseconds since the Unix epoch.

#### Schema Evolution

Schema evolution is one of Avro's strongest selling points. Avro supports the following evolution rules:

| Change | Supported? | Notes |
|--------|-----------|-------|
| Add a field with a default value | Yes | Old readers ignore the new field; new readers use the default when reading old data |
| Remove a field that had a default | Yes | Old readers use the default; new readers ignore the removed field |
| Rename a field | Yes | Using `"aliases"` in the schema |
| Change field type (promotion) | Partially | Only certain promotions are allowed: `int` -> `long` -> `float` -> `double` |
| Add an enum symbol | Yes | But only at the end of the symbols list for backward compatibility |

The key to safe schema evolution is the concept of **compatibility modes**:

- **Backward compatible**: New schema can read data written with the old schema.
- **Forward compatible**: Old schema can read data written with the new schema.
- **Full compatible**: Both directions work.

In Kafka-based architectures, the **Confluent Schema Registry** enforces these compatibility rules automatically, rejecting schema changes that would break consumers.

### 2.3 Row-Based Storage Model

Avro is a **row-oriented** format. Each record is serialized as a contiguous sequence of bytes, with fields written in the order they appear in the schema. This makes Avro excellent for:

- **Write-heavy workloads**: Appending a new record is a simple sequential write.
- **Record-level processing**: Reading an entire record is a single sequential scan.
- **Streaming**: Each record is independently decodable (given the schema), making it ideal for message-by-message processing in Kafka.

However, row orientation means that Avro is **not** the best choice for analytical queries that only need a few columns out of many. In such cases, the engine must deserialize entire records even if it only needs one or two fields, wasting both I/O bandwidth and CPU cycles.

### 2.4 Binary Encoding and Serialization

Avro uses a compact **binary encoding** that omits field names and type tags from the serialized data. Because the schema is known at both write time and read time, the encoder simply writes values in schema-field order, and the decoder reads them back in the same order. This results in very compact serialized representations:

- **Integers** are encoded using variable-length zig-zag encoding (small values use fewer bytes).
- **Strings** are length-prefixed.
- **Union types** are prefixed with a zero-based index indicating which branch of the union is present.
- **Arrays and maps** are written as a series of blocks, where each block has a count followed by that many elements, terminated by a zero-count block.

Because field names are not repeated in the data (unlike JSON), Avro achieves significantly better space efficiency than text-based formats.

### 2.5 File Structure (Object Container Files)

An Avro **Object Container File** (`.avro`) has the following physical structure:

```
+------------------------------------------------------+
|                    FILE HEADER                        |
|  - Magic bytes: 4 bytes ("Obj" + 0x01)               |
|  - File metadata (map of string -> bytes):            |
|      "avro.schema" -> JSON schema string              |
|      "avro.codec"  -> compression codec name          |
|  - 16-byte sync marker (randomly generated)           |
+------------------------------------------------------+
|                   DATA BLOCK 1                        |
|  - Count of objects in this block (long)              |
|  - Size of serialized objects in bytes (long)         |
|  - Serialized objects (possibly compressed)           |
|  - 16-byte sync marker                                |
+------------------------------------------------------+
|                   DATA BLOCK 2                        |
|  - Count of objects in this block (long)              |
|  - Size of serialized objects in bytes (long)         |
|  - Serialized objects (possibly compressed)           |
|  - 16-byte sync marker                                |
+------------------------------------------------------+
|                      ...                              |
+------------------------------------------------------+
```

Key architectural points:

- The **sync marker** is a 16-byte random value generated once per file and repeated at the end of every data block. It serves two purposes: (1) it enables a reader to find block boundaries when seeking into the middle of a file, and (2) it provides a basic corruption detection mechanism.
- Each **data block** is independently compressed (if compression is enabled), which means blocks can be decompressed in parallel.
- The file is **splittable** at block boundaries. This is critical for distributed processing frameworks like MapReduce and Spark, which need to assign different file splits to different worker nodes.

### 2.6 Compression Support

Avro supports several compression codecs applied at the data-block level:

| Codec        | Compression Ratio | Speed   | Notes |
|--------------|-------------------|---------|-------|
| `null`       | None              | Fastest | No compression, raw bytes |
| `deflate`    | High              | Slow    | GZIP-compatible, widely supported |
| `snappy`     | Moderate          | Fast    | Good default for most workloads |
| `bzip2`      | Very High         | Very Slow | Best ratio, rarely used in practice |
| `zstandard`  | High              | Fast    | Best balance; increasingly popular |
| `xz`         | Very High         | Very Slow | Extreme compression |

In practice, **Snappy** and **Zstandard** are the most commonly used codecs in big data pipelines. Snappy is the traditional choice when throughput matters more than compression ratio; Zstandard (`zstd`) is increasingly preferred because it achieves near-GZIP compression ratios at near-Snappy speeds.

### 2.7 Use Cases

Avro is the best choice when:

- **Schema evolution is a hard requirement.** Avro's compatibility rules are the most mature and well-tooled in the ecosystem.
- **The workload is write-heavy or append-heavy.** Row-oriented serialization means writes are cheap.
- **Data flows through Apache Kafka.** Avro is the standard serialization format for Kafka messages, with first-class support from Confluent Schema Registry.
- **Cross-language interoperability is needed.** Avro has official libraries for Java, Python, C, C++, C#, Ruby, and more.
- **The data will later be converted to a columnar format.** A common pattern is to ingest data as Avro in a streaming layer, then periodically compact it into Parquet for analytical queries.

Avro is **not** the best choice when:

- The workload is analytical (selecting a few columns from wide tables).
- Maximum query performance on large datasets is the priority.
- The data is consumed primarily by SQL-based query engines.

### 2.8 Avro vs Parquet Comparison

| Characteristic         | Avro                          | Parquet                        |
|------------------------|-------------------------------|--------------------------------|
| Storage model          | Row-oriented                  | Columnar (hybrid with row groups) |
| Schema location        | Embedded in file header       | Embedded in file footer        |
| Schema evolution       | Excellent (first-class)       | Good (but more limited)        |
| Write performance      | Fast (sequential append)      | Slower (columnar reorganization) |
| Read performance (full row) | Fast                     | Moderate (must reconstruct row) |
| Read performance (few columns) | Slow (reads all columns) | Very fast (column pruning)    |
| Compression ratio      | Moderate                      | High (homogeneous columns)     |
| Splittable             | Yes (at block boundaries)     | Yes (at row-group boundaries)  |
| Nested data            | Yes (union, array, map)       | Yes (Dremel encoding)          |
| Primary use case       | Serialization, streaming, ETL write path | Analytics, ETL read path |
| Ecosystem integration  | Kafka, Flink, NiFi            | Spark, Hive, Impala, Trino, DuckDB |

---

## 3. Apache ORC (Optimized Row Columnar)

### 3.1 History and Origin

Apache ORC (Optimized Row Columnar) was created by **Hortonworks** in 2013, specifically to optimize query performance in **Apache Hive**. The original motivation was that Hive's default text-based storage (CSV/TSV files) and even its earlier columnar format (RCFile) were far too slow for interactive analytics. ORC was designed from the ground up to minimize I/O, maximize compression, and support predicate pushdown -- all within the Hive/HDFS ecosystem.

ORC became a top-level Apache project in 2015. While it has broadened its ecosystem support (Spark, Presto/Trino, and Flink can all read ORC), it remains most strongly associated with the **Hive/HDFS** ecosystem. In contrast, Parquet has achieved broader adoption across multi-engine environments.

### 3.2 Columnar Storage with Stripes

Like Parquet, ORC uses a **hybrid row-group/columnar** approach. The file is divided into large horizontal partitions called **stripes** (analogous to Parquet's row groups). Each stripe typically contains around **250,000 rows** by default (though this is configurable, and stripe sizes are typically 64-256 MB).

Within each stripe, data is organized by column. This means:

- An analytical query reading only 3 columns out of 100 will read data from only those 3 columns within each stripe, skipping the other 97.
- Data within a column is homogeneous (all values are the same type), which leads to excellent compression ratios.
- The stripe structure also makes the file splittable for distributed processing.

### 3.3 File Structure

The ORC file format has a well-defined physical layout. Understanding this layout is important for reasoning about query performance.

```
+------------------------------------------------------+
|                     STRIPE 1                          |
|  +--------------------------------------------------+|
|  |              INDEX DATA                           ||
|  |  - Min/max values for each column                 ||
|  |  - Row positions for seeking                      ||
|  |  - Optional bloom filter data                     ||
|  +--------------------------------------------------+|
|  |              ROW DATA                             ||
|  |  - Column 1 stream: encoded + compressed values   ||
|  |  - Column 2 stream: encoded + compressed values   ||
|  |  - ...                                            ||
|  |  - Column N stream: encoded + compressed values   ||
|  +--------------------------------------------------+|
|  |            STRIPE FOOTER                          ||
|  |  - Stream locations and sizes                     ||
|  |  - Column encoding information                    ||
|  +--------------------------------------------------+|
+------------------------------------------------------+
|                     STRIPE 2                          |
|  (same structure as above)                            |
+------------------------------------------------------+
|                      ...                              |
+------------------------------------------------------+
|                   FILE FOOTER                         |
|  - List of stripes (offset, length, row count)        |
|  - Type information (full schema)                     |
|  - Column-level statistics (min, max, sum, count,     |
|    has_null) for the entire file                      |
|  - User metadata                                      |
+------------------------------------------------------+
|                   POSTSCRIPT                          |
|  - Footer length                                      |
|  - Compression codec                                  |
|  - Compression block size                             |
|  - ORC format version                                 |
|  - Magic string "ORC"                                 |
+------------------------------------------------------+
```

The reading process works from the bottom up:

1. The reader first reads the **Postscript** (last bytes of the file) to learn the compression codec and the footer's location.
2. It then reads the **File Footer** to discover the schema, the stripe layout, and file-level statistics.
3. For each stripe, the reader reads the **Index Data** to determine whether the stripe can be skipped based on predicate pushdown.
4. Only then does it read the relevant **Row Data** streams for the columns requested by the query.

This bottom-up approach is efficient because the file metadata is compact and can be read with just 2-3 small I/O operations before any actual data is touched.

### 3.4 Built-in Indexes

ORC provides two levels of built-in indexing that enable efficient data skipping:

#### Row Group Index (Min/Max Index)

Within each stripe, ORC divides rows into **row groups** of 10,000 rows (by default). For each row group, ORC records:

- **Minimum value** of the column in that row group
- **Maximum value** of the column in that row group
- **Sum** (for numeric columns)
- **Count** of non-null values
- **Whether nulls are present**

When a query includes a predicate like `WHERE age > 30`, the reader can check the min/max statistics for the `age` column in each row group. If the maximum value in a row group is 25, the entire row group can be skipped without reading any data. This is the same principle as Parquet's column-chunk statistics, but ORC provides finer granularity with its 10,000-row default.

#### Bloom Filter Index

For columns with high cardinality (many distinct values), min/max indexes are not very selective. If a column has values from 1 to 1,000,000, almost no row group will be skipped by a point-lookup predicate like `WHERE user_id = 42`.

ORC addresses this with optional **bloom filter indexes**. A bloom filter is a probabilistic data structure that can answer the question "is this value possibly in this set?" with:

- **No false negatives**: If the bloom filter says "no", the value is definitely not present.
- **Possible false positives**: If the bloom filter says "maybe", the value might or might not be present.

Bloom filters are created per column per row group and can be enabled on specific columns:

```sql
-- In Hive DDL, enable bloom filter on specific columns:
CREATE TABLE events (
    event_id STRING,
    user_id  BIGINT,
    ts       TIMESTAMP,
    payload  STRING
)
STORED AS ORC
TBLPROPERTIES (
    "orc.bloom.filter.columns" = "event_id,user_id",
    "orc.bloom.filter.fpp"     = "0.01"
);
```

The `fpp` (false positive probability) parameter controls the trade-off between index size and accuracy. A lower FPP means a larger bloom filter but fewer false positives.

### 3.5 Encoding Schemes

ORC employs several encoding strategies to minimize data size before compression is applied. The encoder automatically selects the best encoding for each column based on the data characteristics.

| Encoding | Applicable To | How It Works |
|----------|---------------|-------------|
| **Run-Length Encoding (RLE)** | Integers, timestamps | Consecutive identical values are stored as (value, count) pairs. ORC uses an advanced "RLEv2" that handles both exact runs and delta-encoded sequences. |
| **Dictionary Encoding** | Strings with low-to-moderate cardinality | Builds a dictionary of unique values and replaces each occurrence with a short integer index. Very effective when a column has few distinct values (e.g., country codes, status values). |
| **Bit Packing** | Booleans, small integers | Packs values into the minimum number of bits needed. For booleans, this means 8 values per byte. |
| **Delta Encoding** | Sorted or nearly-sorted integers | Stores the difference between consecutive values rather than the values themselves. If values increase by a constant amount, delta encoding reduces to a very compact representation. |
| **Direct Encoding** | Strings with high cardinality | When dictionary encoding is not beneficial (too many distinct values), values are stored directly with length prefixes. |

The automatic encoding selection is one of ORC's strengths -- the writer analyzes the data during the write process and picks the most efficient encoding per column without user intervention.

### 3.6 Compression

After encoding, ORC applies a block-level compression algorithm. The available codecs are:

| Codec   | Compression Ratio | CPU Cost | Notes |
|---------|-------------------|----------|-------|
| `NONE`  | 1.0x              | Zero     | No compression |
| `ZLIB`  | High (~5-8x)      | High     | Default in many Hive installations; good compression but slow |
| `SNAPPY`| Moderate (~3-4x)  | Low      | Fast decompression; good for latency-sensitive queries |
| `LZO`   | Moderate (~3-4x)  | Low      | Similar to Snappy; requires separate library installation |
| `LZ4`   | Moderate (~3-4x)  | Very Low | Fastest decompression; lowest CPU cost |
| `ZSTD`  | High (~5-7x)      | Moderate | Best balance; increasingly the recommended default |

The compression is applied to individual **streams** within each stripe, at a configurable block size (default 256 KB). This means decompression is granular -- the reader only decompresses the specific streams it needs.

A useful rule of thumb: if disk I/O is the bottleneck (e.g., spinning HDDs, network storage), prefer higher-ratio codecs like ZLIB or ZSTD. If CPU is the bottleneck (fast SSDs, many concurrent queries), prefer faster codecs like Snappy or LZ4.

### 3.7 ACID Transaction Support

One of ORC's unique differentiators is its support for **ACID transactions in Apache Hive**. Starting with Hive 0.14, ORC tables can support:

- **INSERT, UPDATE, DELETE** operations (not just append-only writes).
- **Transactional consistency**: Readers see a consistent snapshot and are not affected by concurrent writes.
- **Compaction**: Background processes merge delta files into base files to maintain read performance.

This is implemented through a system of **base files** and **delta files**:

1. The **base file** contains the bulk of the data in standard ORC format.
2. **Delta files** record INSERT, UPDATE, and DELETE operations as small ORC files.
3. A **compaction** process periodically merges deltas into the base file.

This makes ORC the only common big data file format that supports mutable data natively within the Hive ecosystem. (Parquet achieves similar functionality through external table formats like Delta Lake, Apache Iceberg, or Apache Hudi, rather than at the file format level.)

### 3.8 Predicate Pushdown and Projection Pushdown

ORC supports two key optimization techniques that dramatically reduce I/O:

**Predicate pushdown** allows the query engine to push filter conditions (WHERE clauses) down to the file reader. The ORC reader uses stripe-level statistics, row-group-level statistics, and bloom filters to skip entire chunks of data that cannot match the predicate. For example:

```sql
SELECT user_id, event_type
FROM events
WHERE event_date = '2025-01-15'
  AND user_id = 42;
```

The ORC reader will:
1. Check stripe-level statistics for `event_date` -- skip stripes where the min/max range does not include `'2025-01-15'`.
2. Within qualifying stripes, check row-group-level statistics for further skipping.
3. If a bloom filter is available on `user_id`, check whether `42` is possibly present in each row group.

**Projection pushdown** (also called column pruning) means the reader only reads the columns listed in the SELECT clause. In the query above, only `user_id` and `event_type` column streams are read from disk -- all other columns are skipped entirely.

Together, these optimizations can reduce the amount of data read from disk by 90% or more on typical analytical queries.

### 3.9 ORC vs Parquet

| Characteristic          | ORC                              | Parquet                           |
|-------------------------|----------------------------------|-----------------------------------|
| Origin                  | Hortonworks (Hive-focused)       | Twitter + Cloudera (cross-engine) |
| Primary ecosystem       | Hive, HDFS                       | Spark, Trino, Impala, broad       |
| Row group name          | Stripe (default ~250K rows)      | Row Group (default ~128 MB)       |
| Built-in indexes        | Min/max + bloom filters          | Min/max statistics, page indexes  |
| ACID transactions       | Native (in Hive)                 | Via Delta Lake / Iceberg / Hudi   |
| Nested data model       | Struct, list, map, union         | Dremel-based repetition/definition levels |
| Default compression     | ZLIB                             | Snappy (often) or ZSTD            |
| Schema evolution        | Good (add/remove columns)        | Good (add/remove columns)         |
| Community breadth       | Narrower (Hive-centric)          | Broader (multi-engine)            |
| Performance (general)   | Comparable                       | Comparable                        |

### 3.10 When to Choose ORC vs Parquet

**Choose ORC when:**
- Your primary query engine is **Apache Hive**.
- You need **ACID transaction support** natively at the file format level.
- You are heavily invested in the **Hortonworks / HDP** ecosystem.
- You need **fine-grained indexes** (bloom filters with minimal configuration).

**Choose Parquet when:**
- You use **multiple query engines** (Spark, Trino, DuckDB, Athena, BigQuery, etc.).
- You need the **broadest ecosystem compatibility**.
- You work with **Delta Lake, Iceberg, or Hudi** table formats.
- You are building on **cloud-native** architectures (AWS, GCP, Azure all have first-class Parquet support).

In practice, the performance difference between ORC and Parquet is small for most workloads. The ecosystem and tooling considerations usually dominate the decision.

---

## 4. CSV (Comma-Separated Values)

### 4.1 Overview

CSV is the oldest and simplest tabular data format still in widespread use. A CSV file is a plain-text file where each line represents a row, and fields within a row are separated by a delimiter character (traditionally a comma, but tabs, semicolons, and pipes are also common).

```csv
event_id,user_id,event_type,timestamp,amount
evt_001,12345,PURCHASE,2025-01-15T10:30:00Z,49.99
evt_002,12346,CLICK,2025-01-15T10:31:00Z,
evt_003,12345,VIEW,2025-01-15T10:32:00Z,
```

CSV has no formal specification that is universally followed, although RFC 4180 provides a de facto standard. In practice, CSV files vary widely in their use of quoting, escaping, line endings, character encoding, and header conventions.

### 4.2 Characteristics

| Property | CSV |
|----------|-----|
| Schema | None (no type information; all values are strings) |
| Compression | None built-in; external compression (gzip, bz2) possible |
| Human-readable | Yes |
| Splittable | Yes (uncompressed) / No (if gzipped) |
| Nested data | Not supported |
| Type safety | None |
| Encoding efficiency | Very low (text representation of numbers, repeated delimiters) |

### 4.3 Problems with CSV in Big Data

While CSV is ubiquitous and convenient, it has several significant problems when used at scale:

**1. No type information.** Every value is a string. The downstream system must infer or be told that `"49.99"` is a floating-point number and `"2025-01-15T10:30:00Z"` is a timestamp. This inference is error-prone and CPU-intensive.

**2. No schema enforcement.** There is nothing preventing a row from having the wrong number of fields, or a field from containing a value of the wrong type. Corrupt or malformed rows are discovered only at read time.

**3. Delimiter conflicts.** If a field value contains the delimiter character (e.g., a comma inside a product description), it must be quoted. But quoting rules are not universally consistent, leading to parse errors.

```csv
product_id,description,price
P001,"Widget, Deluxe Model",29.99
P002,"He said ""hello""",19.99
```

**4. Very poor storage efficiency.** Consider storing the integer `1000000`. In a binary format like Avro, this takes 4 bytes. In CSV, it takes 7 bytes (the characters `1`, `0`, `0`, `0`, `0`, `0`, `0`). For floating-point numbers, the overhead is even worse.

**5. No compression.** CSV has no built-in compression. Files can be gzipped externally, but a gzipped CSV file is **not splittable** -- a distributed processing framework cannot assign different parts of the file to different workers without decompressing the entire file first. (Block-compressed formats like bzip2 are splittable but very slow.)

**6. No nested data.** CSV is strictly flat/tabular. Representing hierarchical or nested data requires denormalization or ugly workarounds like JSON-in-a-cell.

### 4.4 When CSV Is Still Used

Despite its limitations, CSV remains relevant in several scenarios:

- **Data exchange with non-technical stakeholders.** CSV files can be opened in Excel, Google Sheets, or any text editor.
- **Legacy system integration.** Many older systems export data only in CSV format.
- **Simple one-off exports.** When you need to quickly dump a query result for manual inspection.
- **Initial data ingestion.** Source data from vendors, partners, or government agencies is often provided as CSV. The ETL pipeline's first step is typically to convert this to a more efficient format.
- **Debugging.** Reading a few rows of a CSV file requires no special tooling.

In a well-designed big data pipeline, CSV is typically encountered only at the **edges** -- as an input format from external sources or as an output format for human consumption. Data within the pipeline should be stored in a binary format like Parquet, ORC, or Avro.

### 4.5 Working with CSV in Spark

Even when CSV is unavoidable, Spark provides options to mitigate some of its weaknesses:

```python
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .option("dateFormat", "yyyy-MM-dd") \
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .csv("s3://bucket/raw/data.csv")

# Immediately save in a better format for downstream use:
df.write \
    .mode("overwrite") \
    .parquet("s3://bucket/processed/data.parquet")
```

Note the `inferSchema` option -- Spark makes two passes over the data (one to infer types, one to parse) which doubles the read time. In production, it is better to define the schema explicitly.

---

## 5. JSON / JSONL (JSON Lines)

### 5.1 Overview

JSON (JavaScript Object Notation) is a text-based, self-describing data format that has become the lingua franca of web APIs and configuration files. In the big data context, JSON appears in two main forms:

- **Standard JSON**: A single JSON array or object per file. Typically used for configuration, API responses, or small datasets.
- **JSONL (JSON Lines)**: One complete JSON object per line, separated by newlines. This is the preferred form for big data because it is **line-splittable** -- each line can be parsed independently.

```jsonl
{"event_id": "evt_001", "user_id": 12345, "event_type": "PURCHASE", "amount": 49.99}
{"event_id": "evt_002", "user_id": 12346, "event_type": "CLICK", "amount": null}
{"event_id": "evt_003", "user_id": 12345, "event_type": "VIEW", "tags": ["organic", "mobile"]}
```

### 5.2 Characteristics

| Property | JSON/JSONL |
|----------|-----------|
| Schema | None (schema-on-read; self-describing) |
| Compression | None built-in; external compression possible |
| Human-readable | Yes |
| Splittable | JSONL: Yes / Standard JSON: No |
| Nested data | Excellent (native support) |
| Type safety | Partial (numbers, strings, booleans, null; but no integer vs. float distinction, no date/timestamp type) |
| Encoding efficiency | Very low (field names repeated in every record, text encoding of values) |

### 5.3 Verbosity Problem

JSON's biggest weakness for big data is its extreme verbosity. Consider a table with 10 columns and 1 million rows:

- In **Parquet**, column names are stored once in the metadata. The data is binary-encoded and compressed. Total size might be 50 MB.
- In **JSON**, every single record repeats all 10 field names. The data is text-encoded. Total size might be 500 MB or more -- a **10x** overhead.

This verbosity hurts in three ways:

1. **Storage cost**: More bytes on disk or in object storage means higher cost.
2. **I/O bandwidth**: More bytes to read from disk or network.
3. **Parse time**: Parsing JSON is CPU-intensive because the parser must handle arbitrary nesting, Unicode escaping, and number parsing for every record.

### 5.4 Schema-on-Read

JSON is a **schema-on-read** format, meaning there is no predefined schema -- the structure is inferred when the data is read. This provides maximum flexibility but creates several challenges:

- **Inconsistent schemas**: Different records in the same dataset might have different fields or different types for the same field. One record might have `"age": 25` (integer) while another has `"age": "twenty-five"` (string).
- **Schema inference cost**: Tools like Spark must scan a sample of the data (or the entire dataset) to infer the schema, which is slow and potentially inaccurate.
- **No evolution guarantees**: There is no mechanism to enforce backward or forward compatibility when the data structure changes.

### 5.5 When JSON/JSONL Is Still Used

- **API responses**: Most web APIs return JSON, so ingestion pipelines often start with JSON data.
- **Logging**: Application logs are frequently emitted as structured JSON (e.g., from frameworks like `structlog` or `log4j2` with JSON layout).
- **Configuration files**: Spark configurations, Airflow DAGs, and other tools use JSON or YAML.
- **Semi-structured data exploration**: When the schema is unknown or evolving rapidly, JSON provides maximum flexibility for initial exploration.
- **Small datasets**: For datasets under a few hundred megabytes, the overhead of JSON is often acceptable and the human-readability is valuable.

### 5.6 Optimizing JSON Ingestion

In production ETL pipelines, JSON should be treated as a **landing format** and converted to a columnar format as early as possible:

```python
# Read JSON Lines from a streaming source
raw_df = spark.read \
    .option("multiLine", "false") \
    .schema(explicit_schema) \
    .json("s3://bucket/raw/events/*.jsonl")

# Validate, transform, and save as Parquet
raw_df \
    .filter(col("event_id").isNotNull()) \
    .withColumn("event_date", to_date("timestamp")) \
    .write \
    .partitionBy("event_date") \
    .parquet("s3://bucket/curated/events/")
```

Key best practices:

- **Always provide an explicit schema** rather than relying on `inferSchema`. This avoids the expensive inference pass and ensures type consistency.
- **Use JSONL** (one record per line) rather than standard JSON for large datasets. JSONL is splittable and streamable.
- **Convert to a columnar format** (Parquet or ORC) as soon as possible in the pipeline.

---

## 6. Apache Arrow (In-Memory Columnar Format)

### 6.1 What Arrow Is (and Is Not)

Apache Arrow is **not a file format** in the traditional sense. It is a **language-independent specification for in-memory columnar data representation**. Think of it as a standardized way for different tools and languages to share columnar data in memory without serialization or deserialization overhead.

Arrow was created in 2016 by Wes McKinney (creator of pandas) and several other leaders in the data processing community. The motivation was simple: every data processing library (pandas, R data frames, Spark, Drill, Impala, etc.) had its own internal memory format. Moving data between any two tools required serialization (converting from format A to bytes) and deserialization (converting from bytes to format B), which was slow and wasteful.

Arrow eliminates this by defining a single, standardized in-memory layout that all tools can agree on.

### 6.2 Key Design Principles

**Zero-copy reads**: When data is already in Arrow format in memory, another process or library can read it directly without copying or converting. This is transformative for inter-process communication (IPC) -- for example, a Python process can share data with a Java process without any serialization overhead.

**Language-agnostic**: Arrow has implementations in C++, Java, Python (PyArrow), Rust, Go, JavaScript, Julia, R, and others. All implementations use the same memory layout, so data can be shared across language boundaries.

**Cache-friendly columnar layout**: Arrow organizes data in a cache-efficient columnar format with fixed-width values stored in contiguous arrays and variable-width values (strings) stored in offset/value buffer pairs. This layout is optimized for SIMD (Single Instruction, Multiple Data) vectorized processing on modern CPUs.

**Null handling via validity bitmaps**: Rather than using sentinel values or optional wrappers, Arrow uses a compact **validity bitmap** where each bit indicates whether the corresponding value is null. This is both space-efficient and branch-prediction-friendly.

### 6.3 Memory Layout

An Arrow array for an `int32` column with values `[1, null, 3, 4, null]` looks like this in memory:

```
Validity bitmap:  [1, 0, 1, 1, 0]   (bit-packed: 0b00011010 = 0x1A)
                   ^  ^  ^  ^  ^
                   |  |  |  |  +-- index 4: null
                   |  |  |  +---- index 3: valid
                   |  |  +------- index 2: valid
                   |  +---------- index 1: null
                   +------------- index 0: valid

Value buffer:     [1, ?, 3, 4, ?]   (4 bytes each, total 20 bytes)
                       ^        ^
                       |        +-- undefined (null slot, can be any value)
                       +----------- undefined (null slot)
```

For variable-length types like strings, Arrow uses an offset buffer:

```
Column values: ["hello", "big", null, "data"]

Offsets buffer:  [0, 5, 8, 8, 12]   (int32, 5 entries for 4 values)
Data buffer:     h e l l o b i g d a t a
                 |         |     |   |
                 0         5     8   12

Validity bitmap: [1, 1, 0, 1]
```

The third value is null, so its offset span (8 to 8) is zero-length, and the validity bitmap marks it as invalid.

### 6.4 IPC Format and Feather (v2)

While Arrow is primarily an in-memory specification, it also defines an **IPC (Inter-Process Communication) format** for serializing Arrow data to bytes (for writing to files, sending over the network, or sharing between processes via shared memory).

The Arrow IPC format comes in two variants:

- **Streaming format**: A sequence of record batches, suitable for streaming or piping between processes. There is no random access -- the reader must process batches sequentially.
- **File format (also known as Feather v2 or Arrow IPC file)**: Adds a file footer with metadata that enables random access to individual record batches. Files typically use the `.arrow` or `.feather` extension.

The Feather format was originally created by Wes McKinney and Hadley Wickham (of R fame) as a fast, language-agnostic file format for data frames. Feather v2 is simply the Arrow IPC file format and has replaced the original Feather v1.

```python
import pyarrow as pa
import pyarrow.feather as feather

# Create an Arrow table
table = pa.table({
    "user_id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "score": [95.5, 87.3, None, 91.0, 78.2]
})

# Write to Feather v2 (Arrow IPC file)
feather.write_feather(table, "users.feather", compression="zstd")

# Read back -- extremely fast, near zero-copy
table2 = feather.read_feather("users.feather")
```

Feather/Arrow IPC files are **not** designed for long-term storage or as a replacement for Parquet. They are optimized for **fast temporary storage and inter-process data exchange**. The key differences from Parquet are:

| Aspect | Arrow IPC / Feather | Parquet |
|--------|-------------------|---------|
| Primary purpose | Fast IPC, caching, temporary storage | Long-term analytical storage |
| Compression | Optional (LZ4, ZSTD) | Always recommended |
| Compression ratio | Moderate | High (encoding + compression) |
| Read speed | Extremely fast (near zero-copy) | Fast (requires decoding) |
| Write speed | Very fast | Moderate (encoding overhead) |
| Metadata/indexes | Minimal | Rich (statistics, page indexes) |
| Predicate pushdown | No | Yes |
| Ecosystem support | Growing | Universal |

### 6.5 Role in Modern Data Processing

Arrow has become the **backbone of modern data processing**, even when users do not interact with it directly:

- **pandas 2.0+**: Starting with pandas 2.0 (April 2023), Arrow is available as a backend for DataFrames via `dtype_backend="pyarrow"`. This provides significantly better performance and memory efficiency compared to the traditional NumPy backend, especially for string-heavy data and nullable integer types.

```python
import pandas as pd

# Use Arrow backend for better performance
df = pd.read_parquet("data.parquet", dtype_backend="pyarrow")
```

- **Polars**: The Polars DataFrame library (written in Rust) uses Arrow as its native in-memory format. This is one reason Polars is dramatically faster than traditional pandas for many operations.

- **DuckDB**: DuckDB uses Arrow for its internal data representation and provides zero-copy integration with Python via PyArrow. A query result from DuckDB can be consumed as an Arrow table without any data copying.

```python
import duckdb

# DuckDB query result as Arrow table -- zero copy
arrow_table = duckdb.sql("""
    SELECT user_id, COUNT(*) as event_count
    FROM read_parquet('events/*.parquet')
    GROUP BY user_id
""").arrow()
```

- **Spark**: Apache Spark uses Arrow for efficient data transfer between JVM-based Spark and Python (PySpark). When Arrow is enabled, pandas UDFs exchange data via Arrow rather than pickle serialization, providing 10-100x speedups.

- **Flight RPC**: Arrow Flight is a high-performance RPC framework built on Arrow and gRPC that enables network transfer of Arrow data at speeds close to the theoretical maximum of the network hardware.

### 6.6 How Arrow Relates to Parquet

Arrow and Parquet are **complementary**, not competing:

- **Parquet** is the storage format (data at rest). It is optimized for disk: maximum compression, minimal storage footprint, rich metadata for query optimization.
- **Arrow** is the processing format (data in motion / in memory). It is optimized for CPU: cache-friendly layout, vectorized operations, zero-copy sharing.

The typical data flow is:

```
Disk (Parquet) --> Read + Decode --> Memory (Arrow) --> Process --> Encode + Write --> Disk (Parquet)
```

The `pyarrow` library provides highly optimized routines for converting between Parquet and Arrow, making this round-trip very efficient.

---

## 7. Compression Algorithms Overview

Compression plays a critical role in big data storage and processing. The choice of compression algorithm affects storage cost, I/O bandwidth, and CPU utilization. Here is an overview of the most commonly used algorithms in the big data ecosystem.

### 7.1 Snappy

- **Developed by**: Google (originally called "Zippy")
- **Philosophy**: Speed over compression ratio.
- **Typical ratio**: 2-4x
- **Speed**: Very fast compression and decompression. Designed to operate at approximately 250 MB/s compression and 500 MB/s decompression per CPU core.
- **Splittable**: Snappy itself is not splittable, but file formats (Parquet, ORC, Avro) apply Snappy at the block level, making the overall file splittable.
- **Use case**: Default choice for many big data systems (Parquet, HBase). Best when CPU is the bottleneck or latency is critical.

### 7.2 GZIP / ZLIB (Deflate)

- **Standard**: RFC 1952 (GZIP) / RFC 1950 (ZLIB), both based on the DEFLATE algorithm (RFC 1951).
- **Philosophy**: Good compression ratio with reasonable speed.
- **Typical ratio**: 5-8x
- **Speed**: Moderate compression speed (~20-50 MB/s), moderate decompression speed (~200-300 MB/s). Compression is significantly slower than decompression.
- **Splittable**: GZIP files are **not** splittable. However, when GZIP is used within a file format (like Avro blocks or Parquet pages), the file remains splittable at the format level.
- **Use case**: Good default when storage cost is a concern and data is written once and read many times (write overhead is amortized).

### 7.3 LZ4

- **Developed by**: Yann Collet
- **Philosophy**: Maximum speed with acceptable compression.
- **Typical ratio**: 2-3x
- **Speed**: The fastest general-purpose compression algorithm. Decompression speeds exceed 1 GB/s per core on modern hardware.
- **Splittable**: LZ4 frames are not splittable, but block-level application within file formats preserves splittability.
- **Use case**: Real-time processing, in-memory caching, scenarios where CPU cost of decompression must be minimized. Used by Arrow IPC as the default compression.

### 7.4 Zstandard (ZSTD)

- **Developed by**: Yann Collet (also the creator of LZ4) at Facebook
- **Philosophy**: The best of both worlds -- near-GZIP ratios at near-Snappy speeds.
- **Typical ratio**: 4-7x
- **Speed**: Compression speed is configurable via compression levels (1-22). At level 1, it is nearly as fast as Snappy with better ratios. At level 19+, it approaches LZMA ratios but much faster.
- **Splittable**: Not inherently, but block-level application preserves splittability.
- **Use case**: Increasingly the **recommended default** for most big data workloads. Supported by Parquet, ORC, Avro, and Arrow.

### 7.5 BZip2

- **Philosophy**: Maximum compression ratio.
- **Typical ratio**: 6-10x
- **Speed**: Very slow, both compression and decompression (~10-30 MB/s).
- **Splittable**: Yes -- BZip2 is natively block-splittable, which is unique among compression formats. This made it historically important in Hadoop MapReduce.
- **Use case**: Archival storage where storage cost dominates and data is rarely read. Increasingly replaced by ZSTD at high compression levels.

### 7.6 Compression Comparison Table

| Algorithm | Compression Ratio | Compression Speed | Decompression Speed | Splittable | Best For |
|-----------|:-:|:-:|:-:|:-:|------|
| **Snappy** | 2-4x | Very Fast | Very Fast | No* | Low-latency, CPU-bound workloads |
| **LZ4** | 2-3x | Fastest | Fastest | No* | Real-time, in-memory, streaming |
| **GZIP/ZLIB** | 5-8x | Slow | Moderate | No* | Write-once/read-many, cost-sensitive storage |
| **ZSTD** | 4-7x | Fast (tunable) | Fast | No* | General purpose, modern default |
| **BZip2** | 6-10x | Very Slow | Very Slow | Yes | Archival, Hadoop legacy |

> \* Not splittable as standalone compressed files. However, when applied at the block level within Avro, Parquet, or ORC, the parent file format remains splittable at its own block/row-group/stripe boundaries.

### 7.7 Practical Advice

1. **Default to ZSTD** for new pipelines. It offers the best balance across all dimensions.
2. **Use Snappy or LZ4** when latency matters more than storage efficiency (real-time analytics, interactive queries).
3. **Use GZIP** when you need maximum compatibility (some older systems only support GZIP) or when storage cost is the primary concern and write speed is unimportant.
4. **Avoid BZip2** in new systems -- ZSTD at high levels achieves similar ratios with much better speed.
5. **Always compress data in big data pipelines.** The CPU cost of decompression is almost always less than the I/O cost of reading uncompressed data.

---

## 8. Comprehensive Format Comparison

### 8.1 Feature Matrix

| Feature | Parquet | Avro | ORC | CSV | JSON/JSONL | Arrow/Feather |
|---------|:-------:|:----:|:---:|:---:|:----------:|:-------------:|
| **Storage Model** | Columnar (hybrid) | Row | Columnar (hybrid) | Row (text) | Row (text) | Columnar (memory) |
| **Schema** | Embedded (footer) | Embedded (header) | Embedded (footer) | None | None (self-describing) | Embedded |
| **Schema Evolution** | Good | Excellent | Good | N/A | N/A | Minimal |
| **Compression** | Built-in (page-level) | Built-in (block-level) | Built-in (stream-level) | External only | External only | Optional (LZ4/ZSTD) |
| **Compression Ratio** | High | Moderate | High | Very Low | Very Low | Moderate |
| **Splittable** | Yes | Yes | Yes | Yes (uncompressed) | JSONL: Yes | Yes |
| **Nested Data** | Yes (Dremel) | Yes (union/array/map) | Yes (struct/list/map) | No | Yes (native) | Yes |
| **Human-Readable** | No | No | No | Yes | Yes | No |
| **Type Safety** | Strong | Strong | Strong | None | Weak | Strong |
| **Write Speed** | Moderate | Fast | Moderate | Fast | Fast | Very Fast |
| **Read Speed (full scan)** | Very Fast | Fast | Very Fast | Slow | Very Slow | Extremely Fast |
| **Column Pruning** | Yes | No | Yes | No | No | Yes |
| **Predicate Pushdown** | Yes | No | Yes (with bloom filters) | No | No | No |
| **ACID Support** | Via Delta/Iceberg | No | Native (Hive) | No | No | No |

### 8.2 Best Use Case Summary

| Format | Best Use Case |
|--------|--------------|
| **Parquet** | Analytical queries, data warehousing, long-term columnar storage, multi-engine environments |
| **Avro** | Data serialization, Kafka streaming, write-heavy ETL ingestion, schema evolution requirements |
| **ORC** | Hive-centric analytics, ACID transactions in Hive, HDFS-based data lakes |
| **CSV** | Data exchange with non-technical users, legacy integrations, simple exports, debugging |
| **JSON/JSONL** | API ingestion, application logging, semi-structured data exploration, configuration |
| **Arrow/Feather** | In-memory analytics, inter-process data exchange, caching, pandas/Polars/DuckDB processing |

### 8.3 Ecosystem Support Matrix

| Engine/Tool | Parquet | Avro | ORC | CSV | JSON | Arrow |
|-------------|:-------:|:----:|:---:|:---:|:----:|:-----:|
| Apache Spark | Native | Native | Native | Native | Native | Native (PySpark) |
| Apache Hive | Good | Good | Native | Good | Good | Limited |
| Trino (Presto) | Native | Good | Native | Good | Good | Growing |
| Apache Flink | Good | Native | Good | Good | Native | Growing |
| Apache Kafka | Limited | Native | Limited | Limited | Native | Limited |
| DuckDB | Native | Limited | Limited | Native | Native | Native |
| Polars | Native | Limited | Limited | Native | Native | Native |
| pandas | Native (PyArrow) | Via lib | Limited | Native | Native | Native |
| AWS Athena | Native | Native | Native | Native | Native | N/A |
| Google BigQuery | Native | Native | Native | Native | Native | N/A |

### 8.4 Decision Flowchart (Textual)

When choosing a file format for a big data pipeline, consider the following decision process:

1. **Is the data at the edge of the system (ingestion/export)?**
   - Ingesting from APIs? --> Start with **JSON/JSONL**, convert to columnar ASAP.
   - Receiving from partners/vendors? --> Expect **CSV**, convert to columnar ASAP.
   - Writing to Kafka? --> Use **Avro** with Schema Registry.

2. **Is the data in transit (streaming, message passing, IPC)?**
   - Kafka messages? --> **Avro**.
   - Inter-process data sharing (Python/Rust/Java)? --> **Arrow**.
   - Temporary caching between pipeline stages? --> **Arrow/Feather**.

3. **Is the data at rest (data lake, data warehouse)?**
   - Multi-engine environment (Spark, Trino, DuckDB, etc.)? --> **Parquet**.
   - Hive-only with ACID requirements? --> **ORC**.
   - Need schema evolution guarantees on the raw data layer? --> **Avro** for raw, **Parquet** for curated.

4. **Is the primary workload analytical (SELECT few columns, aggregate, filter)?**
   - Yes --> **Parquet** or **ORC** (columnar).
   - No, full-record processing --> **Avro** (row-based).

---

## 9. Key Takeaways

1. **There is no universally "best" file format.** The right choice depends on the specific workload, ecosystem, and requirements. A well-designed data pipeline often uses multiple formats at different stages.

2. **The medallion architecture pattern** (Bronze/Silver/Gold) commonly maps to formats:
   - **Bronze (raw)**: Avro or JSON (preserve original data, prioritize write speed and schema flexibility).
   - **Silver (cleaned)**: Parquet (convert to columnar for efficient processing).
   - **Gold (curated)**: Parquet (optimized with partitioning, sorting, and compaction).

3. **Row-oriented formats (Avro) are write-optimized**; columnar formats (Parquet, ORC) are read-optimized. Most analytical workloads are read-heavy, which is why columnar formats dominate in data warehousing.

4. **Text formats (CSV, JSON) should be treated as interchange formats**, not storage formats. Convert to binary formats as early as possible in the pipeline.

5. **Arrow is the future of in-memory data processing.** Understanding Arrow is important not because you will store data in Arrow format, but because Arrow is the internal engine powering pandas 2.0+, Polars, DuckDB, and many other modern tools.

6. **Always compress your data.** The CPU cost of decompression is almost always cheaper than the I/O cost of reading uncompressed data. Default to ZSTD for new systems.

7. **ORC and Parquet are functionally similar** for most workloads. Choose based on ecosystem fit, not theoretical performance differences. If you are not locked into Hive, Parquet's broader ecosystem support usually wins.

8. **Schema management matters.** Formats with strong schema support (Avro, Parquet, ORC) catch data quality issues early. Formats without schemas (CSV, JSON) push the problem to consumers, increasing fragility.

---

## 10. References and Further Reading

- Apache Avro Specification: https://avro.apache.org/docs/current/specification/
- Apache ORC Specification: https://orc.apache.org/specification/
- Apache Arrow Specification: https://arrow.apache.org/docs/format/Columnar.html
- Apache Parquet Format: https://parquet.apache.org/documentation/latest/
- Confluent Schema Registry: https://docs.confluent.io/platform/current/schema-registry/
- Zstandard (RFC 8878): https://facebook.github.io/zstd/
- "The Design and Implementation of Modern Column-Oriented Database Systems" (Abadi et al.)
- "Dremel: Interactive Analysis of Web-Scale Datasets" (Melnik et al., 2010)
- Wes McKinney, "Apache Arrow and the Future of Data Frames" (blog post)
- Databricks, "Delta Lake: High-Performance ACID Table Storage" (whitepaper)
