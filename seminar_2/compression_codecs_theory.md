# Compression Codecs and Algorithms for Big Data Processing

> **Seminar 2 -- Deep-Dive Supplementary Material**
> Duration: ~30-40 minutes of lecture (or self-study reference)
> Prerequisites: Basic understanding of binary representation, familiarity with file formats from `columnar_and_row_formats_theory.md`
> Companion material: The high-level codec overview in Section 7 of `columnar_and_row_formats_theory.md`

---

## Table of Contents

1. [Introduction to Compression](#1-introduction-to-compression)
2. [LZ77 / LZ78 (Foundation)](#2-lz77--lz78-foundation)
3. [Deflate (GZIP / ZLIB)](#3-deflate-gzip--zlib)
4. [Snappy](#4-snappy)
5. [LZ4](#5-lz4)
6. [Zstandard (ZSTD)](#6-zstandard-zstd)
7. [BZip2](#7-bzip2)
8. [Run-Length Encoding (RLE)](#8-run-length-encoding-rle)
9. [Dictionary Encoding](#9-dictionary-encoding)
10. [Practical Comparison](#10-practical-comparison)
11. [Codec Support Matrix](#11-codec-support-matrix)

---

## 1. Introduction to Compression

### 1.1 Why Compression Matters in Big Data

In big data systems, the bottleneck is almost never the CPU -- it is **I/O**. Reading data from disk, transferring it over the network, and writing it back are orders of magnitude slower than the arithmetic the CPU performs once the data arrives. Consider the numbers:

| Operation                        | Throughput          |
|----------------------------------|---------------------|
| CPU L1 cache read                | ~1 TB/s             |
| CPU L3 cache read                | ~200 GB/s           |
| DDR4 RAM sequential read         | ~40 GB/s            |
| NVMe SSD sequential read        | ~3-7 GB/s           |
| SATA SSD sequential read         | ~500 MB/s           |
| HDD sequential read              | ~100-200 MB/s       |
| 10 Gbit Ethernet (datacenter)    | ~1.2 GB/s           |
| 1 Gbit Ethernet                  | ~125 MB/s           |
| Cloud object store (S3, GCS)     | ~200-500 MB/s       |

When the CPU can decompress data at 800 MB/s but the disk only delivers 200 MB/s, compressing a 1 GB file to 250 MB means the system reads 250 MB from disk (1.25 seconds) plus spends ~0.3 seconds decompressing, for a total of ~1.55 seconds -- versus 5 seconds to read the uncompressed file. **Compression makes reading faster, not slower.**

Three key motivations:

1. **Reduced I/O wait** -- less data to read from disk or receive over the network.
2. **Lower storage cost** -- cloud storage is billed per GB; 4x compression means 4x cost reduction.
3. **Higher network throughput** -- data shuffles in Spark, MapReduce, or distributed joins transfer less data between nodes.

### 1.2 Lossless vs Lossy Compression

- **Lossy compression** discards information that is considered "unimportant." Used for images (JPEG), audio (MP3), and video (H.264). The decompressed output is an approximation of the original.
- **Lossless compression** guarantees that the decompressed output is bit-for-bit identical to the original input. This is the only kind of compression used in data processing, because losing even a single bit in a financial transaction, sensor reading, or log entry would corrupt the dataset.

Every algorithm discussed in this document is **lossless**.

### 1.3 Two Fundamental Approaches

Almost all lossless compression algorithms fall into two broad families, which are often combined:

**Dictionary-based compression (LZ family)**

The core idea: if a pattern of bytes has appeared before, do not write it again -- instead, write a short reference ("go back 47 bytes and copy 12 bytes from there"). The algorithm builds a "dictionary" of previously seen patterns, either implicitly (via a sliding window over recent data, as in LZ77) or explicitly (as a growing table of entries, as in LZ78).

```
Original:    THE CAT SAT ON THE MAT
                                ^^^
                                "THE" appeared before at position 0!

Compressed:  THE CAT SAT ON [ref: offset=15, length=4] MAT
```

**Entropy-based compression (Huffman, Arithmetic, FSE)**

The core idea: assign shorter bit codes to symbols that appear frequently and longer codes to symbols that appear rarely. If the letter "E" appears 100 times but "Z" appears once, give "E" a 2-bit code and "Z" a 12-bit code. This exploits the statistical non-uniformity of the data.

```
Character frequencies:  A=45%  B=30%  C=15%  D=10%
Fixed-length codes:     A=00   B=01   C=10   D=11     (2 bits each)
Huffman codes:          A=0    B=10   C=110  D=111    (1-3 bits each)

100 characters at fixed 2 bits = 200 bits
100 characters at Huffman:  45*1 + 30*2 + 15*3 + 10*3 = 45+60+45+30 = 180 bits  (10% saved)
```

**Most modern codecs combine both approaches**: first, an LZ-family pass finds repeated patterns and replaces them with short references; then, an entropy coder compresses the resulting stream of literals, offsets, and lengths using variable-length codes.

### 1.4 Compression Ratio vs Speed Tradeoff

There is an inherent tension between how small you can make the output and how fast you can produce it:

```
                    Compression Ratio (higher = smaller output)
                    ^
                    |
        BZip2  *   |
                   |
        ZSTD-19 *  |
                   |
        GZIP-9  *  |
                   |
        ZSTD-3   * |
        GZIP-1    *|
                   |
        Snappy      *
        LZ4          *
                    |
                    +-----------------------------------------> Speed (higher = faster)
```

The tradeoff is controlled by:

- **Search depth**: How hard the compressor looks for the best match. A deeper search finds longer matches (better ratio) but takes more CPU time.
- **Entropy coding stage**: Adding Huffman or FSE coding after LZ matching improves the ratio but costs additional CPU. Snappy and LZ4 skip this stage entirely.
- **Window size**: A larger window lets the algorithm reference patterns from further back, finding more matches, but requires more memory.

---

## 2. LZ77 / LZ78 (Foundation)

LZ77 and LZ78 were published by Abraham Lempel and Jacob Ziv in 1977 and 1978, respectively. They are the foundation for nearly every modern compression algorithm used in big data: GZIP, Snappy, LZ4, and ZSTD all descend from LZ77.

### 2.1 LZ77: The Sliding Window Algorithm

**Key insight**: If a sequence of bytes has appeared recently, replace it with a compact back-reference: "go back `offset` positions and copy `length` bytes."

The algorithm maintains two conceptual regions over the input:

```
Already processed (search buffer)          Not yet processed (look-ahead buffer)
<--------- sliding window ---------->      <--- look-ahead --->
                                     |
                                     current position
```

```
+-----------------------------------------------------------+------------------+
|   S E A R C H   B U F F E R   (already encoded)          | L O O K - A H E A D |
|   (the "dictionary" of recent bytes)                      | (bytes to encode)   |
+-----------------------------------------------------------+------------------+
|<-- e.g., 32 KB in classic GZIP ------>|                   |<-- e.g., 258 B -->|
                                        ^
                                        current position
```

At each step, the algorithm:

1. Looks at the bytes in the look-ahead buffer.
2. Searches the search buffer for the **longest match** of those bytes.
3. If a match is found, emits a **(offset, length)** pair (a back-reference).
4. If no match is found, emits the next byte as a **literal**.
5. Advances the window forward.

#### Worked Example: Encoding `AABABCABCD`

Let us trace through encoding the string `AABABCABCD` character by character.

We use a simplified notation where each output is either:
- A **literal** `L(x)` -- emit the character `x` directly
- A **match** `M(offset, length)` -- go back `offset` positions, copy `length` characters

```
Input: A A B A B C A B C D
Pos:   0 1 2 3 4 5 6 7 8 9
```

**Step 1** -- Position 0: `A`
- Search buffer is empty (nothing processed yet).
- No match possible. Emit literal.
- Output: `L(A)`

**Step 2** -- Position 1: `A`
- Search buffer: `A`
- Look-ahead starts with `A`. Found match: offset=1, length=1 (`A` at position 0).
- Could we extend? Look-ahead is `A A B A B C A B C D`. The character at position 0+1=1 is `A`, and the character at position 1+1=2 is `B`. These do not match (`A` != `B`).
- Best match: offset=1, length=1. But a literal is the same size, so we emit a literal.
- Output: `L(A)`

**Step 3** -- Position 2: `B`
- Search buffer: `A A`
- No `B` found. Emit literal.
- Output: `L(B)`

**Step 4** -- Position 3: `A B`
- Search buffer: `A A B`
- Look-ahead starts with `A B C A B C D`.
- Search for `A`: found at positions 0 and 1.
- Try to extend: `AB` -- found at position 0 (`A` at 0, `B` at 1, matches `A` at 3, `B` at 4). Length=2.
- Try to extend further: position 0+2=2 is `B`, but position 3+2=5 is `C`. No match. Best: offset=3, length=2.
- Output: `M(3, 2)` -- "go back 3, copy 2" produces `AB`

**Step 5** -- Position 5: `C`
- Search buffer: `A A B A B`
- No `C` found. Emit literal.
- Output: `L(C)`

**Step 6** -- Position 6: `A B C D`
- Search buffer: `A A B A B C`
- Look-ahead starts with `A B C D`.
- Search for `ABC`: found starting at position 3 (`A=3, B=4, C=5`). Offset=3, length=3.
- Try to extend: position 3+3=6 is `A`, position 6+3=9 is `D`. `A` != `D`. Best: offset=3, length=3.
- Output: `M(3, 3)` -- "go back 3, copy 3" produces `ABC`

**Step 7** -- Position 9: `D`
- No `D` in search buffer. Emit literal.
- Output: `L(D)`

**Complete encoded output**:

```
L(A)  L(A)  L(B)  M(3,2)  L(C)  M(3,3)  L(D)
```

**Verification** (decoding):

```
L(A)    -->  A
L(A)    -->  A A
L(B)    -->  A A B
M(3,2)  -->  A A B [go back 3 from pos 3: copy pos 0,1 = "AB"] --> A A B A B
L(C)    -->  A A B A B C
M(3,3)  -->  A A B A B C [go back 3 from pos 6: copy pos 3,4,5 = "ABC"] --> A A B A B C A B C
L(D)    -->  A A B A B C A B C D
```

Original: 10 characters. Encoded: 4 literals + 2 match references = typically fewer bytes, especially on longer, more repetitive data.

#### Sliding Window Diagram

Here is what the sliding window looks like at Step 6 of the example above:

```
                   Sliding window (search buffer)                 Look-ahead buffer
  +--+--+--+--+--+--+                                        +--+--+--+--+
  | A| A| B| A| B| C|                                        | A| B| C| D|
  +--+--+--+--+--+--+                                        +--+--+--+--+
    0  1  2  3  4  5                                            6  7  8  9

  Match found: positions 3,4,5 = "ABC" match look-ahead "ABC"
  Output: M(offset=3, length=3)

  After encoding, the window slides right by 3:

                         Sliding window                          Look-ahead
     +--+--+--+--+--+--+--+--+--+                            +--+
     | A| A| B| A| B| C| A| B| C|                            | D|
     +--+--+--+--+--+--+--+--+--+                            +--+
       0  1  2  3  4  5  6  7  8                                9
```

### 2.2 LZ78: Dictionary-Based Without Sliding Window

LZ78 takes a different approach. Instead of a sliding window over recent bytes, it builds an **explicit dictionary** as a table of entries:

1. Start with an empty dictionary.
2. Read input characters and try to find the longest string already in the dictionary.
3. Output the **(dictionary_index, next_character)** pair.
4. Add the string + next character as a new dictionary entry.

#### Worked Example: Encoding `AABABCABCD`

```
Input: A A B A B C A B C D
```

| Step | Current  | Longest match in dict | Output        | New dict entry         |
|------|----------|-----------------------|---------------|------------------------|
| 1    | A        | (none)                | (0, A)        | 1: A                   |
| 2    | A        | A (entry 1)           | --            | --                     |
|      | AB       | no "AB"               | (1, B)        | 2: AB                  |
| 3    | A        | A (entry 1)           | --            | --                     |
|      | AB       | AB (entry 2)          | --            | --                     |
|      | ABC      | no "ABC"              | (2, C)        | 3: ABC                 |
| 4    | A        | A (entry 1)           | --            | --                     |
|      | AB       | AB (entry 2)          | --            | --                     |
|      | ABC      | ABC (entry 3)         | --            | --                     |
|      | ABCD     | no "ABCD"             | (3, D)        | 4: ABCD                |

**Output**: `(0,A) (1,B) (2,C) (3,D)`

**Decoding**: Each tuple tells us: take the string from dictionary entry `index`, append `character`.

```
(0,A) --> "" + A = "A"              dict[1] = A
(1,B) --> "A" + B = "AB"            dict[2] = AB
(2,C) --> "AB" + C = "ABC"          dict[3] = ABC
(3,D) --> "ABC" + D = "ABCD"        dict[4] = ABCD

Concatenated: A + AB + ABC + ABCD = AABABCABCD
```

### 2.3 LZ77 vs LZ78 -- Key Differences

| Property            | LZ77                                  | LZ78                                  |
|---------------------|---------------------------------------|---------------------------------------|
| Dictionary          | Implicit (sliding window)             | Explicit (growing table)              |
| Memory usage        | Fixed (window size)                   | Growing (can become large)            |
| Reference format    | (offset, length)                      | (dict_index, next_char)               |
| Patent status       | Expired / freely usable               | Was patented (GIF/LZW controversy)    |
| Modern descendants  | GZIP, Snappy, LZ4, ZSTD              | LZW (GIF, old Unix compress)          |

**Why LZ77 won**: LZ77's fixed-size sliding window gives bounded memory usage -- critical for embedded and streaming systems. Nearly all modern codecs are LZ77-based.

---

## 3. Deflate (GZIP / ZLIB)

### 3.1 The Two-Stage Pipeline

Deflate is the algorithm behind GZIP (files), ZLIB (streaming), and the ZIP archive format. It was designed by Phil Katz in 1993 and is defined in RFC 1951. It combines two techniques in sequence:

```
Input data
    |
    v
+------------------+
|  Stage 1: LZ77   |   Find repeated patterns, emit literals + back-references
+------------------+
    |
    v
Stream of literals, lengths, offsets
    |
    v
+------------------+
|  Stage 2: Huffman |   Assign short bit-codes to common symbols, long codes to rare ones
+------------------+
    |
    v
Compressed bitstream
```

Stage 1 reduces redundancy by eliminating repeated byte sequences. Stage 2 reduces redundancy by exploiting the non-uniform distribution of the symbols that Stage 1 produces.

### 3.2 Huffman Coding -- Worked Example

Huffman coding is a **prefix-free, variable-length coding** scheme. "Prefix-free" means no code is a prefix of any other code, so the decoder never needs to look ahead to determine where one code ends and the next begins.

#### Building a Huffman Tree

Suppose after the LZ77 pass, we have a stream containing these symbols with these frequencies:

```
Symbol:    A    B    C    D    E
Frequency: 15   7    6    6    5
Total: 39 symbols
```

**Algorithm**:

1. Create a leaf node for each symbol with its frequency.
2. Repeat until only one node remains:
   a. Remove the two nodes with the lowest frequencies.
   b. Create a new internal node with these two as children, frequency = sum.
   c. Insert the new node back.

**Step-by-step construction**:

```
Initial queue (sorted by frequency):
  E(5)  C(6)  D(6)  B(7)  A(15)

Step 1: Merge E(5) + C(6) = [EC](11)
  D(6)  B(7)  [EC](11)  A(15)

Step 2: Merge D(6) + B(7) = [DB](13)
  [EC](11)  [DB](13)  A(15)

Step 3: Merge [EC](11) + [DB](13) = [ECDB](24)
  A(15)  [ECDB](24)

Step 4: Merge A(15) + [ECDB](24) = [AECDB](39)
  [AECDB](39)    <-- root
```

**The resulting Huffman tree**:

```
                   [39]
                  /    \
                 0      1
                /        \
             A(15)      [24]
                       /    \
                      0      1
                     /        \
                  [11]       [13]
                 /    \     /    \
                0      1   0      1
               /        \ /        \
            E(5)      C(6) D(6)    B(7)
```

**Reading off the codes** (left edge = 0, right edge = 1):

| Symbol | Frequency | Huffman Code | Bits |
|--------|-----------|-------------|------|
| A      | 15        | `0`         | 1    |
| E      | 5         | `100`       | 3    |
| C      | 6         | `101`       | 3    |
| D      | 6         | `110`       | 3    |
| B      | 7         | `111`       | 3    |

**Verification**: The most frequent symbol (A, 38.5% of occurrences) gets the shortest code (1 bit). The least frequent symbol (E, 12.8%) gets a 3-bit code.

**Space calculation**:

```
Fixed-length (3 bits each): 39 * 3 = 117 bits
Huffman:  15*1 + 7*3 + 6*3 + 6*3 + 5*3 = 15 + 21 + 18 + 18 + 15 = 87 bits

Savings: 117 - 87 = 30 bits (25.6% reduction)
```

**Key property -- prefix-free**: No code is a prefix of another. The decoder reads bits left to right and can unambiguously determine each symbol:

```
Bitstream:  0 1 1 1 0 1 0 1 0 ...
            ^
            0 = A
              ^ ^ ^
              1 1 1 = B
                      ^
                      0 = A
                        ^ ^ ^
                        1 0 1 = C
                                ^
                                0 = A

Decoded: A B A C A ...
```

### 3.3 How Deflate Combines LZ77 and Huffman

In Deflate, the LZ77 pass produces a sequence of:
- **Literal bytes** (0-255): characters that had no match.
- **Length codes** (3-258): how many bytes to copy from a previous match.
- **Distance codes** (1-32768): how far back the match is.

Deflate then applies Huffman coding to this mixed stream. It uses **two separate Huffman trees**:

1. **Literal/Length tree**: encodes literal bytes (0-255) and length values (257-285, representing lengths 3-258) using one shared alphabet.
2. **Distance tree**: encodes distance values (0-29, representing distances 1-32768) using a separate alphabet.

```
LZ77 output:  Lit(T) Lit(H) Lit(E) Lit( ) Lit(C) Lit(A) Lit(T) Match(dist=7, len=5) ...
                                                                        |          |
                                                    encoded in          v          v
                                                    distance tree    lit/len tree
```

### 3.4 GZIP vs ZLIB vs Raw Deflate

All three use the exact same Deflate compression algorithm. They differ only in their **wrapper** (headers, trailers, checksums):

```
+-------------------------------------------------------------------+
|                         Raw Deflate                                |
|  (RFC 1951 -- just the compressed data, no header/checksum)       |
+-------------------------------------------------------------------+

+--------+------------------------------------------+---------+
| ZLIB   |              Raw Deflate                  | ZLIB    |
| header |              data blocks                  | trailer |
| (2 B)  |                                          | Adler32 |
+--------+------------------------------------------+---------+
  (RFC 1950)

+--------+------------------------------------------+---------+
| GZIP   |              Raw Deflate                  | GZIP    |
| header |              data blocks                  | trailer |
| (10 B) |                                          | CRC-32  |
| + optional extras (filename, timestamp, etc.)     | + size  |
+--------+------------------------------------------+---------+
  (RFC 1952)
```

| Wrapper   | Header size | Checksum | Extra metadata       | Typical use               |
|-----------|-------------|----------|----------------------|---------------------------|
| Raw       | 0 bytes     | None     | None                 | Inside other formats      |
| ZLIB      | 2 bytes     | Adler-32 | None                 | PNG images, HTTP streams  |
| GZIP      | 10+ bytes   | CRC-32   | Filename, timestamp  | `.gz` files, HTTP content |

In big data: when Parquet or Avro say they use "GZIP" compression, they actually use **raw Deflate** or ZLIB internally at the block level, with their own checksums on top. The distinction rarely matters in practice.

### 3.5 Compression Levels (1-9)

GZIP/ZLIB expose a compression level parameter from 1 (fastest) to 9 (best compression):

| Level | What changes                                          | Effect                          |
|-------|-------------------------------------------------------|---------------------------------|
| 1     | Minimal LZ77 search, greedy matching                  | Fast, lower ratio               |
| 4-5   | Moderate search depth, lazy matching                  | Balanced                        |
| 6     | Default. Good search depth, lazy matching             | Good ratio, reasonable speed    |
| 9     | Maximum search depth, exhaustive matching             | Best ratio, slowest compression |

What actually changes at each level is the **match-finding strategy**:
- At level 1, the compressor uses a hash table with **greedy matching** -- it accepts the first match it finds.
- At level 6 (default), it uses **lazy matching** -- after finding a match, it checks whether starting one byte later yields a longer match.
- At level 9, it uses the deepest hash chain traversal, exploring many more candidates.

**Decompression speed is the same regardless of compression level**, because the decompressor simply follows the stored instructions without searching.

### 3.6 Use Case in Big Data

- **Strengths**: Excellent compression ratio (5-8x), universally supported, well-understood.
- **Weaknesses**: Compression is slow (~20-50 MB/s), single-threaded by design.
- **Best for**: Cold data, archival, write-once/read-rarely scenarios, compatibility with legacy systems.
- **Typical usage**: `Parquet + GZIP` for data lakes where storage cost matters more than query latency.

---

## 4. Snappy

### 4.1 Design Philosophy

Snappy was developed at Google (originally named "Zippy") and open-sourced in 2011. Its design goal is explicitly stated: **aim for very high speed with reasonable compression**, rather than maximum compression at reasonable speed. Google uses it pervasively in BigTable, MapReduce, and RPC systems where data is compressed and decompressed billions of times per day.

### 4.2 How Snappy Works

Snappy is a **simplified LZ77 without any entropy coding stage**. This is the primary reason it is so fast -- it skips the entire Huffman/arithmetic coding step that GZIP, ZSTD, and BZip2 perform.

The algorithm:

1. Divide input into blocks (up to 64 KB each).
2. For each block, scan forward through the data.
3. At each position, compute a hash of the next 4 bytes.
4. Look up the hash in a small hash table to find a potential match.
5. If the match is valid and long enough (>= 4 bytes), emit a **copy** (back-reference).
6. If no match or the match is too short, emit the bytes as **literals**.

There is **no second pass** -- the output is the final compressed data.

### 4.3 Block Format: Tags, Literals, and Copies

Snappy uses a simple tag-based format. Each element starts with a **tag byte** whose lower 2 bits identify the element type:

```
Tag byte:   xxxxxxTT
                  ||
                  ++--> Type: 00 = literal
                               01 = copy with 1-byte offset  (short copy)
                               10 = copy with 2-byte offset  (medium copy)
                               11 = copy with 4-byte offset  (long copy)
```

**Literal element**:

```
+----------+---+---+---+---+---+
| Tag byte | literal data bytes |
| xxxxxx00 | (1 to 64 bytes)   |
+----------+---+---+---+---+---+

Upper 6 bits of tag = literal length - 1 (for lengths 1-60)
For lengths > 60: upper 6 bits = 60/61/62/63 and 1-4 additional length bytes follow
```

**Copy element (type 01 -- short copy)**:

```
+----------+----------+
| Tag byte | Offset   |
| LLLOO 01 | OOOOOOOO |
+----------+----------+

LLL   = (length - 4):  encodes lengths 4-11
OOOOO + OOOOOOOO = 13-bit offset (up to 8191 bytes back)
```

### 4.4 Concrete Encoding Example

Let us encode the string: `"THE CAT SAT ON THE MAT"` (22 bytes)

```
Position: 0         1         2
          0123456789012345678901
Input:    THE CAT SAT ON THE MAT
```

**Processing**:

- Positions 0-14: `THE CAT SAT ON ` -- no previous match of 4+ bytes. Emit as a single literal run.

- Position 15: `THE ` -- hash of "THE " matches position 0. Offset = 15, length = 4 ("THE "). Emit the pending literal, then emit a copy.

- Position 19: `MAT` -- hash of "MAT?" -- no 4-byte match (we only have 3 bytes left). Emit as literal.

**Encoded output**:

```
Element 1: Literal, length=15
  Tag byte:  00001110 00  (14 in upper 6 bits = length 15, type 00)
  Data:      T H E   C A T   S A T   O N   (15 bytes)

Element 2: Copy, offset=15, length=4
  Tag byte:  000 01111 01  (length-4=0 in LLL, offset low bits in OO=11, type 01)
  Offset:    00001111      (remaining 8 bits of offset=15)

Element 3: Literal, length=3
  Tag byte:  00000010 00  (2 in upper 6 bits = length 3, type 00)
  Data:      M A T         (3 bytes)
```

**Result**: 15 bytes literal + 2 bytes copy + 3 bytes literal + 3 tag bytes = 23 bytes. On this tiny input, there is no compression gain (the original was 22 bytes). Snappy is designed for **large blocks of real-world data** where repetition is abundant. On a typical 64 KB block of log data, Snappy achieves 2-4x compression.

### 4.5 Why Snappy Is So Fast

1. **No entropy coding**: No Huffman tree construction, no bit-level output packing. Output is always byte-aligned.
2. **Simple hash table**: Uses a fixed-size hash table (16 KB) for match finding. No hash chains, no tree traversal.
3. **Greedy matching**: Accepts the first match found, never searches for a "better" match.
4. **Branch-free copy loops**: The copy operation is optimized for modern CPUs using wide `memcpy`-style operations.
5. **No block splitting heuristics**: Does not analyze data statistics to decide on encoding strategies.

### 4.6 Use Case in Big Data

- **Default codec** in Apache Parquet, Apache Avro, and Google's internal data systems.
- **Ideal for**: Interactive analytics (Spark SQL, Presto/Trino), real-time processing, shuffle data.
- **Typical performance**: ~250 MB/s compression, ~500 MB/s decompression per core.
- **Compression ratio**: 2-4x (modest, but the speed makes up for it on I/O-bound systems).

---

## 5. LZ4

### 5.1 Design Philosophy

LZ4 was created by **Yann Collet** in 2011 (the same engineer who later created ZSTD). Its primary design goal is **maximum decompression speed** -- even faster than Snappy for decompression. LZ4 can decompress data at over 1 GB/s per CPU core on modern hardware, often reaching memory bandwidth limits.

### 5.2 How LZ4 Works

Like Snappy, LZ4 is an LZ77 variant without entropy coding. The input is processed as a stream of **sequences**, where each sequence consists of:

1. **Literals**: bytes copied verbatim from the input.
2. **Match**: a back-reference (offset, length) to previously seen data.

The key difference from Snappy is the **token byte** encoding format, which is both simpler and more efficient for the decoder.

### 5.3 The Token Byte Structure

Each sequence starts with a **token byte** that packs two pieces of information into its two nibbles (4-bit halves):

```
Token byte:   LLLL MMMM
              ^^^^-^^^^
              |       |
              |       +-- Low nibble: match length (0-15, add 4 for actual length)
              |
              +---------- High nibble: literal length (0-15)

If either nibble is 15, additional bytes follow to encode longer values.
```

**A complete LZ4 sequence**:

```
+-------+-------------------+--------+--------+
| Token | [Extra lit length] | Literal | Offset | [Extra match length]
| 1 B   | 0+ bytes          | N bytes | 2 B LE | 0+ bytes
+-------+-------------------+--------+--------+
  LLLLMMMM                    literal   16-bit
                               data     little-endian
                                        back-reference
```

**Length encoding** (for both literal length and match length):

```
If nibble value < 15:  actual value = nibble value
If nibble value == 15: read additional bytes, each adding 0-255
                       continue reading while byte == 255
                       actual value = 15 + sum of additional bytes

Example:  nibble=15, additional bytes: 255, 255, 42
          length = 15 + 255 + 255 + 42 = 567
```

**Minimum match length** is 4 bytes. The low nibble stores `match_length - 4`. So a nibble value of 0 means match length 4, and nibble value 15 means match length 19+.

### 5.4 Step-by-Step Encoding Example

Let us encode: `ABCD ABCD EFGH ABCD` (19 bytes, spaces are actual space characters)

```
Position: 0         1
          0123456789012345678
Input:    ABCD ABCD EFGH ABCD
```

**Step 1**: Scan from position 0. Hash "ABCD". No previous match. Continue scanning.

- Positions 0-4 (`ABCD `) have no matches. Accumulate as literals.

**Step 2**: Position 5. Hash "ABCD". Match found at position 0! Offset=5, length=5 (`ABCD `).

- Emit Sequence 1: 5 literals (`ABCD `) + match (offset=5, length=5)
- Token byte: literal_len=5, match_len=5-4=1 --> `0101 0001` = `0x51`

```
Sequence 1:
  Token:    0x51   (lit_len=5, match_len_raw=1 --> match_len=5)
  Literals: A B C D ' '  (5 bytes)
  Offset:   05 00        (16-bit LE, offset=5)
```

**Step 3**: Position 10. Scan `EFGH `. No match for "EFGH". Continue.

**Step 4**: Position 15. Hash "ABCD". Match found at position 5 (or position 0). Offset=10, length=4.

- Emit Sequence 2: 5 literals (`EFGH `) + match (offset=10, length=4)
- Token byte: literal_len=5, match_len=4-4=0 --> `0101 0000` = `0x50`

```
Sequence 2:
  Token:    0x50   (lit_len=5, match_len_raw=0 --> match_len=4)
  Literals: E F G H ' '  (5 bytes)
  Offset:   0A 00        (16-bit LE, offset=10)
```

**Final compressed output** (14 bytes vs 19 original = 26% reduction):

```
[51] [41 42 43 44 20] [05 00] [50] [45 46 47 48 20] [0A 00]
 tok   A  B  C  D  SP  off=5  tok   E  F  G  H  SP  off=10
```

**Decoding** is extremely fast because the decoder just:

1. Reads the token byte.
2. Copies `lit_len` bytes verbatim from the compressed stream.
3. Reads the 2-byte offset.
4. Copies `match_len` bytes from the already-decoded output buffer.

No bit unpacking, no Huffman tree traversal, no arithmetic -- just `memcpy` operations.

### 5.5 LZ4 vs LZ4 HC (High Compression)

| Property             | LZ4 (default)                  | LZ4 HC                           |
|----------------------|--------------------------------|-----------------------------------|
| Compression speed    | ~780 MB/s                      | ~40-100 MB/s                      |
| Decompression speed  | ~4200 MB/s (same!)             | ~4200 MB/s (same!)                |
| Compression ratio    | ~2.1x                          | ~2.7x                             |
| Match finder         | Simple hash table              | Hash chains + binary tree search  |
| Compression levels   | Not applicable                 | 1-12                              |

The key insight: **LZ4 HC uses a more thorough search for matches (finding longer ones), but the output format is identical.** The decompressor does not know or care whether the data was compressed with LZ4 or LZ4 HC -- decompression speed is the same.

### 5.6 Use Case in Big Data

- **Default codec** in Apache Arrow IPC and Feather format.
- **Ideal for**: Streaming pipelines, in-memory caching, real-time analytics, shuffle data.
- **Where it excels**: Scenarios where decompression speed dominates (e.g., reading cached data, hot-path queries).
- **Used by**: Linux kernel (in-memory compression), Facebook (before ZSTD), ClickHouse, DuckDB.

---

## 6. Zstandard (ZSTD)

### 6.1 History and Design Goals

Zstandard (ZSTD) was created by **Yann Collet** at Facebook (now Meta) and released in 2016. Collet, who also created LZ4, designed ZSTD to answer a simple question: can we get GZIP-level compression ratios at speeds close to Snappy/LZ4?

The answer is yes. ZSTD achieves this by combining three techniques:

1. **LZ77 match finding** (like all LZ-family codecs)
2. **Huffman coding** for literals
3. **Finite State Entropy (FSE)** for match lengths, literal lengths, and offsets

### 6.2 Architecture: Three Stages

```
Input data
    |
    v
+----------------------+
| Stage 1: LZ77        |  Find repeated patterns
| (match finder)        |  Output: sequences of (literals, offset, match_length)
+----------------------+
    |
    v
+----------------------+
| Stage 2: Huffman      |  Compress the literal bytes
| (for literals)        |  (the raw bytes that had no LZ match)
+----------------------+
    |
    v
+----------------------+
| Stage 3: FSE          |  Compress the match metadata
| (for offsets, lengths)|  (offsets, match lengths, literal lengths)
+----------------------+
    |
    v
Compressed frame
```

**Why three stages?** The literal bytes and the match metadata have very different statistical distributions. Literals look like normal text/data. Match lengths and offsets have their own patterns (short matches are common, long offsets are rare). Using specialized entropy coders for each stream yields better compression than applying one encoder to everything.

### 6.3 Finite State Entropy (FSE) -- The Key Innovation

FSE is ZSTD's secret weapon. It provides compression quality close to **arithmetic coding** but at speeds close to **Huffman coding**. Here is the intuition:

**The problem with Huffman coding**: Huffman codes must be whole numbers of bits. If a symbol's optimal code length is 2.3 bits, Huffman must round to 2 or 3 bits, losing efficiency.

**Arithmetic coding** can encode symbols with fractional bit costs (2.3 bits), achieving near-optimal compression. But it requires expensive multiplication/division operations for each symbol.

**FSE** achieves fractional-bit encoding using **lookup tables** instead of arithmetic:

```
Traditional arithmetic coding:
  For each symbol: multiply, divide, update range  (slow)

FSE (tANS -- table-based Asymmetric Numeral System):
  For each symbol: table[state] --> new_state, output_bits  (fast!)
```

The FSE encoder maintains a **state** (an integer). For each symbol to encode, it looks up the current state in a precomputed table to determine:
- How many bits to output
- What the new state should be

The decoder does the reverse: it reads bits and uses a decoding table to recover symbols and transition between states. The tables are computed from symbol frequencies and transmitted in the compressed frame header.

```
Encoding with FSE (simplified):

  State: 17
  Symbol to encode: 'B'

  Table lookup: encode_table[state=17][symbol=B]
    --> output 3 bits: 101
    --> new state: 42

  State: 42
  Symbol to encode: 'A'

  Table lookup: encode_table[state=42][symbol=A]
    --> output 1 bit: 0
    --> new state: 8

  ...and so on.
```

The key insight is that FSE distributes symbols across the state table in proportion to their frequency, so common symbols use fewer output bits per state transition -- achieving the same effect as fractional-bit coding without any division.

### 6.4 Dictionary Training

ZSTD has a unique feature for compressing **small data** (< 1 KB): **pre-trained dictionaries**.

In big data, individual records (JSON objects, log lines, Kafka messages) are often very small. LZ77-based algorithms struggle with small inputs because the sliding window is nearly empty -- there is not enough history to find matches. But across millions of records, the same patterns repeat.

ZSTD can **train a dictionary** on a sample of records:

```
zstd --train sample1.json sample2.json ... sample1000.json -o my_dict

Then compress each record using the shared dictionary:
zstd --compress -D my_dict record.json -o record.json.zst
```

The dictionary (typically 32-112 KB) contains common byte sequences extracted from the training samples. The compressor seeds its sliding window with the dictionary content, so even the first byte of a tiny record can reference patterns from the dictionary.

**Impact**: On small JSON records (~500 bytes), dictionary compression can improve the ratio from 1.5x to 5x+.

### 6.5 Compression Levels (1-22)

ZSTD offers a wider range of compression levels than any other codec:

| Level   | Strategy            | Compress speed | Ratio | Use case                      |
|---------|---------------------|----------------|-------|-------------------------------|
| 1       | Fast greedy match   | ~500 MB/s      | ~2.9x | Real-time, hot data           |
| 3       | Default             | ~350 MB/s      | ~3.3x | General purpose (recommended) |
| 5       | Lazy matching       | ~150 MB/s      | ~3.5x | Balanced                      |
| 9       | Double lazy         | ~60 MB/s       | ~3.7x | Write-once/read-many          |
| 13      | BTree match finder  | ~20 MB/s       | ~3.9x | Cold storage                  |
| 19      | Optimal parsing     | ~4 MB/s        | ~4.1x | Archival                      |
| 22      | Ultra (max effort)  | ~1 MB/s        | ~4.2x | Maximum compression           |

What changes at higher levels:

- **Levels 1-3**: Greedy or lazy match finding with a hash table. Fast.
- **Levels 4-9**: Lazy and double-lazy matching. More thorough search for longer matches.
- **Levels 10-15**: Binary tree match finder (like LZ4 HC). Exhaustive search within the window.
- **Levels 16-19**: Optimal parsing using dynamic programming -- the compressor considers the global cost of different match choices, not just the locally best match.
- **Levels 20-22**: Maximum effort optimal parsing with extended search.

**Decompression speed is approximately constant (~800-1000 MB/s) regardless of the level used for compression.** This is a crucial property for big data: compress once at a high level, decompress many times at full speed.

### 6.6 Frame Format

```
+-------------+---------------------------------------------------+-----------+
| Magic Number| Frame Header                                      | Data      |
| 4 bytes     | (frame descriptor, window size, dict ID, etc.)    | Blocks    |
| 0xFD2FB528  |                                                   |           |
+-------------+---------------------------------------------------+-----------+

Each data block:
+------------+-----------------------------------------------------+
| Block      | Block Data                                          |
| Header     | (Huffman table + FSE tables + compressed sequences) |
| 3 bytes    |                                                     |
+------------+-----------------------------------------------------+

Block header (3 bytes = 24 bits):
  - Bit 0:      Last block flag (1 = this is the final block)
  - Bits 1-2:   Block type (0=Raw, 1=RLE, 2=Compressed, 3=Reserved)
  - Bits 3-23:  Block size (up to 128 KB)
```

### 6.7 Use Case in Big Data

- **Increasingly the recommended default** for modern big data systems.
- **Strengths**: Best ratio-to-speed tradeoff, configurable levels, dictionary support.
- **Supported by**: Parquet, Avro, ORC, Arrow/Feather, Kafka, HTTP (RFC 8878).
- **Recommendation**: Use ZSTD level 3 for general big data workloads. Use level 1 for latency-sensitive workloads. Use level 9-19 for archival/cold storage.
- **Real-world adoption**: Facebook (all internal data), Cloudflare, Linux kernel, Chrome, databases (RocksDB, ClickHouse, DuckDB).

---

## 7. BZip2

### 7.1 Overview

BZip2 was created by Julian Seward in 1996. It uses a completely different approach from the LZ family: instead of looking for repeated byte patterns, it performs a series of **reversible data transformations** that make the data more compressible, followed by entropy coding.

The pipeline has three stages:

```
Input data
    |
    v
+---------------------------+
| Stage 1: Burrows-Wheeler  |  Rearrange bytes to cluster identical characters
| Transform (BWT)           |
+---------------------------+
    |
    v
+---------------------------+
| Stage 2: Move-to-Front    |  Convert clustered characters to small integers
| Transform (MTF)           |
+---------------------------+
    |
    v
+---------------------------+
| Stage 3: Huffman Coding   |  Compress the small integers
+---------------------------+
    |
    v
Compressed output
```

### 7.2 Burrows-Wheeler Transform (BWT) -- Worked Example

The BWT is a remarkable algorithm: it rearranges the characters of a string so that **identical characters tend to cluster together**, making the output highly compressible. The transformation is fully reversible -- no data is lost.

#### Step-by-step: BWT of `BANANA$`

The `$` character is a special end-of-string marker, defined to sort before all other characters.

**Step 1: Generate all rotations of the string.**

```
Rotation 0:  B A N A N A $
Rotation 1:  A N A N A $ B
Rotation 2:  N A N A $ B A
Rotation 3:  A N A $ B A N
Rotation 4:  N A $ B A N A
Rotation 5:  A $ B A N A N
Rotation 6:  $ B A N A N A
```

**Step 2: Sort all rotations lexicographically.**

```
Row  Sorted rotation          Last char
---  ----------------------   ---------
 0   $ B A N A N A            A
 1   A $ B A N A N            N
 2   A N A $ B A N            N
 3   A N A N A $ B            B
 4   B A N A N A $            $
 5   N A $ B A N A            A
 6   N A N A $ B A            A
```

**Step 3: The BWT output is the last column, read top to bottom.**

```
BWT("BANANA$") = "ANNB$AA"
```

Also record the row index where the original string appears: row 4.

**Why this helps compression**: Look at the output `ANNB$AA`. The characters are more clustered: two `A`s together, two `N`s together. On longer, real-world data, this clustering effect is dramatic. English text might have runs like `...eeee...ttt...sss...` after BWT.

#### ASCII Rotation Matrix

```
                    +---+---+---+---+---+---+---+
Rotation 0:         | B | A | N | A | N | A | $ |  --> Last: $
Rotation 1:         | A | N | A | N | A | $ | B |  --> Last: B
Rotation 2:         | N | A | N | A | $ | B | A |  --> Last: A
Rotation 3:         | A | N | A | $ | B | A | N |  --> Last: N
Rotation 4:         | N | A | $ | B | A | N | A |  --> Last: A
Rotation 5:         | A | $ | B | A | N | A | N |  --> Last: N
Rotation 6:         | $ | B | A | N | A | N | A |  --> Last: A
                    +---+---+---+---+---+---+---+

After sorting:
                    +---+---+---+---+---+---+---+
Row 0:              | $ | B | A | N | A | N | A |  --> Last: A
Row 1:              | A | $ | B | A | N | A | N |  --> Last: N
Row 2:              | A | N | A | $ | B | A | N |  --> Last: N
Row 3:              | A | N | A | N | A | $ | B |  --> Last: B
Row 4 (original):   | B | A | N | A | N | A | $ |  --> Last: $
Row 5:              | N | A | $ | B | A | N | A |  --> Last: A
Row 6:              | N | A | N | A | $ | B | A |  --> Last: A
                    +---+---+---+---+---+---+---+

BWT output: A N N B $ A A    (last column)
Original row index: 4
```

### 7.3 Move-to-Front (MTF) Transform

After BWT, the data has many runs of identical or similar characters. The MTF transform exploits this by maintaining a list of all possible byte values and replacing each character with **its current position in the list**, then moving that character to the front.

#### Example: MTF of `ANNB$AA`

Start with alphabet list: `[$ A B N ...]` (sorted)

```
Input  | List before              | Position | Output | List after
-------|--------------------------|----------|--------|---------------------------
A      | [$ A B N ...]            | 1        | 1      | [A $ B N ...]
N      | [A $ B N ...]            | 3        | 3      | [N A $ B ...]
N      | [N A $ B ...]            | 0        | 0      | [N A $ B ...]  (already at front)
B      | [N A $ B ...]            | 3        | 3      | [B N A $ ...]
$      | [B N A $ ...]            | 3        | 3      | [$ B N A ...]
A      | [$ B N A ...]            | 3        | 3      | [A $ B N ...]
A      | [A $ B N ...]            | 0        | 0      | [A $ B N ...]
```

**MTF output**: `1 3 0 3 3 3 0`

**Key insight**: The MTF output contains many **small numbers** (especially 0s and 1s) because BWT clusters identical characters together, and a repeated character is always at position 0 after its first occurrence. Huffman coding on a distribution skewed toward small integers is extremely efficient.

### 7.4 Final Stage: Huffman Coding

The MTF output `1 3 0 3 3 3 0` is dominated by small numbers. Huffman coding assigns short codes to 0 and 3 (the most frequent values) and longer codes to rarer values. In practice, BZip2 uses a variant called **canonical Huffman coding** for efficient table representation.

### 7.5 Why BZip2 Is Slow

Each stage adds CPU cost:

1. **BWT**: Requires sorting all rotations of the input block (up to 900 KB). This is O(n log n) in the average case. For an 8 MB input, BZip2 processes nine 900 KB blocks, sorting each one.
2. **MTF**: Linear pass but adds overhead.
3. **Huffman**: Additional pass over the data.

The combination of three sequential passes, especially the sorting in BWT, makes BZip2 significantly slower than single-pass algorithms like Snappy or LZ4.

### 7.6 Native Splittability

BZip2 has a unique advantage: its blocks are **independently compressed** and separated by magic numbers (`0x314159265359` -- the digits of pi). A MapReduce/Spark framework can find block boundaries and assign each block to a separate mapper without decompressing the entire file. This made BZip2 historically important in Hadoop, where splittability was essential for parallelism.

### 7.7 Use Case in Big Data

- **Strengths**: Best compression ratio among common codecs (6-10x), natively splittable.
- **Weaknesses**: Very slow compression (~10-30 MB/s) and decompression (~30-60 MB/s).
- **Best for**: Archival storage, Hadoop legacy workloads.
- **Modern recommendation**: Replace with ZSTD at level 19+, which achieves similar ratios at 2-5x the speed.

---

## 8. Run-Length Encoding (RLE)

### 8.1 The Simplest Compression

Run-Length Encoding is the simplest form of compression: replace consecutive runs of the same value with a **(value, count)** pair.

### 8.2 Worked Examples

**Example 1: Character stream**

```
Input:    A A A A A B B B C C D D D D D D
Encoded:  5A 3B 2C 6D

Input:    16 bytes
Encoded:  8 bytes (4 value-count pairs)
Ratio:    2x
```

**Example 2: Integer column (e.g., "status" column in a table)**

```
Input:    1 1 1 1 1 1 2 2 2 2 3 3 3 3 3 3 3 3 1 1

RLE:     (1, 6) (2, 4) (3, 8) (1, 2)

Input:    20 values * 4 bytes = 80 bytes
Encoded:  4 pairs * 8 bytes = 32 bytes
Ratio:    2.5x
```

**Example 3: When RLE fails**

```
Input:    A B C D E F G H I J
Encoded:  1A 1B 1C 1D 1E 1F 1G 1H 1I 1J

Input:    10 bytes
Encoded:  20 bytes  <-- WORSE than uncompressed!
```

**Key insight**: RLE only works when there are actual **runs** of identical values. On data with no repetition, RLE can expand the data.

### 8.3 Bit-Packing Variant

A related technique used alongside RLE in columnar formats is **bit-packing**: if all values in a column fit in fewer bits than their declared type, pack them tightly.

```
Column type: INT32 (32 bits per value)
Actual values: 0, 1, 2, 3, 0, 1, 2, 3, 0, 1

Max value: 3 --> needs only 2 bits

Standard:    00000000...00 00000000...01 00000000...10 00000000...11
             (32 bits each = 320 bits for 10 values)

Bit-packed:  00 01 10 11 00 01 10 11 00 01
             (2 bits each = 20 bits for 10 values)

Ratio: 16x compression!
```

### 8.4 RLE in Parquet (Hybrid RLE/Bit-Packing)

Apache Parquet uses a **hybrid RLE/bit-packing** encoding for definition levels, repetition levels, and dictionary indices. The format adaptively switches between two modes:

```
Indicator byte:
  - If lowest bit = 0: RLE run follows
  - If lowest bit = 1: bit-packed group follows

RLE run:
  +--------------------+-------+
  | Run length (varint) | Value  |
  | (header >> 1)       | (N bits)|
  +--------------------+-------+

Bit-packed group:
  +--------------------+----------------------------+
  | Group count (varint)| Packed values              |
  | (header >> 1) * 8   | (count * bit_width bits)   |
  +--------------------+----------------------------+
```

**Example**: A dictionary-encoded column with sorted data:

```
Dictionary index values: 0 0 0 0 0 0 0 0 1 1 1 1 2 2 2 3

Using 2-bit width:
  RLE: (8, 0)  (4, 1)  (3, 2)  (1, 3)

  16 values * 2 bits = 32 bits uncompressed
  4 RLE pairs = ~8 bytes compressed (with headers)
```

### 8.5 When RLE Works Well

| Data characteristic                  | RLE effectiveness |
|--------------------------------------|-------------------|
| Sorted column with few distinct vals | Excellent         |
| Boolean column (TRUE/FALSE runs)     | Excellent         |
| Timestamp column (same second)       | Good              |
| Status/enum columns                  | Good              |
| Random UUIDs                         | Terrible          |
| High-cardinality strings             | Terrible          |

**Key takeaway**: RLE is most effective on **sorted, low-cardinality** data. This is why Parquet partitioning and sorting strategies directly impact compression ratios.

---

## 9. Dictionary Encoding

### 9.1 Concept

Dictionary encoding is not a general-purpose compression algorithm but an **encoding scheme** that converts repeated values into compact integer indices. It is a critical component of columnar file formats (Parquet, ORC) and provides the foundation for efficient compression of string columns.

### 9.2 How It Works

**Step 1**: Scan the column values and build a dictionary of unique values.
**Step 2**: Replace each value with its index in the dictionary.
**Step 3**: Store the dictionary and the index array separately.

### 9.3 Worked Example

Consider a column `country` with 12 rows:

```
Original column:
+--------+
| country|
+--------+
| USA    |
| Germany|
| USA    |
| France |
| USA    |
| Germany|
| France |
| USA    |
| Germany|
| USA    |
| USA    |
| France |
+--------+

Storage: 12 strings, average ~5 chars = ~60 bytes
```

**After dictionary encoding**:

```
Dictionary (stored once):         Index array (stored per row):
+-------+---------+               +-------+
| Index | Value   |               | Index |
+-------+---------+               +-------+
|   0   | USA     |               |   0   |  (USA)
|   1   | Germany |               |   1   |  (Germany)
|   2   | France  |               |   0   |  (USA)
+-------+---------+               |   2   |  (France)
                                  |   0   |  (USA)
Storage: 3 strings               |   1   |  (Germany)
= ~15 bytes                      |   2   |  (France)
                                  |   0   |  (USA)
                                  |   1   |  (Germany)
                                  |   0   |  (USA)
                                  |   0   |  (USA)
                                  |   2   |  (France)
                                  +-------+

                                  Storage: 12 indices * 2 bits = 3 bytes
                                  (bit-packed: only 3 unique values = 2 bits each)

Total: 15 + 3 = 18 bytes vs 60 bytes original = 3.3x compression
```

### 9.4 Combining Dictionary Encoding with Other Techniques

The integer index array is highly compressible by further techniques:

```
Original column:       USA USA USA Germany Germany France USA USA
                        |   |   |    |       |      |     |   |
Dictionary indices:     0   0   0    1       1      2     0   0

After dictionary encoding, the index array can be further compressed:

1. Bit-packing: 3 unique values --> 2 bits each (instead of 32 bits per int)

2. RLE (if sorted):
   Sorted indices: 0 0 0 0 0 1 1 2
   RLE: (5, 0) (2, 1) (1, 2)  --> extremely compact!

3. Delta encoding (if ordered):
   Indices: 0 0 0 1 1 2 0 0
   Deltas:  0 0 0 1 0 1 -2 0  --> many zeros, compresses well
```

### 9.5 Dictionary Size Limits and Fallback

Parquet implements dictionary encoding with a **page-level dictionary size limit** (default: 1 MB). When the dictionary grows too large (too many distinct values), Parquet automatically falls back to **PLAIN encoding** for that column chunk.

```
Decision logic in Parquet:

  For each page of data:
    If dict_size < threshold (1 MB default):
      Use DICT_ENCODING (dictionary + RLE/bit-packed indices)
    Else:
      Fall back to PLAIN encoding (raw values)

  This happens automatically -- the writer decides per column chunk.
```

**Practical implication**: Dictionary encoding is effective when a column has **low to moderate cardinality** (fewer than ~100K distinct values). For columns like `user_id` (millions of distinct values) or `description` (nearly all unique), dictionary encoding provides no benefit and Parquet switches to PLAIN.

| Column type         | Distinct values | Dictionary effective? |
|---------------------|-----------------|-----------------------|
| Country code        | ~200            | Excellent             |
| HTTP status code    | ~50             | Excellent             |
| Product category    | ~1,000          | Very good             |
| City name           | ~50,000         | Good                  |
| Email address       | ~10,000,000     | Poor (falls back)     |
| UUID                | All unique      | Terrible (falls back) |

### 9.6 Dictionary Encoding in ORC

ORC also uses dictionary encoding but with a different threshold strategy. ORC estimates whether dictionary encoding will be beneficial based on the ratio of unique values to total values. If the number of distinct values exceeds a configurable threshold (default: 80% of the total values), ORC uses **direct encoding** instead.

---

## 10. Practical Comparison

### 10.1 Benchmark Comparison Table

The following table summarizes typical performance characteristics measured on a single core with a representative big data workload (mixed text and numeric data, ~1 GB uncompressed). Actual numbers vary by data type, hardware, and implementation.

| Codec       | Ratio  | Compress Speed | Decompress Speed | CPU Usage    | Memory    | Best For                         |
|-------------|--------|----------------|------------------|--------------|-----------|----------------------------------|
| **None**    | 1.0x   | N/A            | N/A              | None         | None      | Debugging, very fast I/O         |
| **Snappy**  | 2-4x   | ~250 MB/s      | ~500 MB/s        | Low          | ~256 KB   | Low-latency, interactive queries |
| **LZ4**     | 2-3x   | ~780 MB/s      | ~4200 MB/s       | Very Low     | ~64 KB    | Streaming, in-memory, real-time  |
| **LZ4 HC**  | 2.5-4x | ~40-100 MB/s   | ~4200 MB/s       | Moderate     | ~256 KB   | Write-once, fast decompression   |
| **GZIP-1**  | 4-6x   | ~80 MB/s       | ~250 MB/s        | Moderate     | ~256 KB   | Compatibility, moderate ratio    |
| **GZIP-6**  | 5-8x   | ~30 MB/s       | ~250 MB/s        | High         | ~256 KB   | Storage cost optimization        |
| **GZIP-9**  | 5-9x   | ~10 MB/s       | ~250 MB/s        | Very High    | ~256 KB   | Maximum GZIP compression         |
| **ZSTD-1**  | 3-5x   | ~500 MB/s      | ~800 MB/s        | Low          | ~1 MB     | General purpose, fast            |
| **ZSTD-3**  | 4-6x   | ~350 MB/s      | ~800 MB/s        | Low-Moderate | ~1 MB     | General purpose (recommended)    |
| **ZSTD-9**  | 5-7x   | ~60 MB/s       | ~800 MB/s        | Moderate     | ~2 MB     | Write-once/read-many             |
| **ZSTD-19** | 6-8x   | ~4 MB/s        | ~800 MB/s        | Very High    | ~8 MB     | Archival, cold storage           |
| **BZip2**   | 6-10x  | ~10-30 MB/s    | ~30-60 MB/s      | Very High    | ~8 MB     | Archival, legacy Hadoop          |

> Note: Speeds are per single CPU core. Multi-threaded implementations (pigz for GZIP, pzstd for ZSTD) can scale nearly linearly with core count.

### 10.2 Visual Speed vs Ratio Comparison

```
Compression Ratio
(higher = smaller file)

 10x |                                                      BZip2
     |                                                  *
  8x |                                        ZSTD-19
     |                                    *
  7x |                          ZSTD-9
     |                      *
  6x |             GZIP-6                    GZIP-9
     |           *                         *
  5x |     ZSTD-3          GZIP-1
     |   *              *
  4x |  ZSTD-1                                          LZ4HC
     |*                                               *
  3x |                          Snappy
     |                        *
  2x |                                   LZ4
     |                                 *
  1x |
     +----+--------+--------+--------+--------+--------+----->
          1000     500      250      100       30       10
                                                         MB/s
                    Compression Speed (log scale)
```

### 10.3 Decision Flowchart

```
START: Choosing a compression codec for big data
  |
  v
Is this a latency-sensitive, real-time workload?
  |                     |
  YES                   NO
  |                     |
  v                     v
Do you need THE         Is storage cost a primary concern?
fastest decompression?  |                    |
  |           |         YES                  NO
  YES         NO        |                    |
  |           |         v                    v
  v           v         Will data be read    Use ZSTD level 3
  LZ4         Snappy    frequently?          (best general-purpose default)
                        |           |
                        YES         NO
                        |           |
                        v           v
                        ZSTD        Need max compatibility
                        level 9     with legacy systems?
                                    |           |
                                    YES         NO
                                    |           |
                                    v           v
                                    GZIP        ZSTD level 19
                                    (level 6)   (modern archival)
```

### 10.4 Default Codecs by Format

Understanding which codec each format uses by default and which is recommended helps avoid surprises:

| Format         | Default codec  | Recommended codec  | Notes                                        |
|----------------|----------------|--------------------|----------------------------------------------|
| **Parquet**    | Snappy         | ZSTD (level 3)     | Snappy for compatibility, ZSTD for new work  |
| **Avro**       | null (none)    | Snappy or ZSTD     | Must be explicitly configured                |
| **ORC**        | ZLIB           | ZSTD               | ZLIB for Hive compatibility, ZSTD for speed  |
| **Feather v2** | LZ4            | LZ4 or ZSTD        | LZ4 for speed, ZSTD for smaller files        |
| **CSV**        | none           | GZIP (external)    | `.csv.gz` is the standard convention         |
| **JSON Lines** | none           | GZIP (external)    | `.jsonl.gz` is common                        |

---

## 11. Codec Support Matrix

The following table shows which compression codecs are supported by which file formats. "Default" means the format uses this codec by default if none is specified.

| Codec        | Parquet           | Avro              | ORC               | Feather (Arrow IPC) |
|--------------|-------------------|--------------------|--------------------|---------------------|
| **None**     | Supported         | Default            | Supported          | Supported           |
| **Snappy**   | Default           | Supported          | Supported          | Not supported       |
| **GZIP**     | Supported         | Supported          | Not supported      | Not supported       |
| **ZLIB**     | Not supported     | Not supported      | Default            | Not supported       |
| **LZ4**      | Supported         | Not supported      | Supported          | Default             |
| **LZ4 raw**  | Supported (v2.9+) | Not supported      | Not supported      | Supported           |
| **ZSTD**     | Supported         | Supported (1.9+)   | Supported (1.1+)   | Supported           |
| **BZip2**    | Supported         | Supported          | Not supported      | Not supported       |
| **LZO**      | Supported (ext.)  | Not supported      | Supported          | Not supported       |
| **Brotli**   | Supported         | Not supported      | Not supported      | Not supported       |

> Notes:
> - "Supported (ext.)" means support exists but may require additional libraries.
> - Parquet has the broadest codec support because its specification defines compression at the page level.
> - ORC uses ZLIB (which is raw Deflate + Adler-32 checksum), not GZIP (which adds file-level headers).
> - Feather v2 (Arrow IPC) supports LZ4 and ZSTD; Feather v1 had no compression.
> - Avro's ZSTD support was added in Avro 1.9.0 (2019).
> - Availability also depends on the specific language library (PyArrow, Java, Rust, etc.).

### Quick Reference: Choosing a Format + Codec Combination

| Scenario                                  | Recommended combination              |
|-------------------------------------------|--------------------------------------|
| Analytical data lake (Spark, Trino, etc.) | Parquet + ZSTD (level 3)             |
| Kafka message serialization               | Avro + Snappy (or no compression)    |
| Hive data warehouse                       | ORC + ZSTD (or ZLIB for legacy)      |
| Python/R interactive analytics            | Feather + LZ4                        |
| Inter-process data exchange               | Arrow IPC + LZ4                      |
| Cold archival (rarely read)               | Parquet + ZSTD (level 19)            |
| Maximum compatibility (any tool)          | Parquet + Snappy (or GZIP)           |
| Small files / Kafka Streams state store   | ZSTD with trained dictionary         |

---

## Summary of Key Insights

Each compression algorithm has a **core insight** that makes it work:

| Algorithm         | Core insight                                                                                   |
|-------------------|------------------------------------------------------------------------------------------------|
| **LZ77**          | If a byte pattern appeared recently, store a short back-reference instead of repeating it      |
| **LZ78**          | Build an explicit dictionary of seen patterns; encode new patterns as dictionary index + char   |
| **Huffman**       | Assign short bit codes to frequent symbols and long codes to rare symbols                      |
| **Deflate/GZIP**  | Combine LZ77 (pattern matching) + Huffman (entropy coding) in a two-stage pipeline             |
| **Snappy**        | Skip entropy coding entirely; trade compression ratio for maximum speed                        |
| **LZ4**           | Optimize the LZ77 output format for decoder speed; simple token-based format with no bit ops   |
| **ZSTD**          | Combine LZ77 + Huffman + FSE to get near-arithmetic-coding efficiency at table-lookup speed    |
| **BWT (BZip2)**   | Rearrange bytes via all-rotations sort so identical chars cluster; then entropy code the result |
| **RLE**           | Replace runs of identical values with (value, count) pairs                                     |
| **Dictionary**    | Replace repeated values with integer indices into a lookup table                               |

---

## References and Further Reading

1. Ziv, J. and Lempel, A. (1977). "A Universal Algorithm for Sequential Data Compression." IEEE Transactions on Information Theory.
2. Ziv, J. and Lempel, A. (1978). "Compression of Individual Sequences via Variable-Rate Coding." IEEE Transactions on Information Theory.
3. Huffman, D.A. (1952). "A Method for the Construction of Minimum-Redundancy Codes." Proceedings of the IRE.
4. Deutsch, P. (1996). RFC 1951: DEFLATE Compressed Data Format Specification.
5. Google. "Snappy: A fast compressor/decompressor." https://github.com/google/snappy
6. Collet, Y. "LZ4: Extremely fast compression." https://github.com/lz4/lz4
7. Collet, Y. and Kucherawy, M. (2018). RFC 8478: Zstandard Compression and the application/zstd Media Type.
8. Collet, Y. "Finite State Entropy: A new breed of entropy coder." https://github.com/Cyan4973/FiniteStateEntropy
9. Seward, J. "bzip2 and libbzip2." https://sourceware.org/bzip2/
10. Burrows, M. and Wheeler, D. (1994). "A Block-sorting Lossless Data Compression Algorithm." DEC SRC Research Report 124.
11. Apache Parquet Format Specification. https://parquet.apache.org/documentation/latest/
12. Apache Avro Specification. https://avro.apache.org/docs/current/specification/
13. Apache ORC Specification. https://orc.apache.org/specification/
