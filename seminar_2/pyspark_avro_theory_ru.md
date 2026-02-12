# Формат Avro в PySpark: теория, практика и сравнение

> **Семинар 2 -- Avro и распределённая обработка данных в PySpark**
> Продолжительность: ~25-30 минут лекционного материала (или справочник для самостоятельного изучения)
> Необходимые знания: базовое понимание форматов файлов (CSV, JSON, Parquet), знакомство с Apache Spark
> Сопутствующий материал: `columnar_and_row_formats_theory.md`, `compression_codecs_theory_ru.md`

---

## Содержание

1. [Почему Avro неэффективен в чистом Python](#1-почему-avro-неэффективен-в-чистом-python)
2. [Почему Avro эффективен в PySpark](#2-почему-avro-эффективен-в-pyspark)
3. [Как работать с Avro в PySpark](#3-как-работать-с-avro-в-pyspark)
4. [Avro vs Parquet в PySpark](#4-avro-vs-parquet-в-pyspark)
5. [Интеграция Avro с Kafka в PySpark Structured Streaming](#5-интеграция-avro-с-kafka-в-pyspark-structured-streaming)
6. [Итоги и рекомендации](#6-итоги-и-рекомендации)

---

## 1. Почему Avro неэффективен в чистом Python

### 1.1 Проблема: fastavro и построчная обработка

Apache Avro -- это **строчный (row-based)** формат сериализации данных. В экосистеме Python
для работы с Avro используется библиотека `fastavro`. Хотя fastavro написана на Cython и
работает значительно быстрее, чем официальный пакет `avro-python3`, она всё равно имеет
фундаментальные ограничения, связанные с природой самого формата и однопоточностью Python.

**Ключевые ограничения fastavro:**

| Ограничение | Описание |
|---|---|
| Построчное чтение | fastavro читает данные **строка за строкой** (record by record). Для 5 млн строк это 5 млн итераций Python-цикла |
| Нет column projection | Avro -- строчный формат. Чтобы прочитать 3 колонки из 10, нужно десериализовать **все 10** колонок каждой строки |
| Нет predicate pushdown | Чтобы отфильтровать строки по условию, нужно сначала прочитать **все** строки в память, а потом фильтровать |
| Однопоточность | CPython GIL не позволяет параллелить чтение в рамках одного процесса |

### 1.2 Как выглядит типичный код на fastavro

Рассмотрим, как наш бенчмарк-скрипт пишет данные в Avro через fastavro:

```python
import fastavro
import pandas as pd

def write_avro(df: pd.DataFrame, path: str, codec: str = "snappy") -> None:
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

    # ПРОБЛЕМА: itertuples() создаёт Python-объект для КАЖДОЙ строки
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
```

**Что здесь плохо?**

1. `itertuples()` -- это цикл по каждой строке DataFrame. Для 5 млн строк создаётся
   5 млн Python-объектов `namedtuple`.
2. Для каждой строки создаётся Python-словарь `dict` с 10 ключами -- ещё 5 млн словарей.
3. Вся эта работа выполняется **в одном потоке**, в одном процессе Python.
4. Только после создания всех записей fastavro начинает сериализацию в файл.

### 1.3 Количественная оценка проблемы

На практике для нашего тестового датасета (5 млн строк, 10 колонок) мы видим следующее:

```
Формат                     Запись (сек)   Чтение (сек)   Размер (МБ)
-----------------------------------------------------------------
Parquet (snappy, pyarrow)    3-5            2-4            ~120
CSV (plain, pandas)          25-35          15-25          ~700
Avro (snappy, fastavro)      90-180         40-80          ~250
```

Avro через fastavro **в 20-50 раз медленнее** Parquet при записи! И это при том, что
итоговый файл больше, потому что Avro -- строчный формат и не может использовать
колоночное сжатие (dictionary encoding, RLE для повторяющихся значений в колонке).

### 1.4 Почему это непрактично для больших данных

Для **миллионов строк** и более:

- **Запись 30 млн строк** в Avro через fastavro может занять **30-60 минут** (vs 15-30 секунд для Parquet)
- **Чтение** требует десериализации каждой записи в Python-объект -- нет vectorized операций
- **Фильтрация** невозможна без полного чтения: нужно прочитать все строки, создать DataFrame, потом фильтровать
- **Column projection** невозможна: fastavro не поддерживает чтение отдельных полей из record
- **Память**: 30 млн словарей по ~500 байт каждый = ~15 ГБ RAM только для промежуточного представления

**Вывод**: fastavro подходит для небольших файлов (десятки-сотни тысяч записей), для интеграции
с Java/Scala-системами, для работы со Schema Registry. Но для аналитики на миллионах строк
это инструмент категорически неподходящий.

---

## 2. Почему Avro эффективен в PySpark

### 2.1 Принципиально другая архитектура

Apache Spark кардинально меняет ситуацию с Avro, потому что:

1. **Сериализация выполняется на JVM**, а не в Python
2. Данные обрабатываются **распределённо** на кластере
3. Spark имеет **нативную поддержку** Avro через пакет `spark-avro`
4. **Schema evolution** работает из коробки
5. Avro является **стандартом** для Apache Kafka -- основной шины данных в real-time системах

### 2.2 Как Spark работает с Avro внутри

Когда вы вызываете `spark.read.format("avro").load(path)`, происходит следующее:

```
                                      Spark Cluster
                                  ┌─────────────────────┐
                                  │                     │
  Avro-файл(ы)                    │   Executor 1        │
  ┌──────────┐    ─────────>      │   ┌──────────────┐  │
  │ Block 1  │                    │   │ JVM-десерiali- │  │
  │ Block 2  │    Разделение      │   │ зация Avro    │  │
  │ Block 3  │    по блокам       │   │ (без Python!) │  │
  │ Block 4  │    ─────────>      │   └──────────────┘  │
  │ ...      │                    │                     │
  └──────────┘                    │   Executor 2        │
                                  │   ┌──────────────┐  │
                                  │   │ JVM-десерiali- │  │
                                  │   │ зация Avro    │  │
                                  │   └──────────────┘  │
                                  │                     │
                                  │   Executor N        │
                                  │   ┌──────────────┐  │
                                  │   │ JVM-десерiali- │  │
                                  │   │ зация Avro    │  │
                                  │   └──────────────┘  │
                                  └─────────────────────┘
```

**Ключевые отличия от fastavro:**

| Аспект | fastavro (Python) | spark-avro (JVM) |
|---|---|---|
| Язык сериализации | Cython/Python | Java (JVM, JIT-компиляция) |
| Параллелизм | Один поток | N executor-ов * M ядер |
| Промежуточное представление | Python dict | Tungsten binary format |
| Memory layout | Python heap (GC overhead) | Off-heap, columnar |
| Обработка null-значений | `pd.isna()` проверка для каждого поля | Bitmask в Tungsten |
| Скорость десериализации | ~100K записей/сек | ~10M записей/сек на ядро |

### 2.3 Нативная поддержка через spark-avro

Пакет `spark-avro` (ранее Databricks spark-avro) стал частью Apache Spark начиная с версии 2.4.
Это означает:

- **Нет overhead-а Python** при чтении/записи Avro -- вся работа на JVM
- **Автоматическое определение схемы** из Avro-файла
- **Кодеки сжатия**: snappy, deflate, bzip2, xz
- **Поддержка Avro logical types**: date, timestamp-millis, decimal и т.д.
- **Splittable**: Spark может разбить большой Avro-файл на блоки и обрабатывать параллельно

### 2.4 Распределённая обработка

В режиме кластера Spark распределяет чтение Avro-файлов по executor-ам:

```python
# Один файл 10 ГБ → разбивается на блоки по ~128 МБ
# Каждый блок обрабатывается своим task-ом на своём ядре
df = spark.read.format("avro").load("hdfs:///data/events/*.avro")

# 10 ГБ / 128 МБ = ~80 task-ов
# На кластере из 10 executor-ов * 4 ядра = 40 ядер
# 80 task-ов / 40 ядер = 2 волны по ~2-3 секунды = ~5 секунд
```

Для сравнения, fastavro на одном ядре читает 10 ГБ Avro за ~10-15 минут.

### 2.5 Schema Evolution из коробки

Одно из главных преимуществ Avro -- **эволюция схемы** (schema evolution). В отличие от
CSV или JSON, Avro хранит схему в заголовке файла и поддерживает:

- **Добавление полей** с default-значением -- старые файлы читаются без ошибок
- **Удаление полей** -- при чтении просто игнорируются
- **Переименование полей** через aliases
- **Изменение типов** (с совместимыми промоутами: int → long, float → double)

```
Версия схемы 1 (январь):        Версия схемы 2 (февраль):
┌─────────────────────┐         ┌─────────────────────┐
│ event_id: long      │         │ event_id: long      │
│ user_id: long       │         │ user_id: long       │
│ event_type: string  │         │ event_type: string  │
│ revenue: double     │         │ revenue: double     │
└─────────────────────┘         │ currency: string    │  ← НОВОЕ поле
                                │   default: "USD"    │
                                └─────────────────────┘

Spark прочитает ОБА файла в один DataFrame:
- Старые записи получат currency = "USD" (default)
- Новые записи будут иметь реальное значение currency
```

### 2.6 Стандарт для Kafka + Spark Streaming

В production-системах Avro является **де-факто стандартом** для:

- **Apache Kafka** -- сообщения сериализуются в Avro
- **Confluent Schema Registry** -- хранит и версионирует Avro-схемы
- **Spark Structured Streaming** -- нативно читает Avro из Kafka
- **Apache Flink** -- аналогичная интеграция

Это означает, что данные, приходящие из Kafka, уже в Avro-формате, и Spark может
обрабатывать их без дополнительной конвертации.

---

## 3. Как работать с Avro в PySpark

### 3.1 Подключение spark-avro

Для работы с Avro в PySpark необходим пакет `spark-avro`. Он подключается при создании
SparkSession:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Avro Demo")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-avro_2.12:3.5.0"
    )
    .getOrCreate()
)
```

**Важно**: версия `spark-avro` должна соответствовать версии вашего Spark:
- Spark 3.5.x → `spark-avro_2.12:3.5.0`
- Spark 3.4.x → `spark-avro_2.12:3.4.0`
- Spark 3.3.x → `spark-avro_2.12:3.3.0`

Суффикс `_2.12` указывает на версию Scala, с которой собран Spark.

### 3.2 Чтение Avro-файлов

#### Простое чтение

```python
# Чтение одного файла
df = spark.read.format("avro").load("./data/events.avro")

# Чтение директории с Avro-файлами (все файлы объединяются)
df = spark.read.format("avro").load("./data/events/*.avro")

# Чтение с HDFS
df = spark.read.format("avro").load("hdfs:///warehouse/events/")

# Чтение с S3
df = spark.read.format("avro").load("s3a://bucket/events/")
```

#### Просмотр схемы и данных

```python
# Схема автоматически определяется из Avro-файла
df.printSchema()
# root
#  |-- event_id: long (nullable = true)
#  |-- user_id: long (nullable = true)
#  |-- timestamp: long (nullable = true)
#  |-- event_type: string (nullable = true)
#  |-- page_url: string (nullable = true)
#  |-- duration_sec: double (nullable = true)
#  |-- revenue: double (nullable = true)
#  |-- device: string (nullable = true)
#  |-- country: string (nullable = true)
#  |-- session_tags: string (nullable = true)

# Первые 5 строк
df.show(5, truncate=False)

# Количество строк (ВАЖНО: вызывает полное чтение файла!)
print(f"Всего строк: {df.count()}")
```

### 3.3 Запись в Avro

#### Простая запись

```python
# Запись без сжатия
df.write.format("avro").save("./output/events_avro")

# Запись с перезаписью
df.write.format("avro").mode("overwrite").save("./output/events_avro")

# Режимы записи:
# - "overwrite" -- перезаписать
# - "append"    -- дописать
# - "ignore"    -- не записывать, если существует
# - "error"     -- ошибка, если существует (по умолчанию)
```

#### Запись с настройкой сжатия

```python
# Настройка codec через SparkSession
spark.conf.set("spark.sql.avro.compression.codec", "snappy")
df.write.format("avro").save("./output/events_snappy")

# Или через option при записи
df.write.format("avro") \
    .option("compression", "snappy") \
    .save("./output/events_snappy")

# Доступные кодеки: snappy, deflate, bzip2, xz, uncompressed
# Snappy -- лучший выбор по балансу скорости и сжатия
```

#### Запись с партиционированием

```python
# Партиционирование по country -- создаст поддиректории:
# ./output/events_partitioned/country=US/
# ./output/events_partitioned/country=DE/
# ./output/events_partitioned/country=GB/
# ...
df.write.format("avro") \
    .partitionBy("country") \
    .option("compression", "snappy") \
    .save("./output/events_partitioned")

# Двухуровневое партиционирование
df.write.format("avro") \
    .partitionBy("country", "device") \
    .option("compression", "snappy") \
    .save("./output/events_partitioned_2level")

# При чтении Spark автоматически использует partition pruning:
# Если запрос фильтрует по country='US', Spark прочитает только
# директорию country=US/ и пропустит остальные 9 директорий.
```

### 3.4 Выбор колонок (Column Projection)

Хотя Avro -- строчный формат и не поддерживает нативный column projection на уровне
файла (как Parquet), в PySpark мы можем эффективно выбирать колонки:

```python
# Выбор конкретных колонок
df_selected = df.select("user_id", "event_type", "revenue")
df_selected.show(5)

# +--------+----------+--------+
# | user_id|event_type| revenue|
# +--------+----------+--------+
# |  456123| page_view|    null|
# |  789456|  purchase|   49.99|
# |  123789|     click|    null|
# +--------+----------+--------+

# Выбор с переименованием
from pyspark.sql.functions import col

df_renamed = df.select(
    col("user_id").alias("uid"),
    col("event_type").alias("event"),
    col("revenue").alias("amount")
)

# Выбор с вычисляемыми колонками
from pyspark.sql.functions import when, lit

df_enriched = df.select(
    "user_id",
    "event_type",
    when(col("revenue").isNotNull(), col("revenue"))
        .otherwise(lit(0.0))
        .alias("revenue_filled")
)
```

**Примечание**: для Avro column projection не экономит I/O на уровне файла (все колонки
всё равно десериализуются), но Spark оптимизирует это через Catalyst optimizer --
неиспользуемые колонки отбрасываются сразу после десериализации, не создавая overhead
в последующих трансформациях.

### 3.5 Фильтрация (Predicate Pushdown)

```python
# Простая фильтрация
df_purchases = df.filter(df.event_type == "purchase")
print(f"Покупки: {df_purchases.count()}")

# Альтернативный синтаксис
df_purchases = df.where(col("event_type") == "purchase")

# Составные условия
df_high_value = df.filter(
    (col("event_type") == "purchase") &
    (col("revenue") > 100.0) &
    (col("country").isin("US", "DE", "GB"))
)

# SQL-синтаксис
df.createOrReplaceTempView("events")
df_sql = spark.sql("""
    SELECT *
    FROM events
    WHERE event_type = 'purchase'
      AND revenue > 100.0
      AND country IN ('US', 'DE', 'GB')
""")
```

**Важно**: для Avro predicate pushdown на уровне файла **не работает** (в отличие от
Parquet, где можно пропустить целые row-группы по min/max статистике). Однако:

- Если данные **партиционированы** (`partitionBy("country")`), то Spark пропустит
  целые директории, которые не соответствуют фильтру -- это называется **partition pruning**
- На кластере фильтрация выполняется **параллельно** на всех executor-ах

### 3.6 Агрегация

```python
from pyspark.sql.functions import count, avg, sum as spark_sum, round as spark_round

# Базовая агрегация: аналог SQL GROUP BY
agg_df = df.groupBy("country", "event_type").agg(
    count("*").alias("cnt"),
    spark_round(avg("duration_sec"), 2).alias("avg_duration"),
    spark_round(spark_sum("revenue"), 2).alias("total_revenue")
)

agg_df.orderBy("country", "event_type").show(20)

# +---------+----------+------+------------+-------------+
# | country|event_type|   cnt|avg_duration|total_revenue|
# +---------+----------+------+------------+-------------+
# |       AU| page_view| 10234|      152.31|         null|
# |       AU|  purchase|   987|      148.76|     48923.45|
# |       AU|     click| 10156|      149.88|         null|
# |       BR| page_view| 10312|      151.09|         null|
# ...

# Более сложная агрегация с window functions
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, dense_rank

# Ранжирование стран по суммарной выручке
window_spec = Window.orderBy(col("total_revenue").desc())

top_countries = (
    df.filter(col("event_type") == "purchase")
    .groupBy("country")
    .agg(spark_sum("revenue").alias("total_revenue"))
    .withColumn("rank", dense_rank().over(window_spec))
    .orderBy("rank")
)

top_countries.show(10)
```

### 3.7 Schema Evolution: чтение с изменённой схемой

Одна из главных причин выбирать Avro -- поддержка эволюции схемы.

```python
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

# Допустим, у нас есть старые Avro-файлы с 5 полями:
# event_id, user_id, event_type, revenue, country

# И новые файлы с дополнительным полем:
# event_id, user_id, event_type, revenue, country, currency (default: "USD")

# Определяем "reader schema" -- схему, которую мы ХОТИМ прочитать
avro_schema_json = """
{
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "event_id", "type": "long"},
        {"name": "user_id", "type": "long"},
        {"name": "event_type", "type": "string"},
        {"name": "revenue", "type": ["null", "double"], "default": null},
        {"name": "country", "type": "string"},
        {"name": "currency", "type": "string", "default": "USD"}
    ]
}
"""

# Spark прочитает файлы, применяя schema evolution:
# - Старые файлы: currency будет заполнено "USD" (default)
# - Новые файлы: currency будет иметь реальное значение
df = (
    spark.read.format("avro")
    .option("avroSchema", avro_schema_json)
    .load("./data/events_all_versions/")
)

df.select("event_id", "country", "currency").show(5)
# +--------+-------+--------+
# |event_id|country|currency|
# +--------+-------+--------+
# |       1|     US|     USD|   ← из старого файла (default)
# |       2|     DE|     EUR|   ← из нового файла
# |       3|     JP|     JPY|   ← из нового файла
# +--------+-------+--------+
```

### 3.8 Настройка параметров производительности

```python
# Настройка codec сжатия (применяется глобально)
spark.conf.set("spark.sql.avro.compression.codec", "snappy")

# Настройка уровня сжатия для deflate (1-9, по умолчанию 6)
spark.conf.set("spark.sql.avro.deflate.level", "6")

# Увеличение параллелизма при чтении
# (по умолчанию Spark создаёт 1 partition на 128 МБ данных)
spark.conf.set("spark.sql.files.maxPartitionBytes", "67108864")  # 64 МБ → больше task-ов

# Включение predicate pushdown для партиционированных данных
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "true")

# Настройка памяти для больших датасетов
spark.conf.set("spark.executor.memory", "4g")
spark.conf.set("spark.driver.memory", "4g")
```

### 3.9 Полный пример: чтение, обработка, запись

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum, round as spark_round

# 1. Создаём SparkSession с поддержкой Avro
spark = (
    SparkSession.builder
    .appName("Avro Full Example")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0")
    .config("spark.sql.avro.compression.codec", "snappy")
    .getOrCreate()
)

# 2. Читаем данные (например, из Parquet -- как наш generated_data.parquet)
df = spark.read.parquet("./benchmark_output/generated_data.parquet")
print(f"Загружено строк: {df.count()}")
df.printSchema()

# 3. Пишем в Avro с партиционированием
df.write.format("avro") \
    .mode("overwrite") \
    .partitionBy("country") \
    .option("compression", "snappy") \
    .save("./benchmark_output/spark_avro_output")

# 4. Читаем Avro обратно
df_avro = spark.read.format("avro").load("./benchmark_output/spark_avro_output")

# 5. Column projection + filter
df_filtered = (
    df_avro
    .select("user_id", "event_type", "revenue", "country")
    .filter(col("event_type") == "purchase")
    .filter(col("revenue") > 50.0)
)
print(f"Дорогие покупки: {df_filtered.count()}")

# 6. Агрегация
agg_result = (
    df_avro
    .groupBy("country", "event_type")
    .agg(
        count("*").alias("cnt"),
        spark_round(avg("duration_sec"), 2).alias("avg_duration"),
        spark_round(spark_sum("revenue"), 2).alias("total_revenue")
    )
    .orderBy(col("total_revenue").desc_nulls_last())
)
agg_result.show(20)

# 7. Записываем результат агрегации в Avro
agg_result.write.format("avro") \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .save("./benchmark_output/spark_avro_aggregated")

# 8. Останавливаем SparkSession
spark.stop()
```

---

## 4. Avro vs Parquet в PySpark

### 4.1 Фундаментальные различия

```
                Avro (строчный)                 Parquet (колоночный)
           ┌──────────────────────┐         ┌──────────────────────┐
           │ Row 1: id|name|age   │         │ Col: id              │
           │ Row 2: id|name|age   │         │ 1, 2, 3, 4, 5, ...  │
           │ Row 3: id|name|age   │         │                      │
           │ Row 4: id|name|age   │         │ Col: name            │
           │ Row 5: id|name|age   │         │ A, B, C, D, E, ...   │
           │ ...                  │         │                      │
           └──────────────────────┘         │ Col: age             │
                                            │ 25, 30, 22, 41, ... │
                                            └──────────────────────┘
```

### 4.2 Сравнительная таблица

| Критерий | Avro | Parquet |
|---|---|---|
| **Формат хранения** | Строчный (row-based) | Колоночный (columnar) |
| **Column projection** | Нет (все колонки) | Да (только нужные) |
| **Predicate pushdown** | Нет (no min/max stats) | Да (row group statistics) |
| **Степень сжатия** | Средняя | Высокая (dict encoding, RLE) |
| **Скорость записи** | Быстрая (sequential writes) | Медленнее (columnar buffers) |
| **Скорость чтения (полное)** | Быстрая | Сопоставимая |
| **Скорость чтения (select)** | Как полное | Значительно быстрее |
| **Schema evolution** | Отличная | Ограниченная |
| **Splittable** | Да (по блокам) | Да (по row groups) |
| **Kafka integration** | Нативная | Нет |
| **Вложенные типы** | Хорошая поддержка | Хорошая поддержка |
| **Человекочитаемость** | Нет (бинарный) | Нет (бинарный) |

### 4.3 Когда использовать Avro

Avro -- **лучший выбор**, когда:

1. **Write-heavy нагрузка**: системы, где запись происходит гораздо чаще, чем чтение
   - Логирование событий в реальном времени
   - Сбор метрик с IoT-устройств
   - Запись аудит-логов

2. **Streaming**: потоковая обработка данных
   - Apache Kafka (сообщения в Avro -- стандарт индустрии)
   - Spark Structured Streaming
   - Apache Flink

3. **Schema evolution**: частое изменение схемы данных
   - Микросервисная архитектура, где разные сервисы обновляются независимо
   - A/B тестирование с разными версиями событий
   - Постепенная миграция между версиями API

4. **Межъязыковая совместимость**: данные читаются на Java, Scala, Python, Go
   - Avro имеет библиотеки для всех основных языков
   - Schema Registry обеспечивает единую точку правды о структуре данных

5. **Полное чтение записей**: когда нужны все поля каждой записи
   - Репликация данных между системами
   - Бэкапирование
   - ETL pipelines где все поля нужны на каждом шаге

```python
# Типичный streaming-сценарий: Kafka → Spark → Avro на HDFS
streaming_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "user-events")
    .load()
)

# Десериализация Avro-сообщений из Kafka
from pyspark.sql.avro.functions import from_avro

avro_df = streaming_df.select(
    from_avro(col("value"), avro_schema_json).alias("event")
).select("event.*")

# Запись в Avro на HDFS с партиционированием по дате
avro_df.writeStream \
    .format("avro") \
    .option("compression", "snappy") \
    .option("path", "hdfs:///warehouse/events/") \
    .option("checkpointLocation", "hdfs:///checkpoints/events/") \
    .partitionBy("date") \
    .trigger(processingTime="1 minute") \
    .start()
```

### 4.4 Когда использовать Parquet

Parquet -- **лучший выбор**, когда:

1. **Read-heavy / OLAP нагрузка**: аналитические запросы
   - Dashboards и отчёты
   - Ad-hoc аналитика
   - BI-инструменты (Tableau, Looker, Superset)

2. **Аналитическое хранилище**: долгосрочное хранение данных
   - Data Lake / Data Warehouse
   - Исторические данные для анализа
   - Feature store для ML

3. **Запросы с выборкой колонок**: когда читаются 3-5 колонок из 50+
   - `SELECT user_id, revenue FROM events WHERE country = 'US'`
   - Такой запрос в Parquet прочитает 3 колонки из 10, а в Avro -- все 10

4. **Максимальное сжатие**: когда важна экономия дискового пространства
   - Колоночный формат + dictionary encoding + RLE = лучшее сжатие
   - Типичное соотношение: Parquet zstd ~100 МБ vs Avro snappy ~250 МБ для тех же данных

```python
# Типичный аналитический сценарий: Parquet хранилище
# Parquet оптимален для таких запросов:

# Запрос 1: выбираем 3 колонки из 10 → Parquet читает только 30% данных
df_analytics = (
    spark.read.parquet("./warehouse/events/")
    .select("country", "event_type", "revenue")
    .filter(col("event_type") == "purchase")
    .groupBy("country")
    .agg(spark_sum("revenue").alias("total_revenue"))
    .orderBy(col("total_revenue").desc())
)

# Запрос 2: predicate pushdown → Parquet пропускает row groups
df_us_only = (
    spark.read.parquet("./warehouse/events/")
    .filter(col("country") == "US")  # Parquet проверяет min/max в метаданных
    .filter(col("revenue") > 1000)   # и пропускает целые row groups
)
```

### 4.5 Типичная архитектура: Avro + Parquet вместе

В production-системах Avro и Parquet часто используются **вместе** на разных этапах
data pipeline:

```
                              Data Pipeline
  ┌─────────────────────────────────────────────────────────────────┐
  │                                                                 │
  │   [Producers]     [Kafka]      [Spark Streaming]    [Storage]   │
  │                                                                 │
  │   App Server  →  ┌──────┐  →  ┌──────────────┐  →  ┌────────┐ │
  │   Mobile App  →  │ Avro │  →  │ Read Avro    │  →  │Parquet │ │
  │   IoT Device  →  │ msgs │  →  │ Transform    │  →  │  on    │ │
  │   Web Client  →  └──────┘  →  │ Write Parquet│  →  │ HDFS/  │ │
  │                                └──────────────┘  →  │  S3    │ │
  │                                                     └────────┘ │
  │                                                         │      │
  │                                                         ↓      │
  │                                                   ┌──────────┐ │
  │                                                   │Analytics │ │
  │                                                   │ (Spark,  │ │
  │                                                   │  Presto, │ │
  │                                                   │  Trino)  │ │
  │                                                   └──────────┘ │
  └─────────────────────────────────────────────────────────────────┘

  Avro: быстрая запись, schema evolution, Kafka-совместимость
  Parquet: быстрое чтение, column projection, predicate pushdown
```

Каждый формат используется там, где его сильные стороны наиболее важны:
- **Avro на входе**: приём событий в реальном времени через Kafka
- **Parquet на хранение**: долгосрочное хранение для аналитических запросов
- Spark Structured Streaming выполняет конвертацию автоматически

---

## 5. Интеграция Avro с Kafka в PySpark Structured Streaming

### 5.1 Обзор архитектуры

Apache Kafka -- распределённая шина сообщений, которая является стандартом для
real-time data pipelines. Сообщения в Kafka хранятся как пары `key:value` в бинарном
формате. Avro -- наиболее популярный формат сериализации для value (и иногда для key).

```
Producer (Java/Python)          Kafka Broker              Spark Consumer
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│ Serialize event     │    │ Topic: user-events  │    │ Read from Kafka     │
│ to Avro bytes       │ →  │ Partition 0: [][][]  │ →  │ Deserialize Avro    │
│ Register schema     │    │ Partition 1: [][][]  │    │ Process with Spark  │
│ in Schema Registry  │    │ Partition 2: [][][]  │    │ Write to Parquet    │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
         │                                                      │
         ↓                                                      ↓
┌─────────────────────┐                                ┌─────────────────────┐
│ Confluent Schema    │                                │ HDFS / S3           │
│ Registry            │ ← ── schema lookup ── ── ── → │ Parquet files       │
│ (хранит Avro-схемы) │                                │                     │
└─────────────────────┘                                └─────────────────────┘
```

### 5.2 Чтение Avro-сообщений из Kafka

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.avro.functions import from_avro

# SparkSession с Avro и Kafka
spark = (
    SparkSession.builder
    .appName("Kafka Avro Consumer")
    .master("local[*]")
    .config("spark.jars.packages",
            "org.apache.spark:spark-avro_2.12:3.5.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)

# Avro-схема для десериализации
avro_schema = """
{
    "type": "record",
    "name": "UserEvent",
    "namespace": "com.example.events",
    "fields": [
        {"name": "event_id", "type": "long"},
        {"name": "user_id", "type": "long"},
        {"name": "event_type", "type": "string"},
        {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "revenue", "type": ["null", "double"], "default": null}
    ]
}
"""

# Batch-чтение из Kafka (для тестирования)
kafka_df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "user-events")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

# kafka_df содержит колонки: key, value, topic, partition, offset, timestamp
# value -- это Avro-байты, которые нужно десериализовать

events_df = kafka_df.select(
    from_avro(col("value"), avro_schema).alias("event")
).select("event.*")

events_df.show(5)
# +--------+-------+----------+-------------+-------+
# |event_id|user_id|event_type|    timestamp|revenue|
# +--------+-------+----------+-------------+-------+
# |       1| 456123| page_view|1706745600000|   null|
# |       2| 789456|  purchase|1706745601000|  49.99|
# +--------+-------+----------+-------------+-------+
```

### 5.3 Streaming pipeline: Kafka → Spark → Parquet

```python
# Structured Streaming -- непрерывное чтение из Kafka
streaming_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "user-events")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Десериализация Avro
events_stream = streaming_df.select(
    from_avro(col("value"), avro_schema).alias("event")
).select("event.*")

# Трансформация: добавляем вычисляемые поля
from pyspark.sql.functions import from_unixtime, current_timestamp

processed_stream = (
    events_stream
    .withColumn("event_time", from_unixtime(col("timestamp") / 1000))
    .withColumn("processing_time", current_timestamp())
    .withColumn("is_purchase", col("event_type") == "purchase")
)

# Запись результата в Parquet (Avro → Parquet конвертация)
query = (
    processed_stream.writeStream
    .format("parquet")
    .option("path", "hdfs:///warehouse/events/")
    .option("checkpointLocation", "hdfs:///checkpoints/events/")
    .partitionBy("event_type")
    .trigger(processingTime="30 seconds")
    .outputMode("append")
    .start()
)

# Ожидание завершения (или query.stop() для остановки)
query.awaitTermination()
```

### 5.4 Запись обратно в Kafka в формате Avro

```python
from pyspark.sql.avro.functions import to_avro

output_schema = """
{
    "type": "record",
    "name": "EnrichedEvent",
    "fields": [
        {"name": "user_id", "type": "long"},
        {"name": "event_type", "type": "string"},
        {"name": "revenue", "type": ["null", "double"], "default": null},
        {"name": "is_high_value", "type": "boolean"}
    ]
}
"""

# Обогащаем данные и пишем обратно в Kafka
enriched_stream = (
    events_stream
    .withColumn("is_high_value",
                (col("event_type") == "purchase") & (col("revenue") > 100.0))
    .select(
        col("user_id"),
        to_avro(
            struct("user_id", "event_type", "revenue", "is_high_value"),
            output_schema
        ).alias("value")
    )
)

kafka_output = (
    enriched_stream.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092")
    .option("topic", "enriched-events")
    .option("checkpointLocation", "hdfs:///checkpoints/enriched/")
    .trigger(processingTime="10 seconds")
    .start()
)
```

### 5.5 Confluent Schema Registry интеграция

В production-системах схемы хранятся в Schema Registry, а не хардкодятся:

```python
# Чтение схемы из Schema Registry (через HTTP API)
import requests

def get_avro_schema(registry_url: str, subject: str) -> str:
    """Получить последнюю версию Avro-схемы из Schema Registry."""
    resp = requests.get(f"{registry_url}/subjects/{subject}/versions/latest")
    resp.raise_for_status()
    return resp.json()["schema"]

# Пример использования
registry_url = "http://schema-registry:8081"
avro_schema = get_avro_schema(registry_url, "user-events-value")

# Далее используем как обычно
events_df = kafka_df.select(
    from_avro(col("value"), avro_schema).alias("event")
).select("event.*")
```

---

## 6. Итоги и рекомендации

### 6.1 Ключевые выводы

1. **Avro в чистом Python (fastavro)** -- подходит только для небольших файлов и
   интеграционных задач. Для больших данных -- непрактично из-за построчной обработки
   и отсутствия column projection / predicate pushdown.

2. **Avro в PySpark (spark-avro)** -- эффективный формат благодаря JVM-сериализации
   и распределённой обработке. Является стандартом для Kafka-экосистемы.

3. **Avro vs Parquet** -- это не "или/или", а "и/и". В типичном data pipeline:
   - Avro используется для **приёма данных** (write-heavy, streaming, Kafka)
   - Parquet используется для **хранения и аналитики** (read-heavy, OLAP, column projection)

4. **Schema evolution** -- главное преимущество Avro перед другими форматами.
   Позволяет безболезненно эволюционировать структуру данных в распределённых системах.

### 6.2 Чек-лист выбора формата

```
Нужно ли писать в Kafka?
  → ДА → Avro

Нужна ли schema evolution?
  → ДА → Avro

Данные пишутся чаще, чем читаются?
  → ДА → Avro

Нужна аналитика по отдельным колонкам?
  → ДА → Parquet

Важна максимальная компрессия?
  → ДА → Parquet (zstd)

Данные -- долгосрочное хранилище для BI?
  → ДА → Parquet

Real-time streaming pipeline?
  → Kafka (Avro) → Spark → HDFS/S3 (Parquet)
```

### 6.3 Практическое задание

После изучения теории запустите скрипт `pyspark_benchmark.py`:

```bash
# Быстрый тест (5 млн строк)
python pyspark_benchmark.py --quick

# Полный тест (5 млн строк с подробными метриками)
python pyspark_benchmark.py --num-rows 5000000 --keep

# Сравните результаты Avro в PySpark vs fastavro в benchmark_formats.py
```

Обратите внимание на:
- Насколько быстрее Spark записывает Avro по сравнению с fastavro
- Разницу в размере файлов между Avro и Parquet
- Время чтения с выборкой колонок: Parquet значительно быстрее
- Время записи: Avro часто быстрее Parquet

---

> **Справочные материалы:**
> - [Apache Avro Specification](https://avro.apache.org/docs/current/specification/)
> - [PySpark Avro Data Source Guide](https://spark.apache.org/docs/latest/sql-data-sources-avro.html)
> - [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/)
> - [Spark Structured Streaming + Kafka](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
