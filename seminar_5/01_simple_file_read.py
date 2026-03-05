"""
Скрипт 1: Простое чтение файлов через OneTL
Уровень: Beginner

Демонстрирует:
- Создание Spark сессии
- Подключение к локальной файловой системе
- Чтение CSV файла в DataFrame
- Базовые операции с данными
"""

from pyspark.sql import SparkSession
from onetl.connection import SparkLocalFS
from onetl.file.format import CSV

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("OneTL Simple File Read") \
    .master("local[*]") \
    .getOrCreate()

# Устанавливаем уровень логирования
spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("Скрипт 1: Чтение файлов через OneTL FileDFConnection")
print("=" * 70)

# Создаем подключение к локальной файловой системе
local_fs = SparkLocalFS(spark=spark)

print(f"\n✓ Подключение к локальной ФС создано: {local_fs}")

# Создаем файл для примера
sample_data_path = "/Users/mvliksakov/projects/big_data_processing/seminar_5/seminar5_sample.csv"

# Создаем тестовые данные
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), False),
    StructField("city", StringType(), True)
])

sample_data = [
    (1, "Иван", 25, "Москва"),
    (2, "Мария", 30, "Санкт-Петербург"),
    (3, "Петр", 35, "Новосибирск"),
    (4, "Анна", 28, "Екатеринбург"),
    (5, "Дмитрий", 42, "Казань")
]

df_sample = spark.createDataFrame(sample_data, schema)

# Записываем тестовые данные
print(f"\n📝 Создаем тестовый CSV файл: {sample_data_path}")
df_sample.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(sample_data_path)

# Теперь читаем через OneTL
from onetl.file import FileDFReader

reader = FileDFReader(
    connection=local_fs,
    format=CSV(header=True, sep=","),
    source_path=sample_data_path
)

print(f"\n📖 Читаем данные через OneTL FileDFReader...")
df = reader.run()

print(f"\n✓ Прочитано строк: {df.count()}")
print("\n📊 Схема данных (до приведения типов):")
df.printSchema()

# CSV читает все как строки, приводим типы данных
from pyspark.sql.functions import col

df = df.select(
    col("id").cast("integer"),
    col("name").cast("string"),
    col("age").cast("integer"),
    col("city").cast("string")
)

print("\n📊 Схема данных (после приведения типов):")
df.printSchema()

print("\n📋 Первые 5 записей:")
df.show()

# Простая агрегация (теперь age - integer, можно использовать avg)
print("\n📈 Средний возраст по городам:")
df.groupBy("city").avg("age").show(truncate=False)

# Очистка
spark.stop()
print("\n Скрипт завершен успешно!")
