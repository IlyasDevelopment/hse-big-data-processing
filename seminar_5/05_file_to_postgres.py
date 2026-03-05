from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from onetl.connection import Postgres, SparkLocalFS
from onetl.db import DBWriter
from onetl.file import FileDFReader, FileDFWriter
from onetl.file.format import CSV
from datetime import date, timedelta
import random

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("OneTL ETL: CSV to Postgres") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("Скрипт 5: ETL Pipeline - CSV → PostgreSQL")
print("=" * 70)

# ============================================================================
# Шаг 1: Создание тестовых CSV данных
# ============================================================================
print("\n" + "=" * 70)
print("Шаг 1: Создание тестового CSV файла")
print("=" * 70)

# Генерируем тестовые данные о продажах
schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", IntegerType(), False),
    StructField("order_date", DateType(), False)
])

# Генерация тестовых данных
products = ["Ноутбук", "Телефон", "Планшет", "Наушники", "Клавиатура", "Мышь"]
sales_data = []

start_date = date(2024, 1, 1)
for i in range(1, 1001):  # 1000 заказов
    order_date = start_date + timedelta(days=random.randint(0, 365))
    sales_data.append((
        i,
        random.randint(1, 200),  # customer_id
        random.choice(products),
        random.randint(1, 10),   # quantity
        random.randint(100, 5000),  # price
        order_date
    ))

df_sales = spark.createDataFrame(sales_data, schema)

print(f"📊 Сгенерировано записей: {df_sales.count()}")
print("\n📋 Примеры данных:")
df_sales.show(10, truncate=False)

# Сохраняем в CSV
csv_path = "/tmp/seminar5_sales.csv"
local_fs = SparkLocalFS(spark=spark)

writer = FileDFWriter(
    connection=local_fs,
    format=CSV(header=True, sep=","),
    target_path=csv_path,
    options={"if_exists": "replace_entire_directory"}
)

print(f"\n💾 Сохраняем в CSV: {csv_path}")
writer.run(df_sales)
print("✓ CSV файл создан")

# ============================================================================
# Шаг 2: Чтение CSV через OneTL
# ============================================================================
print("\n" + "=" * 70)
print("Шаг 2: Чтение CSV файла через OneTL")
print("=" * 70)

reader = FileDFReader(
    connection=local_fs,
    format=CSV(header=True, sep=","),
    source_path=csv_path
)

print("📖 Читаем CSV файл...")
df_from_csv = reader.run()

# Приводим типы данных (CSV читает все как строки)
from pyspark.sql.functions import col

df_from_csv = df_from_csv.select(
    col("order_id").cast("int"),
    col("customer_id").cast("int"),
    col("product").cast("string"),
    col("quantity").cast("int"),
    col("price").cast("int"),
    col("order_date").cast("date")
)

print(f"✓ Прочитано строк: {df_from_csv.count()}")
print("\n📊 Схема данных:")
df_from_csv.printSchema()

# ============================================================================
# Шаг 3: Подключение к PostgreSQL
# ============================================================================
print("\n" + "=" * 70)
print("Шаг 3: Подключение к PostgreSQL")
print("=" * 70)

postgres = Postgres(
    host="localhost",
    port=5433,
    user="student",
    password="bigdata2024",
    database="seminar5",
    spark=spark
)

print("✓ Подключение к PostgreSQL создано")

# ============================================================================
# Шаг 4: Запись данных в PostgreSQL (автоматическое создание таблицы)
# ============================================================================
print("\n" + "=" * 70)
print("Шаг 4: Запись данных в PostgreSQL с автоматическим созданием таблицы")
print("=" * 70)

print("\n💡 Используем mode='overwrite':")
print("   - Spark автоматически создаст таблицу на основе схемы DataFrame")
print("   - Если таблица существует, она будет перезаписана")
print("   - Типы колонок определяются из DataFrame\n")

writer_db = DBWriter(
    connection=postgres,
    target="public.sales",
    options={
        "mode": "overwrite",  # Spark создаст таблицу автоматически
        "batchsize": "1000"   # размер батча для вставки
    }
)

print("💾 Записываем данные в таблицу 'sales'...")
writer_db.run(df_from_csv)
print("✓ Данные успешно записаны")

# Проверяем что записалось
from onetl.db import DBReader

reader_verify = DBReader(connection=postgres, source="public.sales")
df_verify = reader_verify.run()

print(f"\n✓ Записано строк в PostgreSQL: {df_verify.count()}")
print("\n📋 Первые записи из PostgreSQL:")
df_verify.show(10, truncate=False)

# ============================================================================
# Шаг 6: Запись с режимом overwrite
# ============================================================================
print("\n" + "=" * 70)
print("Шаг 6: Перезапись данных (режим: overwrite)")
print("=" * 70)

# Берем только часть данных
df_partial = df_from_csv.filter("order_id <= 100")
print(f"📊 Записываем только {df_partial.count()} строк (order_id <= 100)")

writer_overwrite = DBWriter(
    connection=postgres,
    target="public.sales",
    options={"mode": "overwrite"}  # перезаписываем таблицу
)

print("\n💾 Перезаписываем таблицу...")
writer_overwrite.run(df_partial)

df_after_overwrite = DBReader(connection=postgres, source="public.sales").run()
print(f"✓ После overwrite в таблице: {df_after_overwrite.count()} строк")

# ============================================================================
# Шаг 7: Восстановление полных данных
# ============================================================================
print("\n" + "=" * 70)
print("Шаг 7: Восстановление полных данных")
print("=" * 70)

# Сначала очищаем таблицу
writer_overwrite.run(df_from_csv)
print(f"✓ Данные восстановлены: {DBReader(connection=postgres, source='public.sales').run().count()} строк")

# ============================================================================
# Шаг 8: Запись с партиционированием для ускорения
# ============================================================================
print("\n" + "=" * 70)
print("Шаг 8: Параллельная запись с партиционированием")
print("=" * 70)

print("\n💡 Партиционируем DataFrame для параллельной записи")
print("   - 4 партиции = 4 параллельных потока записи\n")

# Партиционируем DataFrame перед записью
df_repartitioned = df_from_csv.repartition(4)  # 4 партиции для параллельной записи

writer_parallel = DBWriter(
    connection=postgres,
    target="public.sales_parallel",
    options={
        "mode": "overwrite",   # Автоматически создаст таблицу
        "batchsize": "500"
        # Параллелизм обеспечивается через repartition(4) выше
    }
)

print("💾 Записываем данные параллельно (4 потока)...")
writer_parallel.run(df_repartitioned)

df_parallel_verify = DBReader(connection=postgres, source="public.sales_parallel").run()
print(f"✓ Записано строк: {df_parallel_verify.count()}")

# ============================================================================
# Итоговая статистика
# ============================================================================
print("\n" + "=" * 70)
print("📊 Итоговая статистика ETL процесса")
print("=" * 70)

print(f"""
✓ Источник: CSV файл ({csv_path})
✓ Назначение: PostgreSQL (таблицы 'sales', 'sales_parallel')
✓ Обработано строк: {df_from_csv.count()}
✓ Режимы записи: append, overwrite
✓ Использовано параллельной записи: 4 потока
""")

# Очистка
spark.stop()
print("\n ETL процесс завершен успешно!")
