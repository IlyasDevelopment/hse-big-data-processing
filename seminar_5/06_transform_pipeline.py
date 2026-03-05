from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, when, lit, round, year, month
from pyspark.sql.window import Window
from onetl.connection import Postgres
from onetl.db import DBReader, DBWriter
import psycopg2

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("OneTL ETL with Transformations") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("Скрипт 6: ETL с трансформациями данных")
print("=" * 70)

# ============================================================================
# Подключение к PostgreSQL
# ============================================================================
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
# EXTRACT: Чтение данных из таблицы sales
# ============================================================================
print("\n" + "=" * 70)
print("EXTRACT: Чтение данных о продажах")
print("=" * 70)

reader = DBReader(
    connection=postgres,
    source="public.sales"
)

print("📖 Читаем таблицу 'sales'...")
df_sales = reader.run()

print(f"✓ Прочитано строк: {df_sales.count()}")
print("\n📊 Исходные данные:")
df_sales.show(10, truncate=False)

# ============================================================================
# TRANSFORM: Применяем трансформации
# ============================================================================
print("\n" + "=" * 70)
print("TRANSFORM: Применяем трансформации")
print("=" * 70)

# Трансформация 1: Добавляем вычисляемое поле total_amount
print("\n1️⃣ Добавляем поле total_amount (quantity * price)...")
df_transformed = df_sales.withColumn(
    "total_amount",
    col("quantity") * col("price")
)

print("✓ Поле добавлено")
df_transformed.select("order_id", "product", "quantity", "price", "total_amount").show(5)

# Трансформация 2: Извлекаем год и месяц из даты
print("\n2️⃣ Извлекаем год и месяц из order_date...")
df_transformed = df_transformed.withColumn("order_year", year(col("order_date"))) \
                               .withColumn("order_month", month(col("order_date")))

print("✓ Поля добавлены")
df_transformed.select("order_id", "order_date", "order_year", "order_month").show(5)

# Трансформация 3: Категоризация заказов по размеру
print("\n3️⃣ Категоризация заказов по размеру суммы...")
df_transformed = df_transformed.withColumn(
    "order_category",
    when(col("total_amount") < 500, "Малый")
    .when((col("total_amount") >= 500) & (col("total_amount") < 2000), "Средний")
    .when((col("total_amount") >= 2000) & (col("total_amount") < 5000), "Большой")
    .otherwise("Очень большой")
)

print("✓ Категории добавлены")
df_transformed.groupBy("order_category").count().show()

# Трансформация 4: Агрегация - статистика по продуктам
print("\n4️⃣ Агрегация: статистика по продуктам...")
df_product_stats = df_transformed.groupBy("product").agg(
    count("order_id").alias("order_count"),
    sum("quantity").alias("total_quantity"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_order_amount"),
    round(avg("price"), 2).alias("avg_price")
).orderBy(col("total_revenue").desc())

print("✓ Агрегация выполнена")
print("\n📊 Статистика по продуктам:")
df_product_stats.show(truncate=False)

# Трансформация 5: Агрегация - статистика по месяцам
print("\n5️⃣ Агрегация: статистика по месяцам...")
df_monthly_stats = df_transformed.groupBy("order_year", "order_month").agg(
    count("order_id").alias("order_count"),
    sum("total_amount").alias("monthly_revenue"),
    avg("total_amount").alias("avg_order_amount")
).orderBy("order_year", "order_month")

print("✓ Агрегация выполнена")
print("\n📊 Статистика по месяцам:")
df_monthly_stats.show(truncate=False)

# Трансформация 6: Window функции - ранжирование продуктов по выручке
print("\n6️⃣ Window функции: топ продуктов по выручке...")
from pyspark.sql.functions import row_number

window_spec = Window.orderBy(col("total_revenue").desc())
df_product_ranked = df_product_stats.withColumn(
    "rank",
    row_number().over(window_spec)
)

print("✓ Ранжирование выполнено")
print("\n📊 Топ-5 продуктов по выручке:")
df_product_ranked.filter(col("rank") <= 5).show(truncate=False)

# Трансформация 7: Join - обогащаем данные информацией о клиентах
print("\n7️⃣ Join: обогащаем заказы информацией о клиентах...")

# Читаем таблицу пользователей
df_users = DBReader(connection=postgres, source="public.users").run()

# Переименовываем customer_id в id для джойна
df_sales_enriched = df_transformed.join(
    df_users.select(
        col("id").alias("customer_id"),
        col("name").alias("customer_name"),
        col("city").alias("customer_city")
    ),
    on="customer_id",
    how="left"
)

print("✓ Join выполнен")
print("\n📊 Обогащенные данные:")
df_sales_enriched.select(
    "order_id", "customer_name", "customer_city", "product", "total_amount"
).show(10, truncate=False)

# ============================================================================
# LOAD: Сохраняем результаты трансформаций в новые таблицы
# ============================================================================
print("\n" + "=" * 70)
print("LOAD: Сохраняем результаты в PostgreSQL")
print("=" * 70)

# Создаем таблицы для результатов
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    user="student",
    password="bigdata2024",
    database="seminar5"
)
cursor = conn.cursor()

# Таблица для обогащенных продаж
cursor.execute("DROP TABLE IF EXISTS sales_enriched")
cursor.execute("""
    CREATE TABLE sales_enriched (
        order_id INTEGER,
        customer_id INTEGER,
        customer_name VARCHAR(100),
        customer_city VARCHAR(100),
        product VARCHAR(100),
        quantity INTEGER,
        price INTEGER,
        total_amount BIGINT,
        order_date DATE,
        order_year INTEGER,
        order_month INTEGER,
        order_category VARCHAR(50)
    )
""")

# Таблица для статистики по продуктам
cursor.execute("DROP TABLE IF EXISTS product_statistics")
cursor.execute("""
    CREATE TABLE product_statistics (
        product VARCHAR(100) PRIMARY KEY,
        order_count BIGINT,
        total_quantity BIGINT,
        total_revenue BIGINT,
        avg_order_amount DOUBLE PRECISION,
        avg_price DOUBLE PRECISION,
        rank INTEGER
    )
""")

# Таблица для статистики по месяцам
cursor.execute("DROP TABLE IF EXISTS monthly_statistics")
cursor.execute("""
    CREATE TABLE monthly_statistics (
        order_year INTEGER,
        order_month INTEGER,
        order_count BIGINT,
        monthly_revenue BIGINT,
        avg_order_amount DOUBLE PRECISION,
        PRIMARY KEY (order_year, order_month)
    )
""")

conn.commit()
cursor.close()
conn.close()

print("✓ Таблицы для результатов созданы")

# Загрузка 1: Обогащенные продажи
print("\n1️⃣ Загружаем sales_enriched...")
writer1 = DBWriter(
    connection=postgres,
    target="public.sales_enriched",
    options={"mode": "append"}
)

df_sales_enriched_final = df_sales_enriched.select(
    "order_id", "customer_id", "customer_name", "customer_city",
    "product", "quantity", "price", "total_amount",
    "order_date", "order_year", "order_month", "order_category"
)

writer1.run(df_sales_enriched_final)
print(f"✓ Загружено {df_sales_enriched_final.count()} записей")

# Загрузка 2: Статистика по продуктам
print("\n2️⃣ Загружаем product_statistics...")
writer2 = DBWriter(
    connection=postgres,
    target="public.product_statistics",
    options={"mode": "append"}
)
writer2.run(df_product_ranked)
print(f"✓ Загружено {df_product_ranked.count()} записей")

# Загрузка 3: Статистика по месяцам
print("\n3️⃣ Загружаем monthly_statistics...")
writer3 = DBWriter(
    connection=postgres,
    target="public.monthly_statistics",
    options={"mode": "append"}
)
writer3.run(df_monthly_stats)
print(f"✓ Загружено {df_monthly_stats.count()} записей")

# ============================================================================
# Проверка результатов
# ============================================================================
print("\n" + "=" * 70)
print(" Проверка загруженных данных")
print("=" * 70)

# Проверяем каждую таблицу
tables = ["public.sales_enriched", "public.product_statistics", "public.monthly_statistics"]

for table in tables:
    df_check = DBReader(connection=postgres, source=table).run()
    table_name = table.replace("public.", "")  # для красивого вывода
    print(f"\n📊 Таблица '{table_name}': {df_check.count()} записей")
    df_check.show(5, truncate=False)

# ============================================================================
# Итоговая статистика
# ============================================================================
print("\n" + "=" * 70)
print("📊 Итоговая статистика ETL процесса")
print("=" * 70)

print(f"""
✓ EXTRACT: Прочитано {df_sales.count()} заказов из таблицы 'sales'
✓ TRANSFORM: Применено 7 типов трансформаций:
   - Вычисляемые поля (total_amount)
   - Извлечение компонентов даты (year, month)
   - Категоризация (order_category)
   - Агрегация по продуктам
   - Агрегация по месяцам
   - Window функции (ранжирование)
   - Join с таблицей пользователей
✓ LOAD: Загружено в 3 таблицы:
   - sales_enriched: {df_sales_enriched_final.count()} записей
   - product_statistics: {df_product_ranked.count()} записей
   - monthly_statistics: {df_monthly_stats.count()} записей
""")

# Очистка
spark.stop()
print("\n ETL процесс с трансформациями завершен успешно!")
