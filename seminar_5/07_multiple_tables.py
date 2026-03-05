from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, coalesce, lit
from onetl.connection import Postgres
from onetl.db import DBReader, DBWriter
import psycopg2

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("OneTL Multiple Tables") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("Скрипт 7: Работа с несколькими таблицами")
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

# ============================================================================
# Создание дополнительных тестовых таблиц
# ============================================================================
print("\n" + "=" * 70)
print("Подготовка: Создание дополнительных таблиц")
print("=" * 70)

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    user="student",
    password="bigdata2024",
    database="seminar5"
)
cursor = conn.cursor()

# Таблица категорий продуктов
cursor.execute("DROP TABLE IF EXISTS product_categories")
cursor.execute("""
    CREATE TABLE product_categories (
        product VARCHAR(100) PRIMARY KEY,
        category VARCHAR(50),
        supplier VARCHAR(100),
        warranty_months INTEGER
    )
""")

# Вставляем данные о категориях
categories_data = [
    ('Ноутбук', 'Компьютеры', 'TechCorp', 24),
    ('Телефон', 'Мобильные устройства', 'PhoneInc', 12),
    ('Планшет', 'Мобильные устройства', 'TabletPro', 12),
    ('Наушники', 'Аксессуары', 'AudioMax', 6),
    ('Клавиатура', 'Аксессуары', 'PeripheralsCo', 12),
    ('Мышь', 'Аксессуары', 'PeripheralsCo', 12)
]

for product, category, supplier, warranty in categories_data:
    cursor.execute(
        "INSERT INTO product_categories VALUES (%s, %s, %s, %s)",
        (product, category, supplier, warranty)
    )

# Таблица регионов для городов
cursor.execute("DROP TABLE IF EXISTS city_regions")
cursor.execute("""
    CREATE TABLE city_regions (
        city VARCHAR(100) PRIMARY KEY,
        region VARCHAR(100),
        population INTEGER
    )
""")

# Вставляем данные о регионах
regions_data = [
    ('Москва', 'Центральный', 13000000),
    ('Санкт-Петербург', 'Северо-Западный', 5400000),
    ('Новосибирск', 'Сибирский', 1600000),
    ('Екатеринбург', 'Уральский', 1500000),
    ('Казань', 'Приволжский', 1300000)
]

for city, region, population in regions_data:
    cursor.execute(
        "INSERT INTO city_regions VALUES (%s, %s, %s)",
        (city, region, population)
    )

conn.commit()
cursor.close()
conn.close()

print("✓ Созданы таблицы:")
print("  - product_categories (категории продуктов)")
print("  - city_regions (регионы городов)")

# ============================================================================
# Чтение всех таблиц
# ============================================================================
print("\n" + "=" * 70)
print("Чтение данных из всех таблиц")
print("=" * 70)

# Читаем все таблицы параллельно
tables_to_read = {
    "sales": "public.sales",
    "users": "public.users",
    "product_categories": "public.product_categories",
    "city_regions": "public.city_regions"
}

dataframes = {}

for name, table in tables_to_read.items():
    print(f"\n📖 Читаем таблицу '{table}'...")
    reader = DBReader(connection=postgres, source=table)
    df = reader.run()
    dataframes[name] = df
    print(f"   ✓ Прочитано {df.count()} записей")

# ============================================================================
# JOIN операции
# ============================================================================
print("\n" + "=" * 70)
print("JOIN операции: объединение данных из разных таблиц")
print("=" * 70)

# JOIN 1: Sales + Product Categories (INNER JOIN)
print("\n1️⃣ INNER JOIN: Продажи + Категории продуктов")
df_sales_with_categories = dataframes["sales"].join(
    dataframes["product_categories"],
    on="product",
    how="inner"
)

print(f"   ✓ Результат: {df_sales_with_categories.count()} записей")
df_sales_with_categories.select(
    "order_id", "product", "category", "supplier", "warranty_months", "price"
).show(5, truncate=False)

# JOIN 2: Sales + Users (LEFT JOIN)
print("\n2️⃣ LEFT JOIN: Продажи + Пользователи")
df_sales_with_users = dataframes["sales"].join(
    dataframes["users"].select(
        col("id").alias("customer_id"),
        col("name").alias("customer_name"),
        col("city").alias("customer_city"),
        col("age").alias("customer_age")
    ),
    on="customer_id",
    how="left"
)

print(f"   ✓ Результат: {df_sales_with_users.count()} записей")
df_sales_with_users.select(
    "order_id", "customer_name", "customer_city", "customer_age", "product", "price"
).show(5, truncate=False)

# JOIN 3: Множественный JOIN (Sales + Users + Regions + Categories)
print("\n3️⃣ Множественный JOIN: Все таблицы вместе")

# Начинаем с sales
df_complete = dataframes["sales"]

# Добавляем информацию о пользователях
df_complete = df_complete.join(
    dataframes["users"].select(
        col("id").alias("customer_id"),
        col("name").alias("customer_name"),
        col("city").alias("customer_city"),
        col("age").alias("customer_age")
    ),
    on="customer_id",
    how="left"
)

# Добавляем информацию о категориях продуктов
df_complete = df_complete.join(
    dataframes["product_categories"],
    on="product",
    how="left"
)

# Добавляем информацию о регионах
df_complete = df_complete.join(
    dataframes["city_regions"].select(
        col("city").alias("customer_city"),
        col("region").alias("customer_region"),
        col("population").alias("city_population")
    ),
    on="customer_city",
    how="left"
)

# Добавляем вычисляемое поле total_amount
df_complete = df_complete.withColumn(
    "total_amount",
    col("quantity") * col("price")
)

print(f"   ✓ Результат: {df_complete.count()} записей")
print("\n📊 Полная витрина данных:")
df_complete.select(
    "order_id", "customer_name", "customer_city", "customer_region",
    "product", "category", "supplier", "quantity", "price", "total_amount"
).show(10, truncate=False)

# ============================================================================
# Аналитика на основе множественных таблиц
# ============================================================================
print("\n" + "=" * 70)
print("Аналитика на основе объединенных данных")
print("=" * 70)

# Анализ 1: Продажи по регионам
print("\n1️⃣ Продажи по регионам:")
df_regional_sales = df_complete.groupBy("customer_region").agg(
    count("order_id").alias("order_count"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_order_amount")
).orderBy(col("total_revenue").desc())

df_regional_sales.show(truncate=False)

# Анализ 2: Продажи по категориям продуктов
print("\n2️⃣ Продажи по категориям продуктов:")
df_category_sales = df_complete.groupBy("category").agg(
    count("order_id").alias("order_count"),
    sum("quantity").alias("total_units"),
    sum("total_amount").alias("total_revenue")
).orderBy(col("total_revenue").desc())

df_category_sales.show(truncate=False)

# Анализ 3: Продажи по поставщикам
print("\n3️⃣ Продажи по поставщикам:")
df_supplier_sales = df_complete.groupBy("supplier").agg(
    count("order_id").alias("order_count"),
    sum("total_amount").alias("total_revenue"),
    avg("warranty_months").alias("avg_warranty")
).orderBy(col("total_revenue").desc())

df_supplier_sales.show(truncate=False)

# Анализ 4: Возрастные группы покупателей по категориям
print("\n4️⃣ Возрастные группы покупателей по категориям:")

from pyspark.sql.functions import when

df_age_categories = df_complete.withColumn(
    "age_group",
    when(col("customer_age") < 25, "18-24")
    .when((col("customer_age") >= 25) & (col("customer_age") < 35), "25-34")
    .when((col("customer_age") >= 35) & (col("customer_age") < 45), "35-44")
    .otherwise("45+")
)

df_age_analysis = df_age_categories.groupBy("age_group", "category").agg(
    count("order_id").alias("order_count"),
    sum("total_amount").alias("revenue")
).orderBy("age_group", col("revenue").desc())

df_age_analysis.show(20, truncate=False)

# ============================================================================
# Сохранение витрины данных
# ============================================================================
print("\n" + "=" * 70)
print("Сохранение витрины данных в PostgreSQL")
print("=" * 70)

# Создаем таблицу для полной витрины данных
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    user="student",
    password="bigdata2024",
    database="seminar5"
)
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS sales_data_mart")
cursor.execute("""
    CREATE TABLE sales_data_mart (
        order_id INTEGER,
        order_date DATE,
        customer_id INTEGER,
        customer_name VARCHAR(100),
        customer_city VARCHAR(100),
        customer_region VARCHAR(100),
        customer_age INTEGER,
        city_population INTEGER,
        product VARCHAR(100),
        category VARCHAR(50),
        supplier VARCHAR(100),
        warranty_months INTEGER,
        quantity INTEGER,
        price INTEGER,
        total_amount BIGINT
    )
""")

conn.commit()
cursor.close()
conn.close()

print("✓ Таблица 'sales_data_mart' создана")

# Сохраняем витрину данных
print("\n💾 Загружаем витрину данных...")

df_mart = df_complete.select(
    "order_id", "order_date", "customer_id", "customer_name",
    "customer_city", "customer_region", "customer_age", "city_population",
    "product", "category", "supplier", "warranty_months",
    "quantity", "price", "total_amount"
)

writer = DBWriter(
    connection=postgres,
    target="public.sales_data_mart",
    options={"mode": "append"}
)

writer.run(df_mart)
print(f"✓ Загружено {df_mart.count()} записей")

# Проверка
print("\n📊 Проверка витрины данных:")
df_verify = DBReader(connection=postgres, source="public.sales_data_mart").run()
df_verify.show(5, truncate=False)

# ============================================================================
# Итоговая статистика
# ============================================================================
print("\n" + "=" * 70)
print("📊 Итоговая статистика")
print("=" * 70)

print(f"""
✓ Обработано таблиц: 4
  - sales: {dataframes['sales'].count()} записей
  - users: {dataframes['users'].count()} записей
  - product_categories: {dataframes['product_categories'].count()} записей
  - city_regions: {dataframes['city_regions'].count()} записей

✓ Выполнено JOIN операций: 4
  - INNER JOIN (sales + categories)
  - LEFT JOIN (sales + users)
  - Множественный JOIN (все таблицы)
  - Дополнительный JOIN с регионами

✓ Создана витрина данных: sales_data_mart
  - Записей: {df_mart.count()}
  - Колонок: {len(df_mart.columns)}
  - Источников данных: 4 таблицы
""")

# Очистка
spark.stop()
print("\n Работа с множественными таблицами завершена успешно!")
