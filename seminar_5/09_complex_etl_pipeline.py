from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count as spark_count, avg, when, lit, coalesce,
    current_timestamp, date_format, year, month, dayofmonth, countDistinct
)
from onetl.connection import Postgres, SparkLocalFS
from onetl.db import DBReader, DBWriter
from onetl.file import FileDFWriter
from onetl.file.format import Parquet, JSON
import psycopg2
from datetime import datetime

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("OneTL Complex ETL Pipeline") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================================================================
# Класс для логирования ETL процесса
# ============================================================================
class ETLLogger:
    """Логгер для отслеживания этапов ETL процесса"""

    def __init__(self, pipeline_name):
        self.pipeline_name = pipeline_name
        self.start_time = datetime.now()
        self.steps = []

    def log_step(self, step_name, status, records_count=None, error=None):
        """Логирует шаг ETL процесса"""
        step_info = {
            'timestamp': datetime.now(),
            'step': step_name,
            'status': status,
            'records': records_count,
            'error': error
        }
        self.steps.append(step_info)

        status_icon = "✓" if status == "SUCCESS" else "✗"
        records_info = f" ({records_count} записей)" if records_count else ""
        error_info = f" - {error}" if error else ""

        print(f"{status_icon} {step_name}{records_info}{error_info}")

    def summary(self):
        """Выводит итоговую статистику"""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()

        success_steps = sum(1 for s in self.steps if s['status'] == 'SUCCESS')
        failed_steps = sum(1 for s in self.steps if s['status'] == 'FAILED')

        print("\n" + "=" * 70)
        print(f"📊 Итоговая статистика: {self.pipeline_name}")
        print("=" * 70)
        print(f"Начало: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Окончание: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Длительность: {duration:.2f} секунд")
        print(f"Успешных шагов: {success_steps}")
        print(f"Неудачных шагов: {failed_steps}")
        print("\nДетали шагов:")

        for step in self.steps:
            status_icon = "✓" if step['status'] == 'SUCCESS' else "✗"
            records_info = f" - {step['records']} записей" if step['records'] else ""
            print(f"  {status_icon} {step['step']}{records_info}")


print("=" * 70)
print("Скрипт 9: Комплексный ETL Pipeline")
print("=" * 70)

# Инициализируем логгер
logger = ETLLogger("Complex Multi-Source ETL Pipeline")

# ============================================================================
# Подключения
# ============================================================================
print("\n" + "=" * 70)
print("Инициализация подключений")
print("=" * 70)

postgres = Postgres(
    host="localhost",
    port=5433,
    user="student",
    password="bigdata2024",
    database="seminar5",
    spark=spark
)

local_fs = SparkLocalFS(spark=spark)

logger.log_step("Создание подключений к источникам и назначениям", "SUCCESS")

# ============================================================================
# EXTRACT: Чтение из множества источников
# ============================================================================
print("\n" + "=" * 70)
print("EXTRACT: Чтение данных из источников")
print("=" * 70)

# Источник 1: Продажи
try:
    reader_sales = DBReader(connection=postgres, source="public.sales")
    df_sales = reader_sales.run()
    logger.log_step("Чтение таблицы 'sales'", "SUCCESS", df_sales.count())
except Exception as e:
    logger.log_step("Чтение таблицы 'sales'", "FAILED", error=str(e))
    raise

# Источник 2: Пользователи
try:
    reader_users = DBReader(connection=postgres, source="public.users")
    df_users = reader_users.run()
    logger.log_step("Чтение таблицы 'users'", "SUCCESS", df_users.count())
except Exception as e:
    logger.log_step("Чтение таблицы 'users'", "FAILED", error=str(e))
    raise

# Источник 3: Категории продуктов
try:
    reader_categories = DBReader(connection=postgres, source="public.product_categories")
    df_categories = reader_categories.run()
    logger.log_step("Чтение таблицы 'product_categories'", "SUCCESS", df_categories.count())
except Exception as e:
    logger.log_step("Чтение таблицы 'product_categories'", "FAILED", error=str(e))
    raise

# Источник 4: Регионы
try:
    reader_regions = DBReader(connection=postgres, source="public.city_regions")
    df_regions = reader_regions.run()
    logger.log_step("Чтение таблицы 'city_regions'", "SUCCESS", df_regions.count())
except Exception as e:
    logger.log_step("Чтение таблицы 'city_regions'", "FAILED", error=str(e))
    raise

# ============================================================================
# VALIDATE: Валидация данных
# ============================================================================
print("\n" + "=" * 70)
print("VALIDATE: Проверка качества данных")
print("=" * 70)

# Проверка 1: Нет NULL в обязательных полях
null_check_sales = df_sales.filter(
    col("order_id").isNull() |
    col("customer_id").isNull() |
    col("product").isNull()
).count()

if null_check_sales > 0:
    logger.log_step("Проверка NULL в sales", "FAILED",
                   error=f"Найдено {null_check_sales} записей с NULL")
else:
    logger.log_step("Проверка NULL в sales", "SUCCESS")

# Проверка 2: Диапазон значений
invalid_amounts = df_sales.filter(
    (col("price") < 0) | (col("quantity") < 0)
).count()

if invalid_amounts > 0:
    logger.log_step("Проверка диапазонов значений", "FAILED",
                   error=f"Найдено {invalid_amounts} записей с некорректными значениями")
else:
    logger.log_step("Проверка диапазонов значений", "SUCCESS")

# Проверка 3: Referential Integrity
# Проверяем, что все customer_id из sales существуют в users
sales_customer_ids = df_sales.select("customer_id").distinct()
users_ids = df_users.select(col("id").alias("customer_id")).distinct()

orphan_customers = sales_customer_ids.join(
    users_ids,
    on="customer_id",
    how="left_anti"
).count()

if orphan_customers > 0:
    logger.log_step("Проверка referential integrity", "FAILED",
                   error=f"Найдено {orphan_customers} заказов с несуществующими клиентами")
    # Можем либо отфильтровать, либо создать "Unknown" клиента
else:
    logger.log_step("Проверка referential integrity", "SUCCESS")

# ============================================================================
# TRANSFORM: Комплексные трансформации
# ============================================================================
print("\n" + "=" * 70)
print("TRANSFORM: Применение комплексных трансформаций")
print("=" * 70)

# Трансформация 1: Обогащение продаж информацией о клиентах
df_enriched = df_sales.join(
    df_users.select(
        col("id").alias("customer_id"),
        col("name").alias("customer_name"),
        col("city").alias("customer_city"),
        col("age").alias("customer_age"),
        col("email").alias("customer_email")
    ),
    on="customer_id",
    how="left"
)

logger.log_step("JOIN: sales + users", "SUCCESS", df_enriched.count())

# Трансформация 2: Добавление информации о категориях
df_enriched = df_enriched.join(
    df_categories.select(
        "product",
        col("category").alias("product_category"),
        col("supplier").alias("product_supplier")
    ),
    on="product",
    how="left"
)

logger.log_step("JOIN: + product_categories", "SUCCESS", df_enriched.count())

# Трансформация 3: Добавление информации о регионах
df_enriched = df_enriched.join(
    df_regions.select(
        col("city").alias("customer_city"),
        col("region").alias("customer_region")
    ),
    on="customer_city",
    how="left"
)

logger.log_step("JOIN: + city_regions", "SUCCESS", df_enriched.count())

# Трансформация 4: Вычисляемые поля
df_enriched = df_enriched.withColumn(
    "total_amount",
    col("quantity") * col("price")
).withColumn(
    "order_year",
    year(col("order_date"))
).withColumn(
    "order_month",
    month(col("order_date"))
).withColumn(
    "order_day",
    dayofmonth(col("order_date"))
).withColumn(
    "order_date_formatted",
    date_format(col("order_date"), "yyyy-MM-dd")
).withColumn(
    "etl_loaded_at",
    current_timestamp()
)

logger.log_step("Добавление вычисляемых полей", "SUCCESS", df_enriched.count())

# Трансформация 5: Категоризация клиентов
df_enriched = df_enriched.withColumn(
    "customer_segment",
    when(col("customer_age") < 25, "Молодежь")
    .when((col("customer_age") >= 25) & (col("customer_age") < 40), "Средний возраст")
    .when((col("customer_age") >= 40) & (col("customer_age") < 60), "Зрелый возраст")
    .otherwise("Пожилой возраст")
)

logger.log_step("Категоризация клиентов по возрасту", "SUCCESS")

# Трансформация 6: Обработка NULL значений
df_enriched = df_enriched.withColumn(
    "customer_region",
    coalesce(col("customer_region"), lit("Неизвестный регион"))
).withColumn(
    "product_category",
    coalesce(col("product_category"), lit("Без категории"))
)

logger.log_step("Обработка NULL значений", "SUCCESS")

# ============================================================================
# AGGREGATE: Создание агрегированных витрин
# ============================================================================
print("\n" + "=" * 70)
print("AGGREGATE: Создание аналитических витрин")
print("=" * 70)

# Витрина 1: Агрегация по клиентам
df_customer_summary = df_enriched.groupBy(
    "customer_id", "customer_name", "customer_city",
    "customer_region", "customer_segment"
).agg(
    spark_count("order_id").alias("total_orders"),
    spark_sum("quantity").alias("total_items"),
    spark_sum("total_amount").alias("total_spent"),
    avg("total_amount").alias("avg_order_amount"),
    spark_count(when(col("order_year") == 2024, True)).alias("orders_2024")
)

logger.log_step("Создание витрины: customer_summary", "SUCCESS", df_customer_summary.count())

# Витрина 2: Агрегация по продуктам и регионам
df_product_region = df_enriched.groupBy(
    "product", "product_category", "customer_region"
).agg(
    spark_count("order_id").alias("order_count"),
    spark_sum("quantity").alias("quantity_sold"),
    spark_sum("total_amount").alias("revenue")
).orderBy(col("revenue").desc())

logger.log_step("Создание витрины: product_region_summary", "SUCCESS", df_product_region.count())

# Витрина 3: Временная агрегация (по месяцам)
df_monthly = df_enriched.groupBy(
    "order_year", "order_month"
).agg(
    spark_count("order_id").alias("monthly_orders"),
    spark_sum("total_amount").alias("monthly_revenue"),
    countDistinct("customer_id").alias("unique_customers")
).orderBy("order_year", "order_month")

logger.log_step("Создание витрины: monthly_summary", "SUCCESS", df_monthly.count())

# ============================================================================
# LOAD: Загрузка в множество назначений
# ============================================================================
print("\n" + "=" * 70)
print("LOAD: Загрузка результатов в назначения")
print("=" * 70)

# Создаем таблицы в PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    user="student",
    password="bigdata2024",
    database="seminar5"
)
cursor = conn.cursor()

# Таблица для обогащенных продаж
cursor.execute("DROP TABLE IF EXISTS sales_fact_table")
cursor.execute("""
    CREATE TABLE sales_fact_table (
        order_id INTEGER,
        customer_id INTEGER,
        customer_name VARCHAR(100),
        customer_city VARCHAR(100),
        customer_region VARCHAR(100),
        customer_age INTEGER,
        customer_segment VARCHAR(50),
        product VARCHAR(100),
        product_category VARCHAR(50),
        product_supplier VARCHAR(100),
        quantity INTEGER,
        price INTEGER,
        total_amount BIGINT,
        order_date DATE,
        order_year INTEGER,
        order_month INTEGER,
        etl_loaded_at TIMESTAMP
    )
""")

# Таблица для customer summary
cursor.execute("DROP TABLE IF EXISTS customer_summary")
cursor.execute("""
    CREATE TABLE customer_summary (
        customer_id INTEGER PRIMARY KEY,
        customer_name VARCHAR(100),
        customer_city VARCHAR(100),
        customer_region VARCHAR(100),
        customer_segment VARCHAR(50),
        total_orders BIGINT,
        total_items BIGINT,
        total_spent BIGINT,
        avg_order_amount DOUBLE PRECISION,
        orders_2024 BIGINT
    )
""")

# Таблица для product-region summary
cursor.execute("DROP TABLE IF EXISTS product_region_summary")
cursor.execute("""
    CREATE TABLE product_region_summary (
        product VARCHAR(100),
        product_category VARCHAR(50),
        customer_region VARCHAR(100),
        order_count BIGINT,
        quantity_sold BIGINT,
        revenue BIGINT,
        PRIMARY KEY (product, customer_region)
    )
""")

conn.commit()
cursor.close()
conn.close()

logger.log_step("Создание таблиц в PostgreSQL", "SUCCESS")

# Назначение 1: Полная таблица фактов в PostgreSQL
try:
    writer_fact = DBWriter(
        connection=postgres,
        target="public.sales_fact_table",
        options={"mode": "append"}
    )

    df_fact = df_enriched.select(
        "order_id", "customer_id", "customer_name", "customer_city",
        "customer_region", "customer_age", "customer_segment",
        "product", "product_category", "product_supplier",
        "quantity", "price", "total_amount",
        "order_date", "order_year", "order_month", "etl_loaded_at"
    )

    writer_fact.run(df_fact)
    logger.log_step("Загрузка в PostgreSQL: sales_fact_table", "SUCCESS", df_fact.count())
except Exception as e:
    logger.log_step("Загрузка в PostgreSQL: sales_fact_table", "FAILED", error=str(e))

# Назначение 2: Customer summary в PostgreSQL
try:
    writer_customer = DBWriter(
        connection=postgres,
        target="public.customer_summary",
        options={"mode": "append"}
    )
    writer_customer.run(df_customer_summary)
    logger.log_step("Загрузка в PostgreSQL: customer_summary", "SUCCESS", df_customer_summary.count())
except Exception as e:
    logger.log_step("Загрузка в PostgreSQL: customer_summary", "FAILED", error=str(e))

# Назначение 3: Product-Region summary в PostgreSQL
try:
    writer_product = DBWriter(
        connection=postgres,
        target="public.product_region_summary",
        options={"mode": "append"}
    )
    writer_product.run(df_product_region)
    logger.log_step("Загрузка в PostgreSQL: product_region_summary", "SUCCESS", df_product_region.count())
except Exception as e:
    logger.log_step("Загрузка в PostgreSQL: product_region_summary", "FAILED", error=str(e))

# Назначение 4: Экспорт в Parquet (для аналитики)
try:
    writer_parquet = FileDFWriter(
        connection=local_fs,
        format=Parquet(compression="snappy"),
        target_path="/tmp/seminar5_complex_etl/sales_fact",
        options={
            "if_exists": "replace_entire_directory",
            "partitionBy": "order_year,order_month"  # партиционирование по дате
        }
    )
    writer_parquet.run(df_fact)
    logger.log_step("Экспорт в Parquet (партиционированный)", "SUCCESS", df_fact.count())
except Exception as e:
    logger.log_step("Экспорт в Parquet", "FAILED", error=str(e))

# Назначение 5: Экспорт месячной статистики в JSON
try:
    writer_json = FileDFWriter(
        connection=local_fs,
        format=JSON(),
        target_path="/tmp/seminar5_complex_etl/monthly_stats",
        options={"if_exists": "replace_entire_directory"}
    )
    writer_json.run(df_monthly)
    logger.log_step("Экспорт в JSON: monthly_stats", "SUCCESS", df_monthly.count())
except Exception as e:
    logger.log_step("Экспорт в JSON", "FAILED", error=str(e))

# ============================================================================
# Итоговая статистика
# ============================================================================
logger.summary()

print("\n📋 Структура созданных данных:")
print("""
PostgreSQL:
  ├── sales_fact_table (детальные продажи)
  ├── customer_summary (агрегация по клиентам)
  └── product_region_summary (агрегация по продуктам и регионам)

Файловая система:
  ├── /tmp/seminar5_complex_etl/sales_fact/ (Parquet, партиционированный)
  └── /tmp/seminar5_complex_etl/monthly_stats/ (JSON)
""")

spark.stop()
print("\n Комплексный ETL pipeline завершен успешно!")
