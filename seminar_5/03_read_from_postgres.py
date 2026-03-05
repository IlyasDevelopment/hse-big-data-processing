from pyspark.sql import SparkSession
from onetl.connection import Postgres
from onetl.db import DBReader

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("OneTL Read from Postgres") \
    .master("local[*]") \
    .config("spark.jars.packages", Postgres.get_packages()) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("Скрипт 3: Чтение данных из PostgreSQL")
print("=" * 70)

# Создаем подключение к PostgreSQL
postgres = Postgres(
    host="localhost",
    port=5433,
    user="student",
    password="bigdata2024",
    database="seminar5",
    spark=spark
)

print(f"\n✓ Подключение к PostgreSQL создано")

# Проверяем наличие тестовой таблицы
try:
    postgres.check()
except Exception as e:
    print(f"\n❌ Ошибка подключения: {e}")
    print("💡 Запустите сначала setup_test_data.py")
    spark.stop()
    exit(1)

# ============================================================================
# Пример 1: Простое чтение всей таблицы
# ============================================================================
print("\n" + "=" * 70)
print("Пример 1: Чтение всей таблицы 'users'")
print("=" * 70)

reader = DBReader(
    connection=postgres,
    source="public.users",  # имя таблицы
)

print("\n📖 Читаем таблицу users...")
df_users = reader.run()

print(f"✓ Прочитано строк: {df_users.count()}")
print("\n📊 Схема таблицы:")
df_users.printSchema()

print("\n📋 Первые 10 записей:")
df_users.show(10, truncate=False)

# ============================================================================
# Пример 2: Чтение с выбором колонок
# ============================================================================
print("\n" + "=" * 70)
print("Пример 2: Чтение только определенных колонок")
print("=" * 70)

reader_columns = DBReader(
    connection=postgres,
    source="public.users",
    columns=["id", "name", "city"]  # выбираем только нужные колонки
)

print("\n📖 Читаем только id, name, city...")
df_selected = reader_columns.run()

print(f"✓ Прочитано строк: {df_selected.count()}")
print("\n📋 Данные:")
df_selected.show(10, truncate=False)

# ============================================================================
# Пример 3: Чтение с фильтрацией
# ============================================================================
print("\n" + "=" * 70)
print("Пример 3: Чтение с WHERE условием (возраст > 30)")
print("=" * 70)

reader_filtered = DBReader(
    connection=postgres,
    source="public.users",
    where="age > 30"  # SQL WHERE условие
)

print("\n📖 Читаем пользователей старше 30 лет...")
df_filtered = reader_filtered.run()

print(f"✓ Прочитано строк: {df_filtered.count()}")
print("\n📋 Данные:")
df_filtered.show(truncate=False)

# ============================================================================
# Пример 4: Чтение с партиционированием для параллелизма
# ============================================================================
print("\n" + "=" * 70)
print("Пример 4: Параллельное чтение с партиционированием")
print("=" * 70)

# Партиционирование позволяет читать данные параллельно в несколько потоков
# Это ускоряет чтение больших таблиц
reader_partitioned = DBReader(
    connection=postgres,
    source="public.users",
    options={
        "partitionColumn": "id",  # колонка для партиционирования
        "lowerBound": "1",        # минимальное значение
        "upperBound": "1000",     # максимальное значение
        "numPartitions": "4"      # количество партиций
    }
)

print("\n📖 Читаем с партиционированием по колонке 'id'...")
print("   Партиций: 4")
print("   Диапазон id: 1-1000")

df_partitioned = reader_partitioned.run()

print(f"\n✓ Прочитано строк: {df_partitioned.count()}")
print(f"✓ Количество партиций в DataFrame: {df_partitioned.rdd.getNumPartitions()}")

# ============================================================================
# Пример 5: Чтение с использованием SQL запроса
# ============================================================================
print("\n" + "=" * 70)
print("Пример 5: Чтение с помощью SQL запроса")
print("=" * 70)

# Можно читать результат произвольного SQL запроса
reader_query = DBReader(
    connection=postgres,
    source="""
        (SELECT
            city,
            COUNT(*) as user_count,
            AVG(age) as avg_age,
            MIN(age) as min_age,
            MAX(age) as max_age
        FROM public.users
        GROUP BY city
        ORDER BY user_count DESC) as city_stats
    """
)

print("\n📖 Выполняем агрегацию на уровне БД...")
df_stats = reader_query.run()

print(f"\n✓ Прочитано строк: {df_stats.count()}")
print("\n📊 Статистика по городам:")
df_stats.show(truncate=False)


