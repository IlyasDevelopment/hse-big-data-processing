from pyspark.sql import SparkSession
from onetl.connection import Postgres, SparkLocalFS
from onetl.db import DBReader
from onetl.file import FileDFWriter
from onetl.file.format import Parquet
import os

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("OneTL ETL: Postgres to Parquet") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("Скрипт 4: ETL Pipeline - PostgreSQL → Parquet")
print("=" * 70)

# Путь для сохранения результатов
output_path = "/tmp/seminar5_etl_output"

# ============================================================================
# Шаг 1: Подключение к источнику (PostgreSQL)
# ============================================================================
print("\n" + "=" * 70)
print("Шаг 1: Подключение к PostgreSQL (источник)")
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
# Шаг 2: Чтение данных из источника
# ============================================================================
print("\n" + "=" * 70)
print("Шаг 2: Чтение данных из таблицы 'users'")
print("=" * 70)

reader = DBReader(
    connection=postgres,
    source="public.users"
)

print("📖 Читаем данные из PostgreSQL...")
df = reader.run()

print(f"✓ Прочитано строк: {df.count()}")
print("\n📊 Схема данных:")
df.printSchema()

# ============================================================================
# Шаг 3: Подключение к целевой системе (локальная ФС)
# ============================================================================
print("\n" + "=" * 70)
print("Шаг 3: Подключение к локальной файловой системе (назначение)")
print("=" * 70)

local_fs = SparkLocalFS(spark=spark)
print("✓ Подключение к локальной ФС создано")

# ============================================================================
# Шаг 4: Запись данных в Parquet (overwrite режим)
# ============================================================================
print("\n" + "=" * 70)
print("Шаг 4: Запись в Parquet формат (режим: overwrite)")
print("=" * 70)

writer = FileDFWriter(
    connection=local_fs,
    format=Parquet(),
    target_path=f"{output_path}/users_parquet",
    options={"if_exists": "replace_entire_directory"}  # перезаписываем если существует
)

print(f"💾 Записываем данные в: {output_path}/users_parquet")
writer.run(df)
print("✓ Данные успешно записаны в Parquet")

# Проверяем что записалось
files = os.listdir(f"{output_path}/users_parquet")
parquet_files = [f for f in files if f.endswith('.parquet')]
print(f"\n✓ Создано файлов: {len(parquet_files)}")

# Читаем обратно для проверки
df_verify = spark.read.parquet(f"{output_path}/users_parquet")
print(f"✓ Проверка: прочитано {df_verify.count()} строк")

# ============================================================================
# Шаг 5: Запись с партиционированием по городам
# ============================================================================
print("\n" + "=" * 70)
print("Шаг 5: Запись с партиционированием по колонке 'city'")
print("=" * 70)

writer_partitioned = FileDFWriter(
    connection=local_fs,
    format=Parquet(),
    target_path=f"{output_path}/users_partitioned",
    options={
        "if_exists": "replace_entire_directory",
        "partitionBy": "city"  # партиционируем по городу
    }
)

print(f"💾 Записываем данные с партиционированием...")
writer_partitioned.run(df)
print("✓ Данные записаны с партициями")

# Проверяем структуру директорий
print("\n📁 Структура партиций:")
for root, dirs, files in os.walk(f"{output_path}/users_partitioned"):
    level = root.replace(f"{output_path}/users_partitioned", '').count(os.sep)
    indent = ' ' * 2 * level
    print(f'{indent}{os.path.basename(root)}/')
    subindent = ' ' * 2 * (level + 1)
    for file in files[:3]:  # показываем только первые 3 файла
        print(f'{subindent}{file}')
    if len(files) > 3:
        print(f'{subindent}... ещё {len(files) - 3} файлов')

# ============================================================================
# Шаг 6: Инкрементальная загрузка (append режим)
# ============================================================================
print("\n" + "=" * 70)
print("Шаг 6: Инкрементальная загрузка (режим: append)")
print("=" * 70)

# Читаем только новые записи (например, с id > 500)
reader_incremental = DBReader(
    connection=postgres,
    source="public.users",
    where="id > 500"
)

df_new = reader_incremental.run()
print(f"📖 Прочитано новых записей: {df_new.count()}")

# Добавляем к существующим данным
writer_append = FileDFWriter(
    connection=local_fs,
    format=Parquet(),
    target_path=f"{output_path}/users_incremental",
    options={"if_exists": "append"}  # добавляем к существующим данным
)

# Первая загрузка - все данные
print("\n💾 Первая загрузка (все данные)...")
writer_initial = FileDFWriter(
    connection=local_fs,
    format=Parquet(),
    target_path=f"{output_path}/users_incremental",
    options={"if_exists": "replace_entire_directory"}
)
df_initial = DBReader(connection=postgres, source="public.users", where="id <= 500").run()
writer_initial.run(df_initial)
print(f"✓ Загружено: {df_initial.count()} записей")

# Вторая загрузка - новые данные (append)
print("\n💾 Вторая загрузка (новые данные)...")
writer_append.run(df_new)
print(f"✓ Добавлено: {df_new.count()} записей")

# Проверка итогового результата
df_total = spark.read.parquet(f"{output_path}/users_incremental")
print(f"\n✓ Итого записей в файле: {df_total.count()}")

# ============================================================================
# Шаг 7: Запись с компрессией
# ============================================================================
print("\n" + "=" * 70)
print("Шаг 7: Запись с различными кодеками сжатия")
print("=" * 70)

# Различные кодеки сжатия для Parquet
codecs = ["snappy", "gzip", "uncompressed"]

for codec in codecs:
    writer_compressed = FileDFWriter(
        connection=local_fs,
        format=Parquet(compression=codec),
        target_path=f"{output_path}/users_{codec}",
        options={"if_exists": "replace_entire_directory"}
    )

    print(f"\n💾 Записываем с кодеком: {codec}")
    writer_compressed.run(df)

    # Проверяем размер
    import subprocess
    result = subprocess.run(
        ["du", "-sh", f"{output_path}/users_{codec}"],
        capture_output=True,
        text=True
    )
    size = result.stdout.split()[0]
    print(f"   Размер на диске: {size}")

# ============================================================================
# Итоговая статистика
# ============================================================================
print("\n" + "=" * 70)
print("📊 Итоговая статистика ETL процесса")
print("=" * 70)

print(f"""
✓ Источник: PostgreSQL (таблица 'users')
✓ Назначение: Parquet файлы
✓ Прочитано строк: {df.count()}
✓ Записано файлов:
   - users_parquet: простая запись
   - users_partitioned: с партиционированием по city
   - users_incremental: инкрементальная загрузка
   - users_snappy/gzip/uncompressed: разные кодеки сжатия
""")

# Очистка
spark.stop()
print("\n ETL процесс завершен успешно!")
