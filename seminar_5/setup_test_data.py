"""
Скрипт для подготовки тестовых данных в PostgreSQL

Создает:
- Таблицу users с тестовыми пользователями
- Индексы для оптимизации запросов

Запуск: python setup_test_data.py
"""

import psycopg2
from psycopg2.extras import execute_batch
import random
from datetime import date, timedelta

print("=" * 70)
print("Подготовка тестовых данных для Семинара 5")
print("=" * 70)

# ============================================================================
# Параметры подключения
# ============================================================================
DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "user": "student",
    "password": "bigdata2024",
    "database": "seminar5"
}

print("\n📡 Подключение к PostgreSQL...")
print(f"   Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
print(f"   Database: {DB_CONFIG['database']}")

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("✓ Подключение установлено")
except Exception as e:
    print(f"\n❌ Ошибка подключения: {e}")
    print("\n💡 Убедитесь, что PostgreSQL запущен:")
    print("""
    docker run -d --name postgres-seminar5 \\
      -e POSTGRES_USER=student \\
      -e POSTGRES_PASSWORD=bigdata2024 \\
      -e POSTGRES_DB=seminar5 \\
      -p 5433:5432 \\
      postgres:15

    Или используйте docker-compose:
    docker-compose up -d
    """)
    exit(1)

# ============================================================================
# Создание таблицы users
# ============================================================================
print("\n" + "=" * 70)
print("Создание таблицы 'users'")
print("=" * 70)

cursor.execute("DROP TABLE IF EXISTS users CASCADE")
cursor.execute("""
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(255) NOT NULL UNIQUE,
        age INTEGER NOT NULL CHECK (age >= 0 AND age <= 150),
        city VARCHAR(100) NOT NULL,
        registration_date DATE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")

print("✓ Таблица 'users' создана")

# ============================================================================
# Генерация тестовых данных
# ============================================================================
print("\n" + "=" * 70)
print("Генерация тестовых данных")
print("=" * 70)

# Списки для генерации случайных данных
russian_first_names = [
    "Александр", "Дмитрий", "Максим", "Сергей", "Андрей", "Алексей", "Артем", "Илья",
    "Кирилл", "Михаил", "Никита", "Матвей", "Роман", "Егор", "Арсений", "Иван",
    "Анна", "Мария", "Екатерина", "Дарья", "Алина", "Ирина", "Наталья", "Татьяна",
    "Ольга", "Юлия", "Елена", "Светлана", "Марина", "Анастасия", "Виктория", "Полина"
]

russian_last_names = [
    "Иванов", "Смирнов", "Кузнецов", "Попов", "Васильев", "Петров", "Соколов", "Михайлов",
    "Новиков", "Федоров", "Морозов", "Волков", "Алексеев", "Лебедев", "Семенов", "Егоров",
    "Павлов", "Козлов", "Степанов", "Николаев", "Орлов", "Андреев", "Макаров", "Никитин"
]

russian_cities = [
    "Москва", "Санкт-Петербург", "Новосибирск", "Екатеринбург", "Казань",
    "Нижний Новгород", "Челябинск", "Самара", "Омск", "Ростов-на-Дону",
    "Уфа", "Красноярск", "Воронеж", "Пермь", "Волгоград", "Краснодар",
    "Саратов", "Тюмень", "Тольятти", "Ижевск"
]

# Генерируем данные
users_data = []
start_date = date(2020, 1, 1)
end_date = date(2024, 12, 31)

print("📊 Генерируем пользователей...")

for i in range(1, 1001):  # 1000 пользователей
    first_name = random.choice(russian_first_names)
    last_name = random.choice(russian_last_names)

    # Генерируем email на основе имени
    email_variants = [
        f"{first_name.lower()}.{last_name.lower()}{i}@example.com",
        f"{last_name.lower()}{i}@mail.ru",
        f"{first_name.lower()}{i}@yandex.ru"
    ]
    email = random.choice(email_variants)

    # Возраст с нормальным распределением (средний возраст 35, стандартное отклонение 12)
    age = int(random.gauss(35, 12))
    age = max(18, min(70, age))  # ограничиваем 18-70

    # Случайный город
    city = random.choice(russian_cities)

    # Случайная дата регистрации
    days_between = (end_date - start_date).days
    random_days = random.randint(0, days_between)
    registration_date = start_date + timedelta(days=random_days)

    users_data.append((
        f"{first_name} {last_name}",
        email,
        age,
        city,
        registration_date
    ))

# Вставляем данные батчами для производительности
print("💾 Загружаем данные в PostgreSQL...")

insert_query = """
    INSERT INTO users (name, email, age, city, registration_date)
    VALUES (%s, %s, %s, %s, %s)
"""

execute_batch(cursor, insert_query, users_data, page_size=100)
conn.commit()

print(f"✓ Загружено пользователей: {len(users_data)}")

# ============================================================================
# Создание индексов
# ============================================================================
print("\n" + "=" * 70)
print("Создание индексов")
print("=" * 70)

indexes = [
    ("idx_users_city", "CREATE INDEX idx_users_city ON users(city)"),
    ("idx_users_age", "CREATE INDEX idx_users_age ON users(age)"),
    ("idx_users_registration_date", "CREATE INDEX idx_users_registration_date ON users(registration_date)")
]

for index_name, index_sql in indexes:
    cursor.execute(index_sql)
    print(f"✓ Создан индекс: {index_name}")

conn.commit()

# ============================================================================
# Статистика по созданным данным
# ============================================================================
print("\n" + "=" * 70)
print("📊 Статистика по созданным данным")
print("=" * 70)

# Общее количество
cursor.execute("SELECT COUNT(*) FROM users")
total_users = cursor.fetchone()[0]
print(f"\nВсего пользователей: {total_users}")

# Распределение по городам
print("\n📍 Топ-10 городов по количеству пользователей:")
cursor.execute("""
    SELECT city, COUNT(*) as user_count
    FROM users
    GROUP BY city
    ORDER BY user_count DESC
    LIMIT 10
""")

for city, count in cursor.fetchall():
    print(f"   {city:20s}: {count:4d} пользователей")

# Распределение по возрасту
print("\n👥 Распределение по возрастным группам:")
cursor.execute("""
    SELECT
        CASE
            WHEN age < 25 THEN '18-24'
            WHEN age >= 25 AND age < 35 THEN '25-34'
            WHEN age >= 35 AND age < 45 THEN '35-44'
            WHEN age >= 45 AND age < 55 THEN '45-54'
            ELSE '55+'
        END as age_group,
        COUNT(*) as user_count,
        ROUND(AVG(age), 1) as avg_age
    FROM users
    GROUP BY age_group
    ORDER BY age_group
""")

for age_group, count, avg_age in cursor.fetchall():
    print(f"   {age_group:10s}: {count:4d} пользователей (средний возраст: {avg_age})")

# Распределение по годам регистрации
print("\n📅 Распределение по годам регистрации:")
cursor.execute("""
    SELECT
        EXTRACT(YEAR FROM registration_date) as year,
        COUNT(*) as user_count
    FROM users
    GROUP BY year
    ORDER BY year
""")

for year, count in cursor.fetchall():
    print(f"   {int(year):4d}: {count:4d} пользователей")

# Статистика по возрасту
cursor.execute("""
    SELECT
        MIN(age) as min_age,
        MAX(age) as max_age,
        ROUND(AVG(age), 1) as avg_age,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age) as median_age
    FROM users
""")

min_age, max_age, avg_age, median_age = cursor.fetchone()
print(f"\n📈 Статистика по возрасту:")
print(f"   Минимальный: {min_age}")
print(f"   Максимальный: {max_age}")
print(f"   Средний: {avg_age}")
print(f"   Медиана: {median_age}")

# ============================================================================
# Примеры записей
# ============================================================================
print("\n" + "=" * 70)
print("📋 Примеры записей")
print("=" * 70)

cursor.execute("""
    SELECT id, name, email, age, city, registration_date
    FROM users
    ORDER BY RANDOM()
    LIMIT 10
""")

print("\n{:>5} {:25} {:35} {:>5} {:20} {:12}".format(
    "ID", "Имя", "Email", "Возр", "Город", "Регистрация"
))
print("-" * 110)

for row in cursor.fetchall():
    print("{:5d} {:25} {:35} {:5d} {:20} {}".format(*row))

# ============================================================================
# Информация о таблицах
# ============================================================================
print("\n" + "=" * 70)
print("ℹ️  Информация о созданных объектах")
print("=" * 70)

cursor.execute("""
    SELECT
        schemaname,
        tablename,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
    FROM pg_tables
    WHERE schemaname = 'public'
    ORDER BY tablename
""")

print("\n📊 Таблицы:")
for schema, table, size in cursor.fetchall():
    print(f"   {table:30s}: {size}")

cursor.execute("""
    SELECT
        indexname,
        tablename,
        pg_size_pretty(pg_relation_size(indexname::regclass)) AS size
    FROM pg_indexes
    WHERE schemaname = 'public'
    ORDER BY indexname
""")

print("\n🔍 Индексы:")
for index, table, size in cursor.fetchall():
    print(f"   {index:40s} на {table:20s}: {size}")

# ============================================================================
# Завершение
# ============================================================================
cursor.close()
conn.close()

print("\n" + "=" * 70)
print(" Подготовка тестовых данных завершена успешно!")
print("=" * 70)

print("""
Следующие шаги:
1. Запустите любой скрипт из семинара 5, начиная с 01_simple_file_read.py
2. Скрипты рекомендуется выполнять последовательно
3. Все комментарии в скриптах на русском языке

Примеры запуска:
  python 01_simple_file_read.py
  python 02_simple_postgres_connection.py
  python 03_read_from_postgres.py
  ...

Удачи в изучении OneTL! 🚀
""")
