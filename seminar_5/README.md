# Семинар 5: ETL с OneTL и Apache Spark

## Описание

Практический семинар по построению ETL-пайплайнов с использованием библиотеки [OneTL](https://onetl.readthedocs.io/en/stable/) и Apache Spark.

OneTL (One Tool for ETL) - это Python-библиотека для построения ETL-процессов, которая предоставляет:
- Унифицированный интерфейс для работы с различными источниками данных
- Интеграцию с Apache Spark для распределенной обработки
- Коннекторы для баз данных (PostgreSQL, MySQL, Oracle, Clickhouse и др.)
- Коннекторы для файловых систем (HDFS, S3, FTP и др.)
- Возможность инкрементальной загрузки данных
- Встроенную валидацию и мониторинг

## Структура семинара

### Уровень 1: Основы (Beginner)
1. **01_simple_file_read.py** - Чтение файлов через FileDFConnection
2. **02_simple_postgres_connection.py** - Подключение к PostgreSQL
3. **03_read_from_postgres.py** - Чтение данных из PostgreSQL в Spark DataFrame

### Уровень 2: Базовые ETL (Intermediate)
4. **04_postgres_to_file.py** - ETL: PostgreSQL → Parquet
5. **05_file_to_postgres.py** - ETL: CSV → PostgreSQL
6. **06_transform_pipeline.py** - ETL с трансформациями данных
7. **07_multiple_tables.py** - Работа с несколькими таблицами

### Уровень 3: Продвинутые паттерны (Advanced)
8. **08_incremental_load.py** - Инкрементальная загрузка с фильтрацией
9. **09_complex_etl_pipeline.py** - Сложный ETL: несколько источников и назначений
10. **10_s3_to_postgres.py** - Работа с S3 и PostgreSQL
11. **11_data_quality_validation.py** - Валидация качества данных
12. **12_table_creation_comparison.py** - Сравнение способов создания таблиц (авто vs ручной) с DECIMAL типами

## Требования

```bash
pip install -r requirements.txt
```

### Зависимости
- `onetl` - основная библиотека для ETL
- `pyspark` - Apache Spark для Python
- `psycopg2-binary` - драйвер PostgreSQL
- `pandas` - для вспомогательных операций

## Подготовка окружения

### 1. Запуск PostgreSQL через Docker

```bash
docker run -d \
  --name postgres-seminar5 \
  -e POSTGRES_USER=student \
  -e POSTGRES_PASSWORD=bigdata2024 \
  -e POSTGRES_DB=seminar5 \
  -p 5432:5432 \
  postgres:15
```

### 2. Создание тестовых данных

Используйте скрипт `setup_test_data.py` для создания тестовых таблиц и данных:

```bash
python setup_test_data.py
```

## Концепции OneTL

### Connection (Подключение)
Объект, представляющий соединение с источником данных:
- `Postgres` - PostgreSQL
- `MySQL` - MySQL/MariaDB
- `Oracle` - Oracle Database
- `HDFS` - Hadoop Distributed File System
- `S3` - Amazon S3
- `FTP` - FTP/SFTP серверы

### DBReader / DBWriter
Классы для чтения/записи данных из/в базы данных:
- Поддержка партиционирования
- Фильтрация на уровне источника
- Управление параллелизмом

### FileDFConnection
Работа с файлами через Spark DataFrame API:
- Чтение/запись в различных форматах
- Поддержка партиционирования
- Работа с локальными и распределенными файловыми системами

### Стратегии записи
- `append` - добавление данных
- `overwrite` - перезапись данных
- `ignore` - игнорировать, если существует
- `error` - ошибка, если существует

## Прогрессия сложности

1. **Простое чтение** → Понимание коннекторов
2. **Чтение из БД** → Работа с реляционными источниками
3. **Базовый ETL** → Перенос данных между системами
4. **Трансформации** → Обработка данных в процессе переноса
5. **Инкрементальные загрузки** → Оптимизация для больших данных
6. **Комплексные пайплайны** → Реальные сценарии использования

## Полезные ссылки

- [Документация OneTL](https://onetl.readthedocs.io/en/stable/)
- [GitHub репозиторий](https://github.com/MTSWebServices/onetl)
- [Примеры использования](https://onetl.readthedocs.io/en/stable/examples.html)
- [Список коннекторов](https://onetl.readthedocs.io/en/stable/connection/index.html)

## Примечания

- HWM (High Water Mark) будет рассмотрен в Семинаре 6
- Все скрипты содержат подробные комментарии на русском языке
- Рекомендуется выполнять скрипты последовательно для лучшего понимания
