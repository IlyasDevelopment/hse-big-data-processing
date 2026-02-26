from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("my-app")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    df0 = spark.range(10)
    df0.withColumn("x", df0.id * 2)  # map-операция — никаких перемещений данных между экзекуторами нет

    df = spark.createDataFrame([
        ("a", 1),
        ("b", 2),
        ("a", 3),
        ("b", 4)
    ], ["key", "value"])

    df.groupBy("key").sum("value").show()

    """
    Допустим у нас 2 партиции:
    Partition 1: ("a",1), ("b",2)
    Partition 2: ("a",3), ("b",4)
    
    Чтобы посчитать сумму по ключу:
    Map-фаза
    Каждый executor считает локальные суммы:
    Partition 1 -- a:1, b:2
    Partition 2 -- a:3, b:4
    
    Shuffle-фаза
    Spark пересылает данные так, чтобы:
    Все "a" попали в одну партицию
    Все "b" в другую
    То есть производим сетевое перемещение данных.
    
    Reduce-фаза
    Теперь можно посчитать финальные суммы:
    a: 1 + 3 = 4
    b: 2 + 4 = 6
    """
