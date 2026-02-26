from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("my-rdd-app")
        .master("spark://localhost:7077")
        # .master("local[*]")
        # .config("spark.driver.bindAddress", "127.0.0.1")
        # .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    raw_orders = spark.sparkContext.parallelize([
        "1001,42,electronics,19990,1",
        "1001,42,accessories,990,2",
        "1002,17,books,590,1",
        "1003,42,electronics,29990,1",
        "1004,99,books,790,3",
        "1005,17,electronics,15990,1",
    ])
    parsed = raw_orders.map(lambda r: r.split(","))

    typed = parsed.map(
        lambda r: (
            int(r[0]),     # order_id
            int(r[1]),     # user_id
            r[2],          # category
            float(r[3]),   # price
            int(r[4])      # quantity
        )
    )

    electronics = typed.filter(lambda r: r[2] == "electronics")
    user_spend = electronics.map(
        lambda r: (r[1], r[3] * r[4])
    )

    result = user_spend.reduceByKey(lambda a, b: a + b)
    print(result.collect())

    input("Press enter to stop Spark Driver")
    spark.stop()
