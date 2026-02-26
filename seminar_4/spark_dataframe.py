from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType
)


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("my-app")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    schema = StructType([
        StructField("order_id", IntegerType(), nullable=False),
        StructField("user_id", IntegerType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
        StructField("quantity", IntegerType(), nullable=False),
    ])

    data = [
        (1001, 42, "electronics", 19990.0, 1),
        (1001, 42, "accessories", 990.0, 2),
        (1002, 17, "books", 590.0, 1),
        (1003, 42, "electronics", 29990.0, 1),
        (1004, 99, "books", 790.0, 3),
        (1005, 17, "electronics", 15990.0, 1),
    ]

    orders_df = spark.createDataFrame(data, schema)
    print(f"{orders_df.count()=}")

    electronics_df = orders_df.filter(
        F.col("category") == "electronics"
    )
    user_spend_df = electronics_df.withColumn(
        "amount",
        F.col("price") * F.col("quantity")
    )
    result_df = (
        user_spend_df
        .groupBy("user_id")
        .agg(F.sum("amount").alias("total_spent"))
    )
    result_df.show()


    # Dataframe VS list
    values = [1, 3, 2]
    values.sort()
    sorted_values = sorted(values)

    df1 = spark.range(5)
    df2 = df1

    df1 = df1.filter("id > 2")

    df1.show()
    df2.show()
