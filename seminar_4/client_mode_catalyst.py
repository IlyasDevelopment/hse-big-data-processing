from pyspark.sql import SparkSession, functions as F


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("my-app")
        .master("spark://localhost:7077")
        .getOrCreate()
    )

    (
        spark
        .read
        .option("header", True)
        .csv("/Users/gasanov/Desktop/pycharmProjects/uni_big_data/seminar_4/nyctaxi/ID100_2018_Yellow_Taxi_Trip_Data.csv")
        .filter("passenger_count = 1")
        .groupBy("payment_type").agg(F.avg("trip_distance").alias("avg_distance"))
        .filter("payment_type = 2")
        # .show()
        .explain()
    )
    input("Press enter to stop Spark Driver")
    spark.stop()
