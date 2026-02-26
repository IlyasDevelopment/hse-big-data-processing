from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("my-app")
        .master("spark://localhost:7077")
        .getOrCreate()
    )

    # Должно быть внешнее хранилище!
    # Источник датасета https://www.kaggle.com/datasets/teenakunsoth/nyctaxi
    print(spark.read.csv("/Users/gasanov/Desktop/pycharmProjects/uni_big_data/seminar_4/nyctaxi/").count())

    input("Press enter to stop Spark Driver")
    spark.stop()
