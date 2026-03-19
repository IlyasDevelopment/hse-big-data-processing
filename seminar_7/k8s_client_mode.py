from operator import add
from random import random

from pyspark.sql import SparkSession


def build_spark():
    return (
        SparkSession.builder.appName("SparkPi")
        .config("spark.master", "k8s://https://127.0.0.1:52337")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.kubernetes.container.image", "apache/spark:4.1.1")
        .config("spark.kubernetes.namespace", "dn")
        .config("spark.executor.instances", "2")
        .config("spark.ui.port", "4040")
        .getOrCreate()
    )

def f(_):
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x**2 + y**2 <= 1 else 0


if __name__ == "__main__":
    spark = build_spark()

    partitions = 2
    n = 100000 * partitions

    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    pi = 4.0 * count / n
    print(f"Pi is roughly {pi}")

    input("Press enter to stop Spark Driver")
