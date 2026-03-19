from random import random
from operator import add

from pyspark.sql import SparkSession


def f(_):
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x**2 + y**2 <= 1 else 0


if __name__ == "__main__":
    spark = SparkSession.builder.appName("SparkPi").getOrCreate()

    partitions = 2
    n = 100000 * partitions

    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    pi = 4.0 * count / n
    print(f"Pi is roughly {pi}")
