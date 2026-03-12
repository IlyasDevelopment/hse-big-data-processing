from etl_entities.hwm_store import BaseHWMStore
from horizon.client.auth import LoginPassword
from horizon_hwm_store import HorizonHWMStore
from onetl.file.format import Parquet
from onetl.connection import Postgres
from onetl.connection import SparkS3
from onetl.db import DBReader
from onetl.file import FileDFWriter
from onetl.strategy import IncrementalStrategy
from onetl.log import setup_logging
from pyspark.sql import SparkSession


def build_spark(spark_version: str) -> SparkSession:
    maven_packages = SparkS3.get_packages(spark_version=spark_version) + Postgres.get_packages()
    excluded_packages = SparkS3.get_exclude_packages()
    return (
        SparkSession.builder.appName("pg_to_s3_incremental")
        .config("spark.jars.packages", ",".join(maven_packages))
        .config("spark.jars.excludes", ",".join(excluded_packages))
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )


def main(spark: SparkSession):
    postgres = Postgres(
        host="localhost",
        port=5432,
        user="postgres",
        password="secret",
        database="postgres",
        spark=spark,
    ).check()

    s3 = SparkS3(
        host="localhost",
        protocol="http",
        port=9010,
        bucket="mybucket",
        region="us-east-1",
        access_key="minio",
        secret_key="miniosecret",
        path_style_access=True,
        spark=spark,
    ).check()

    reader = DBReader(
        connection=postgres,
        source="public.orders",
        columns=[
            "id",
            "user_id",
            "amount",
            "updated_at",
        ],
        hwm=DBReader.AutoDetectHWM(
            name="orders_hwm",
            expression="updated_at",
        ),
    )
    writer = FileDFWriter(
        connection=s3,
        target_path="orders/",
        format=Parquet(),
        options=FileDFWriter.Options(
            if_exists="append"
        ),
    )
    with get_hwm_store(), IncrementalStrategy():
        df = reader.run()
        if not df.isEmpty():
            writer.run(df)


def get_hwm_store() -> BaseHWMStore:
    return HorizonHWMStore(
        api_url="http://localhost:8020",
        auth=LoginPassword(
            login="admin",
            password="pass",
        ),
        namespace="my_namespace",
    ).force_create_namespace()


if __name__ == "__main__":
    setup_logging()

    spark = build_spark(spark_version="4.1.1")
    main(spark)
