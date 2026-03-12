import logging
from datetime import datetime, timedelta
import random

import psycopg
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PG_DSN = "postgresql://postgres:secret@localhost:5432/postgres"
S3_ENDPOINT = "http://localhost:9010"
S3_ACCESS_KEY = "minio"
S3_SECRET_KEY = "miniosecret"
S3_BUCKET = "mybucket"


def init_postgres(rows_amount: int = 1000):
    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                user_id INT,
                amount NUMERIC(10,2),
                updated_at TIMESTAMP
            )
            """)

            now = datetime.now()
            rows = []
            for i in range(rows_amount):
                rows.append((
                    random.randint(1, 50),
                    round(random.uniform(10, 1000), 2),
                    now - timedelta(seconds=rows_amount-i),
                ))

            cur.executemany(
                """
                INSERT INTO orders (user_id, amount, updated_at)
                VALUES (%s,%s,%s)
                """,
                rows
            )


def init_s3():
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )
    try:
        s3.create_bucket(Bucket=S3_BUCKET)
    except ClientError:
        pass


def main():
    init_postgres()
    init_s3()
    logger.info("Data prepared")


if __name__ == "__main__":
    main()
