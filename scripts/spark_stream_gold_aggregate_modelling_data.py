from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, max, min, first, last, sum, count, expr, current_timestamp
)
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType, BooleanType

import os
from dotenv import load_dotenv

load_dotenv()

MINIO_CONF = {
    "endpoint": os.getenv("MINIO_ENDPOINT"),
    "access_key": os.getenv("MINIO_ACCESS_KEY"),
    "secret_key": os.getenv("MINIO_SECRET_KEY")
}

DB_URL = "jdbc:postgresql://postgres:5432/warehouse_db"

DB_PROPERTIES = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

def create_spark_session():
    return SparkSession.builder \
        .appName("BinanceGoldAggregation") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("Gold Aggregation Job Started...")

    silver_df = spark.read \
        .format("parquet") \
        .load("s3a://silver/crypto_trades/")

    gold_df = silver_df \
        .groupBy(
            window(col("trade_timestamp"), "1 minute"),
            col("symbol")
        ) \
        .agg(
            first("price", ignorenulls=True).alias("open_price"),
            max("price").alias("high_price"),
            min("price").alias("low_price"),
            last("price", ignorenulls=True).alias("close_price"),
            sum("quantity").alias("total_volume"),
            count("*").alias("trade_count"),
            sum(expr("CASE WHEN is_buyer_maker = false THEN quantity ELSE 0 END")).alias("buy_volume_taker"),
            sum(expr("CASE WHEN is_buyer_maker = true THEN quantity ELSE 0 END")).alias("sell_volume_maker")
        ) \
        .select(
            col("window.start").alias("candle_start_time"),
            col("symbol"),
            col("open_price"),
            col("high_price"),
            col("low_price"),
            col("close_price"),
            col("total_volume"),
            col("buy_volume_taker"),
            col("sell_volume_maker"),
            col("trade_count"),
            current_timestamp().alias("ingested_at")
        )

    gold_df.write \
        .mode("overwrite") \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "fact_market_candles") \
        .option("user", DB_PROPERTIES["user"]) \
        .option("password", DB_PROPERTIES["password"]) \
        .option("driver", DB_PROPERTIES["driver"]) \
        .save()

    print("Gold aggregation complete — data written to PostgreSQL")

if __name__ == "__main__":
    main()