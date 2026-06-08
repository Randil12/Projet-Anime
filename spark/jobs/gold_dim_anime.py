import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from db_write_utils import append_rejects, split_duplicate_rejects, split_null_rejects, write_staging_then_replace

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_PASSWORD")

DB_USER = os.environ.get("DB_WRITE_USER", "data_engineer")
DB_PASSWORD = os.environ.get("DB_WRITE_PASSWORD")
DB_NAME = os.environ.get("DB_BUSINESS_NAME")
JDBC_URL = f"jdbc:postgresql://postgres-business:5432/{DB_NAME}"

JDBC_PROPS = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("gold_dim_anime")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


if __name__ == "__main__":
    spark = build_spark_session()

    df = spark.read.parquet("s3a://silver/anime/")

    # Cast vers les bons types Postgres + renommage rating → mal_rating
    dim_anime = df.select(
        F.col("anime_id").cast("int").alias("anime_id"),
        "name",
        "type",
        F.col("episodes").cast("int").alias("episodes"),
        F.col("is_airing").cast("boolean").alias("is_airing"),
        F.col("rating").cast("float").alias("mal_rating"),
        F.col("members").cast("int").alias("members"),
    )

    valid, null_rejects = split_null_rejects(dim_anime, ["anime_id", "name"])
    valid, duplicate_rejects = split_duplicate_rejects(valid, ["anime_id"])
    rejects = null_rejects.unionByName(duplicate_rejects)
    rejected_count = append_rejects(
        rejects, JDBC_URL, JDBC_PROPS, "gold_dim_anime", "dim_anime"
    )

    inserted_count = write_staging_then_replace(
        valid,
        JDBC_URL,
        JDBC_PROPS,
        staging_table="stg_dim_anime",
        target_table="dim_anime",
        columns=["anime_id", "name", "type", "episodes", "is_airing", "mal_rating", "members"],
        truncate_cascade=True,
    )

    print(
        f"gold_dim_anime : {inserted_count} lignes ecrites, "
        f"{rejected_count} lignes ignorees dans reject_records"
    )

    spark.stop()
