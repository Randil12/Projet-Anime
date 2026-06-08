import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from db_write_utils import append_rejects, reject_left_anti, split_duplicate_rejects, split_null_rejects, write_staging_then_replace

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
        SparkSession.builder.appName("gold_fact_ratings")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


if __name__ == "__main__":
    spark = build_spark_session()

    df = spark.read.parquet("s3a://silver/rating/")

    # Cast explicite vers int (rating peut être null = vu mais pas noté)
    fact = df.select(
        F.col("user_id").cast("int").alias("user_id"),
        F.col("anime_id").cast("int").alias("anime_id"),
        F.col("rating").cast("int").alias("rating"),
    )

    dim_anime = spark.read.jdbc(
        url=JDBC_URL,
        table="dim_anime",
        properties=JDBC_PROPS,
    ).select("anime_id")

    valid, null_rejects = split_null_rejects(fact, ["user_id", "anime_id"])
    valid, missing_anime_rejects = reject_left_anti(
        valid,
        dim_anime,
        ["anime_id"],
        "anime_id not found in dim_anime",
    )
    valid, duplicate_rejects = split_duplicate_rejects(valid, ["user_id", "anime_id"])
    rejects = (
        null_rejects.unionByName(missing_anime_rejects, allowMissingColumns=True)
        .unionByName(duplicate_rejects, allowMissingColumns=True)
    )
    rejected_count = append_rejects(
        rejects, JDBC_URL, JDBC_PROPS, "gold_fact_ratings", "fact_ratings"
    )

    inserted_count = write_staging_then_replace(
        valid,
        JDBC_URL,
        JDBC_PROPS,
        staging_table="stg_fact_ratings",
        target_table="fact_ratings",
        columns=["user_id", "anime_id", "rating"],
    )

    print(
        f"gold_fact_ratings : {inserted_count} lignes ecrites, "
        f"{rejected_count} lignes ignorees dans reject_records"
    )

    spark.stop()
