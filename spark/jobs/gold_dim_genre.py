import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
        SparkSession.builder.appName("gold_dim_genre")
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

    # Exploser les genres et garder les uniques
    unique_genres = (
        df.filter(F.col("genre").isNotNull())
        .withColumn("genre_name", F.explode(F.split(F.col("genre"), ", ")))
        .select(F.trim("genre_name").alias("genre_name"))
        .filter(F.col("genre_name") != "")
        .distinct()
    )

    # genre_id déterministe (basé sur l'ordre alphabétique du nom)
    window = Window.orderBy("genre_name")
    dim_genre = unique_genres.withColumn(
        "genre_id", F.row_number().over(window)
    ).select("genre_id", "genre_name")

    valid, null_rejects = split_null_rejects(dim_genre, ["genre_id", "genre_name"])
    valid, duplicate_id_rejects = split_duplicate_rejects(valid, ["genre_id"])
    valid, duplicate_name_rejects = split_duplicate_rejects(valid, ["genre_name"])
    rejects = null_rejects.unionByName(duplicate_id_rejects).unionByName(duplicate_name_rejects)
    rejected_count = append_rejects(
        rejects, JDBC_URL, JDBC_PROPS, "gold_dim_genre", "dim_genre"
    )

    inserted_count = write_staging_then_replace(
        valid,
        JDBC_URL,
        JDBC_PROPS,
        staging_table="stg_dim_genre",
        target_table="dim_genre",
        columns=["genre_id", "genre_name"],
        truncate_cascade=True,
    )

    print(
        f"gold_dim_genre : {inserted_count} genres ecrits, "
        f"{rejected_count} lignes ignorees dans reject_records"
    )

    spark.stop()
