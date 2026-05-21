import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_PASSWORD")

DB_USER = os.environ.get("DB_BUSINESS_USER")
DB_PASSWORD = os.environ.get("DB_BUSINESS_PASSWORD")
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

    (dim_genre.write
        .mode("overwrite")
        .option("truncate", "true")
        .option("cascadeTruncate", "true")
        .jdbc(url=JDBC_URL, table="dim_genre", properties=JDBC_PROPS))

    print(f"gold_dim_genre : {dim_genre.count()} genres uniques écrits")

    spark.stop()
