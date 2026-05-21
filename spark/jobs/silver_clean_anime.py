import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_PASSWORD")

EXCLUDED_GENRES = ["Hentai", "Yaoi", "Yuri"]


def build_spark_session() -> SparkSession:
    # Plus besoin de spécifier master() ou les JARS ici !
    return (
        SparkSession.builder.appName("silver_clean_anime")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


if __name__ == "__main__":
    spark = build_spark_session()

    df = spark.read.csv("s3a://bronze/anime/anime.csv", header=True, inferSchema=False)

    # is_airing avant de caster episodes
    df = df.withColumn("is_airing", F.col("episodes") == "Unknown")

    # episodes : Unknown → null puis cast int
    df = df.withColumn(
        "episodes",
        F.when(F.col("episodes") == "Unknown", None)
        .otherwise(F.col("episodes").cast("int")),
    )

    # Trim des colonnes string
    for col in ["name", "genre", "type"]:
        df = df.withColumn(col, F.trim(F.col(col)))

    # genre vide → null
    df = df.withColumn(
        "genre", F.when(F.col("genre") == "", None).otherwise(F.col("genre"))
    )

    # Supprimer les lignes sans anime_id ou sans name
    df = df.filter(F.col("anime_id").isNotNull() & F.col("name").isNotNull())

    # Filtrer les genres exclus
    excluded_pattern = "|".join(EXCLUDED_GENRES)
    df = df.filter(
        F.col("genre").isNull() | ~F.col("genre").rlike(excluded_pattern)
    )

    # Dédupliquer sur anime_id
    df = df.dropDuplicates(["anime_id"])

    df.write.mode("overwrite").parquet("s3a://silver/anime/")
    print(f"silver_clean_anime : {df.count()} lignes écrites dans silver/anime/")

    spark.stop()
