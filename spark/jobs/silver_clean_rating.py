import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_PASSWORD")


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

    df = spark.read.csv("s3a://bronze/rating/rating.csv", header=True, inferSchema=False)

    # Cast des colonnes
    df = df.withColumn("user_id", F.col("user_id").cast("int"))
    df = df.withColumn("anime_id", F.col("anime_id").cast("int"))
    df = df.withColumn("rating", F.col("rating").cast("int"))

    # Supprimer les lignes sans user_id ou anime_id
    df = df.filter(F.col("user_id").isNotNull() & F.col("anime_id").isNotNull())

    # -1 va être null (vu mais pas noté)
    df = df.withColumn(
        "rating", F.when(F.col("rating") == -1, None).otherwise(F.col("rating"))
    )

    # Dédupliquer sur user_id + anime_id
    df = df.dropDuplicates(["user_id", "anime_id"])

    # Intégrité référentielle : garder uniquement les anime_id présents dans silver/anime
    anime_df = spark.read.parquet("s3a://silver/anime/").select("anime_id")
    df = df.join(anime_df, on="anime_id", how="inner")

    df.write.mode("overwrite").parquet("s3a://silver/rating/")
    print(f"silver_clean_rating : {df.count()} lignes écrites dans silver/rating/")

    spark.stop()
