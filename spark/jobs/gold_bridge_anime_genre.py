import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
        SparkSession.builder.appName("gold_bridge_anime_genre")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


if __name__ == "__main__":
    spark = build_spark_session()

    # Lire silver/anime et exploser les genres
    anime_df = spark.read.parquet("s3a://silver/anime/")
    anime_genres = (
        anime_df.filter(F.col("genre").isNotNull())
        .select(
            F.col("anime_id").cast("int").alias("anime_id"),
            F.explode(F.split(F.col("genre"), ", ")).alias("genre_name"),
        )
        .withColumn("genre_name", F.trim("genre_name"))
        .filter(F.col("genre_name") != "")
    )

    # Lire dim_genre depuis Postgres pour récupérer les genre_id
    dim_genre = spark.read.jdbc(
        url=JDBC_URL,
        table="dim_genre",
        properties=JDBC_PROPS,
    )

    # Joindre pour transformer les genre_name en genre_id
    # distinct() : protège contre les doublons (genre listé 2 fois dans la même chaîne)
    bridge = (
        anime_genres.join(dim_genre, on="genre_name", how="inner")
        .select("anime_id", "genre_id")
        .distinct()
    )

    (bridge.write
        .mode("overwrite")
        .option("truncate", "true")
        .jdbc(url=JDBC_URL, table="bridge_anime_genre", properties=JDBC_PROPS))

    print(f"gold_bridge_anime_genre : {bridge.count()} liaisons écrites")

    spark.stop()
