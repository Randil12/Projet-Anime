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
    dim_anime = spark.read.jdbc(
        url=JDBC_URL,
        table="dim_anime",
        properties=JDBC_PROPS,
    ).select("anime_id")

    # Joindre pour transformer les genre_name en genre_id
    valid_genres, missing_genre_rejects = reject_left_anti(
        anime_genres,
        dim_genre.select("genre_name"),
        ["genre_name"],
        "genre not found in dim_genre",
    )
    bridge = valid_genres.join(dim_genre, on="genre_name", how="inner").select(
        "anime_id", "genre_id"
    )

    valid, null_rejects = split_null_rejects(bridge, ["anime_id", "genre_id"])
    valid, missing_anime_rejects = reject_left_anti(
        valid,
        dim_anime,
        ["anime_id"],
        "anime_id not found in dim_anime",
    )
    valid, duplicate_rejects = split_duplicate_rejects(valid, ["anime_id", "genre_id"])
    rejects = (
        missing_genre_rejects.unionByName(null_rejects, allowMissingColumns=True)
        .unionByName(missing_anime_rejects, allowMissingColumns=True)
        .unionByName(duplicate_rejects, allowMissingColumns=True)
    )
    rejected_count = append_rejects(
        rejects, JDBC_URL, JDBC_PROPS, "gold_bridge_anime_genre", "bridge_anime_genre"
    )

    inserted_count = write_staging_then_replace(
        valid,
        JDBC_URL,
        JDBC_PROPS,
        staging_table="stg_bridge_anime_genre",
        target_table="bridge_anime_genre",
        columns=["anime_id", "genre_id"],
    )

    print(
        f"gold_bridge_anime_genre : {inserted_count} liaisons ecrites, "
        f"{rejected_count} lignes ignorees dans reject_records"
    )

    spark.stop()
