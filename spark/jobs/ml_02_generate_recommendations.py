"""
Génère les top-N recommandations par utilisateur via ALS et écrit dans Postgres.

Pipeline :
  1. Charge le modèle ALS entraîné par ml_01
  2. Top-(N*5) candidats par user via ALS.recommendForAllUsers
  3. Exclut les animes déjà notés (left_anti join)
  4. Garde top-N final par user
  5. Écrit dans la table `recommendations` de Postgres
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALSModel
from db_write_utils import append_rejects, reject_left_anti, split_duplicate_rejects, split_null_rejects, write_staging_then_replace

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_PASSWORD")

DB_USER = os.environ.get("DB_ML_USER", "data_scientist")
DB_PASSWORD = os.environ.get("DB_ML_PASSWORD")
DB_NAME = os.environ.get("DB_BUSINESS_NAME")
JDBC_URL = f"jdbc:postgresql://postgres-business:5432/{DB_NAME}"

JDBC_PROPS = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}

TOP_N = 10
OVERSAMPLE = 5  # demande TOP_N * 5 candidats puis exclut les déjà-vus


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ml_02_generate_recommendations")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


if __name__ == "__main__":
    spark = build_spark_session()

    # ========================================================================
    # 1. Charger modèle ALS + déjà-vus
    # ========================================================================
    als_model = ALSModel.load("s3a://ml-models/als_latest/")

    already_seen = (
        spark.read.parquet("s3a://silver/rating/")
        .filter(F.col("rating").isNotNull())
        .select(
            F.col("user_id").cast("int"),
            F.col("anime_id").cast("int"),
        )
        .distinct()
    )

    # ========================================================================
    # 2. Top-K candidats par user (avec oversample)
    # ========================================================================
    print(f"Génération top-{TOP_N * OVERSAMPLE} par user via ALS...")
    raw_recs = als_model.recommendForAllUsers(TOP_N * OVERSAMPLE)

    candidates = raw_recs.select(
        F.col("user_id"),
        F.explode("recommendations").alias("rec"),
    ).select(
        "user_id",
        F.col("rec.anime_id").alias("anime_id"),
        F.col("rec.rating").alias("predicted_rating"),
    )

    # ========================================================================
    # 3. Exclure les animes déjà notés
    # ========================================================================
    candidates = candidates.join(
        already_seen, on=["user_id", "anime_id"], how="left_anti"
    )

    # ========================================================================
    # 4. Top-N final par user
    # ========================================================================
    w = Window.partitionBy("user_id").orderBy(F.col("predicted_rating").desc())
    final = (
        candidates.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= TOP_N)
        .select("user_id", "anime_id", "predicted_rating", "rank")
    )

    dim_anime = spark.read.jdbc(
        url=JDBC_URL,
        table="dim_anime",
        properties=JDBC_PROPS,
    ).select("anime_id")

    valid, null_rejects = split_null_rejects(final, ["user_id", "anime_id", "rank"])
    valid, missing_anime_rejects = reject_left_anti(
        valid,
        dim_anime,
        ["anime_id"],
        "anime_id not found in dim_anime",
    )
    valid, duplicate_rejects = split_duplicate_rejects(valid, ["user_id", "rank"])
    rejects = (
        null_rejects.unionByName(missing_anime_rejects, allowMissingColumns=True)
        .unionByName(duplicate_rejects, allowMissingColumns=True)
    )
    rejected_count = append_rejects(
        rejects, JDBC_URL, JDBC_PROPS, "ml_02_generate_recommendations", "recommendations"
    )

    # ========================================================================
    # 5. Écriture Postgres (truncate pour préserver les FK)
    # ========================================================================
    inserted_count = write_staging_then_replace(
        valid,
        JDBC_URL,
        JDBC_PROPS,
        staging_table="stg_recommendations",
        target_table="recommendations",
        columns=["user_id", "anime_id", "predicted_rating", "rank"],
    )
    print(
        f"Recommandations ecrites dans Postgres : {inserted_count:,}, "
        f"{rejected_count} lignes ignorees dans reject_records."
    )

    spark.stop()
