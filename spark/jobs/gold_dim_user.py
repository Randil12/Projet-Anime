import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from db_write_utils import append_rejects, split_duplicate_rejects, split_null_rejects, write_staging_then_replace

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
    # Pas besoin de config S3A ici : on ne lit/écrit que dans Postgres
    return SparkSession.builder.appName("gold_dim_user").getOrCreate()


if __name__ == "__main__":
    spark = build_spark_session()

    fact = spark.read.jdbc(
        url=JDBC_URL,
        table="fact_ratings",
        properties=JDBC_PROPS,
    )

    # user_total_reviews : nb total d'animes dans la liste (notés OU non) → count(*)
    # user_average_given_rating : moyenne des vraies notes (AVG ignore les null)
    dim_user = fact.groupBy("user_id").agg(
        F.count("*").alias("user_total_reviews"),
        F.avg("rating").alias("user_average_given_rating"),
    )

    valid, null_rejects = split_null_rejects(dim_user, ["user_id"])
    valid, duplicate_rejects = split_duplicate_rejects(valid, ["user_id"])
    rejects = null_rejects.unionByName(duplicate_rejects)
    rejected_count = append_rejects(
        rejects, JDBC_URL, JDBC_PROPS, "gold_dim_user", "dim_user"
    )

    inserted_count = write_staging_then_replace(
        valid,
        JDBC_URL,
        JDBC_PROPS,
        staging_table="stg_dim_user",
        target_table="dim_user",
        columns=["user_id", "user_total_reviews", "user_average_given_rating"],
    )

    print(
        f"gold_dim_user : {inserted_count} utilisateurs ecrits, "
        f"{rejected_count} lignes ignorees dans reject_records"
    )

    spark.stop()
