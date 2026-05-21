import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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

    (dim_user.write
        .mode("overwrite")
        .option("truncate", "true")
        .jdbc(url=JDBC_URL, table="dim_user", properties=JDBC_PROPS))

    print(f"gold_dim_user : {dim_user.count()} utilisateurs écrits")

    spark.stop()
