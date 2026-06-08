import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_gold_aggregate",
    description="Silver → Gold (star schema dans Postgres) via SparkSubmitOperator",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=["gold", "star-schema"],
) as dag:

    MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")

    # Config S3A pour lire depuis MinIO
    spark_s3_conf = {
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": os.environ.get("MINIO_USER"),
        "spark.hadoop.fs.s3a.secret.key": os.environ.get("MINIO_PASSWORD"),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    }

    # JARs : S3A + driver JDBC PostgreSQL
    spark_jars = ",".join([
        "/opt/spark/extra-jars/hadoop-aws-3.3.4.jar",
        "/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar",
        "/opt/spark/extra-jars/wildfly-openssl-1.0.7.Final.jar",
        "/opt/spark/extra-jars/postgresql-42.7.3.jar",
    ])

    dim_anime = SparkSubmitOperator(
        task_id="gold_dim_anime",
        application="/opt/spark/jobs/gold_dim_anime.py",
        conn_id="spark_default",
        conf=spark_s3_conf,
        jars=spark_jars,
    )

    dim_genre = SparkSubmitOperator(
        task_id="gold_dim_genre",
        application="/opt/spark/jobs/gold_dim_genre.py",
        conn_id="spark_default",
        conf=spark_s3_conf,
        jars=spark_jars,
    )

    bridge_anime_genre = SparkSubmitOperator(
        task_id="gold_bridge_anime_genre",
        application="/opt/spark/jobs/gold_bridge_anime_genre.py",
        conn_id="spark_default",
        conf=spark_s3_conf,
        jars=spark_jars,
    )

    fact_ratings = SparkSubmitOperator(
        task_id="gold_fact_ratings",
        application="/opt/spark/jobs/gold_fact_ratings.py",
        conn_id="spark_default",
        conf=spark_s3_conf,
        jars=spark_jars,
    )

    dim_user = SparkSubmitOperator(
        task_id="gold_dim_user",
        application="/opt/spark/jobs/gold_dim_user.py",
        conn_id="spark_default",
        jars=spark_jars,
    )

    # Exécution séquentielle (1 seul Spark worker → pas de gain à paralléliser)
    # Ordre respectant les FK : dim_anime + dim_genre avant bridge, dim_anime avant fact, fact avant dim_user
    dim_anime >> dim_genre >> bridge_anime_genre >> fact_ratings >> dim_user
