"""
Pipeline ML simplifié : ALS → recommandations dans Postgres.

  1. ml_01_train_als                (Spark)  silver/rating → ALS model
  2. ml_02_generate_recommendations (Spark)  ALS + silver → Postgres recommendations
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_ml_recommendation",
    description="ALS → table recommendations (Postgres)",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=["ml", "recommendation"],
) as dag:

    MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")

    spark_s3_conf = {
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": os.environ.get("MINIO_USER"),
        "spark.hadoop.fs.s3a.secret.key": os.environ.get("MINIO_PASSWORD"),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    }

    spark_jars_minio = ",".join([
        "/opt/spark/extra-jars/hadoop-aws-3.3.4.jar",
        "/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar",
        "/opt/spark/extra-jars/wildfly-openssl-1.0.7.Final.jar",
    ])

    # Pour le job qui écrit dans Postgres : ajouter le driver JDBC
    spark_jars_with_pg = spark_jars_minio + ",/opt/spark/extra-jars/postgresql-42.7.3.jar"

    train_als = SparkSubmitOperator(
        task_id="ml_01_train_als",
        application="/opt/spark/jobs/ml_01_train_als.py",
        conn_id="spark_default",
        conf=spark_s3_conf,
        jars=spark_jars_minio,
    )

    generate_recos = SparkSubmitOperator(
        task_id="ml_02_generate_recommendations",
        application="/opt/spark/jobs/ml_02_generate_recommendations.py",
        conn_id="spark_default",
        conf=spark_s3_conf,
        jars=spark_jars_with_pg,
    )

    train_als >> generate_recos
