"""
DAG complet : enchaine tout le pipeline dans un seul DAG Airflow.

Les DAGs separes restent disponibles pour rejouer une couche seule, mais ce DAG
orchestre directement les tasks pour eviter l'attente de DAG runs externes.
"""
import os
from datetime import datetime, timedelta

import boto3
from botocore.client import Config
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup


MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.environ.get("MINIO_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_PASSWORD")
BRONZE_BUCKET = "bronze"
DATA_DIR = "/opt/airflow/data"

FILES = {
    "anime.csv": "anime/anime.csv",
    "rating.csv": "rating/rating.csv",
}

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


def upload_file_to_bronze(filename: str, s3_key: str) -> None:
    local_path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Fichier introuvable : {local_path}")

    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    client.upload_file(local_path, BRONZE_BUCKET, s3_key)
    size = os.path.getsize(local_path)
    print(f"Uploade : {filename} -> s3://{BRONZE_BUCKET}/{s3_key} ({size} octets)")


with DAG(
    dag_id="dag_full_pipeline",
    description="Pipeline complet : Bronze -> Silver -> Gold -> ML",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=["master", "pipeline"],
) as dag:

    spark_s3_conf = {
        "spark.hadoop.fs.s3a.endpoint": os.environ.get("MINIO_ENDPOINT"),
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
    spark_jars_with_pg = spark_jars_minio + ",/opt/spark/extra-jars/postgresql-42.7.3.jar"

    with TaskGroup(group_id="bronze") as bronze:
        upload_anime = PythonOperator(
            task_id="upload_anime_csv",
            python_callable=upload_file_to_bronze,
            op_args=["anime.csv", FILES["anime.csv"]],
        )

        upload_rating = PythonOperator(
            task_id="upload_rating_csv",
            python_callable=upload_file_to_bronze,
            op_args=["rating.csv", FILES["rating.csv"]],
        )

        upload_anime >> upload_rating

    with TaskGroup(group_id="silver") as silver:
        silver_clean_anime = SparkSubmitOperator(
            task_id="clean_anime",
            application="/opt/spark/jobs/silver_clean_anime.py",
            conn_id="spark_default",
            conf=spark_s3_conf,
            jars=spark_jars_minio,
        )

        silver_clean_rating = SparkSubmitOperator(
            task_id="clean_rating",
            application="/opt/spark/jobs/silver_clean_rating.py",
            conn_id="spark_default",
            conf=spark_s3_conf,
            jars=spark_jars_minio,
        )

        silver_clean_anime >> silver_clean_rating

    with TaskGroup(group_id="gold") as gold:
        gold_dim_anime = SparkSubmitOperator(
            task_id="dim_anime",
            application="/opt/spark/jobs/gold_dim_anime.py",
            conn_id="spark_default",
            conf=spark_s3_conf,
            jars=spark_jars_with_pg,
        )

        gold_dim_genre = SparkSubmitOperator(
            task_id="dim_genre",
            application="/opt/spark/jobs/gold_dim_genre.py",
            conn_id="spark_default",
            conf=spark_s3_conf,
            jars=spark_jars_with_pg,
        )

        gold_bridge_anime_genre = SparkSubmitOperator(
            task_id="bridge_anime_genre",
            application="/opt/spark/jobs/gold_bridge_anime_genre.py",
            conn_id="spark_default",
            conf=spark_s3_conf,
            jars=spark_jars_with_pg,
        )

        gold_fact_ratings = SparkSubmitOperator(
            task_id="fact_ratings",
            application="/opt/spark/jobs/gold_fact_ratings.py",
            conn_id="spark_default",
            conf=spark_s3_conf,
            jars=spark_jars_with_pg,
        )

        gold_dim_user = SparkSubmitOperator(
            task_id="dim_user",
            application="/opt/spark/jobs/gold_dim_user.py",
            conn_id="spark_default",
            jars=spark_jars_with_pg,
        )

        (
            gold_dim_anime
            >> gold_dim_genre
            >> gold_bridge_anime_genre
            >> gold_fact_ratings
            >> gold_dim_user
        )

    with TaskGroup(group_id="ml") as ml:
        ml_train_als = SparkSubmitOperator(
            task_id="train_als",
            application="/opt/spark/jobs/ml_01_train_als.py",
            conn_id="spark_default",
            conf=spark_s3_conf,
            jars=spark_jars_minio,
        )

        ml_generate_recommendations = SparkSubmitOperator(
            task_id="generate_recommendations",
            application="/opt/spark/jobs/ml_02_generate_recommendations.py",
            conn_id="spark_default",
            conf=spark_s3_conf,
            jars=spark_jars_with_pg,
        )

        ml_train_als >> ml_generate_recommendations

    bronze >> silver >> gold >> ml
