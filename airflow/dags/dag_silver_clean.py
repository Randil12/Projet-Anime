import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_silver_clean",
    description="Nettoyage Bronze → Silver via SparkSubmitOperator",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    tags=["silver", "clean"],
) as dag:

    # Récupération et nettoyage de l'URL MinIO (retrait du http:// requis par certaines versions de S3A)
    MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
    
    # Configuration Hadoop S3A commune pour MinIO
    spark_s3_conf = {
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": os.environ.get("MINIO_USER"),
        "spark.hadoop.fs.s3a.secret.key": os.environ.get("MINIO_PASSWORD"),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    }

    # Liste de tes JARs locaux (basée sur le chemin d'Airflow : /opt/spark/extra-jars)
    spark_jars = ",".join([
        "/opt/spark/extra-jars/hadoop-aws-3.3.4.jar",
        "/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar",
        "/opt/spark/extra-jars/wildfly-openssl-1.0.7.Final.jar",
    ])

    # Tâche 1 : Nettoyage Anime
    clean_anime = SparkSubmitOperator(
        task_id="silver_clean_anime",
        application="/opt/spark/jobs/silver_clean_anime.py",
        conn_id="spark_default",
        conf=spark_s3_conf,
        jars=spark_jars,
    )

    # Tâche 2 : Nettoyage Rating
    clean_rating = SparkSubmitOperator(
        task_id="silver_clean_rating",
        application="/opt/spark/jobs/silver_clean_rating.py",
        conn_id="spark_default",
        conf=spark_s3_conf,
        jars=spark_jars,
    )

    # Ordre de dépendance
    clean_anime >> clean_rating