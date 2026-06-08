from datetime import datetime, timedelta
import os
import boto3
from botocore.client import Config
from airflow import DAG
from airflow.operators.python import PythonOperator

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
    "retries": 1,
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
    print(f"Uploadé : {filename} → s3://{BRONZE_BUCKET}/{s3_key} ({size} octets)")


with DAG(
    dag_id="dag_bronze_ingestion",
    description="Ingestion des fichiers CSV locaux vers le bucket bronze MinIO",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=["bronze", "ingestion"],
) as dag:

    tasks = []
    for filename, s3_key in FILES.items():
        task = PythonOperator(
            task_id=f"upload_{filename.replace('.', '_')}",
            python_callable=upload_file_to_bronze,
            op_args=[filename, s3_key],
        )
        tasks.append(task)

    # Exécution en séquence pour éviter de surcharger MinIO
    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]
