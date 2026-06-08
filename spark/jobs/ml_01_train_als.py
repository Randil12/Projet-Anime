import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_PASSWORD")

# Hyperparamètres ALS (ajustés pour plus de diversité dans les recos)
ALS_RANK = 30           # plus de facteurs latents → meilleure personnalisation
ALS_MAX_ITER = 15       # meilleure convergence
ALS_REG_PARAM = 0.05    # moins de régularisation → plus de signal user-spécifique
TRAIN_RATIO = 0.8
SEED = 42


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ml_01_train_als")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


if __name__ == "__main__":
    spark = build_spark_session()

    # 1. Lecture des notes (on exclut les null = vu mais pas noté)
    ratings = (
        spark.read.parquet("s3a://silver/rating/")
        .filter(F.col("rating").isNotNull())
        .select(
            F.col("user_id").cast("int"),
            F.col("anime_id").cast("int"),
            F.col("rating").cast("float"),
        )
    )
    total = ratings.count()
    print(f"Notes utilisables : {total:,}")

    # 2. Split train / test (80/20)
    train, test = ratings.randomSplit([TRAIN_RATIO, 1.0 - TRAIN_RATIO], seed=SEED)
    print(f"Train : {train.count():,}  |  Test : {test.count():,}")

    # 3. Entraînement ALS
    als = ALS(
        userCol="user_id",
        itemCol="anime_id",
        ratingCol="rating",
        rank=ALS_RANK,
        maxIter=ALS_MAX_ITER,
        regParam=ALS_REG_PARAM,
        coldStartStrategy="drop",  # évite les NaN lors de l'évaluation
        seed=SEED,
    )
    model = als.fit(train)
    print("Modèle ALS entraîné.")

    # 4. Évaluation sur le test set (RMSE)
    predictions = model.transform(test)
    rmse = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction",
    ).evaluate(predictions)
    print(f"RMSE test : {rmse:.4f}")

    # 5. Sauvegarde du modèle (lu ensuite par ml_02_generate_recommendations)
    model.write().overwrite().save("s3a://ml-models/als_latest/")
    print("Modèle sauvegardé → s3a://ml-models/als_latest/")

    spark.stop()
