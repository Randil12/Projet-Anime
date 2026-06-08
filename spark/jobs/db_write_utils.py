from __future__ import annotations

import os

import psycopg2
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


DB_HOST = os.environ.get("DB_BUSINESS_HOST", "postgres-business")
DB_PORT = int(os.environ.get("DB_BUSINESS_PORT", "5432"))
DB_NAME = os.environ.get("DB_BUSINESS_NAME")


def split_null_rejects(df: DataFrame, required_columns: list[str]) -> tuple[DataFrame, DataFrame]:
    invalid_condition = None
    for column in required_columns:
        condition = F.col(column).isNull()
        invalid_condition = condition if invalid_condition is None else invalid_condition | condition

    invalid = df.filter(invalid_condition).withColumn(
        "error_reason",
        F.concat(F.lit("missing required column(s): "), F.lit(", ".join(required_columns))),
    )
    valid = df.filter(~invalid_condition)
    return valid, invalid


def split_duplicate_rejects(df: DataFrame, key_columns: list[str]) -> tuple[DataFrame, DataFrame]:
    window = Window.partitionBy(*key_columns)
    with_counts = df.withColumn("__key_count", F.count("*").over(window))

    invalid = (
        with_counts.filter(F.col("__key_count") > 1)
        .drop("__key_count")
        .withColumn(
            "error_reason",
            F.concat(F.lit("duplicate key: "), F.lit(", ".join(key_columns))),
        )
    )
    valid = with_counts.filter(F.col("__key_count") == 1).drop("__key_count")
    return valid, invalid


def reject_left_anti(
    df: DataFrame,
    ref_df: DataFrame,
    key_columns: list[str],
    reason: str,
) -> tuple[DataFrame, DataFrame]:
    valid = df.join(ref_df.select(*key_columns).distinct(), on=key_columns, how="inner")
    invalid = df.join(ref_df.select(*key_columns).distinct(), on=key_columns, how="left_anti")
    invalid = invalid.withColumn("error_reason", F.lit(reason))
    return valid, invalid


def append_rejects(
    df: DataFrame,
    jdbc_url: str,
    jdbc_props: dict,
    job_name: str,
    target_table: str,
) -> int:
    count = df.count()
    if count == 0:
        return 0

    payload_columns = [column for column in df.columns if column != "error_reason"]
    rejects = df.select(
        F.lit(job_name).alias("job_name"),
        F.lit(target_table).alias("target_table"),
        F.col("error_reason"),
        F.to_json(F.struct(*[F.col(column) for column in payload_columns])).alias("payload"),
    )

    rejects.write.mode("append").jdbc(
        url=jdbc_url,
        table="reject_records",
        properties=jdbc_props,
    )
    return count


def write_staging_then_replace(
    df: DataFrame,
    jdbc_url: str,
    jdbc_props: dict,
    staging_table: str,
    target_table: str,
    columns: list[str],
    truncate_cascade: bool = False,
) -> int:
    count = df.count()
    user = jdbc_props["user"]
    password = jdbc_props["password"]
    column_sql = ", ".join(columns)
    cascade_sql = " CASCADE" if truncate_cascade else ""

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=user,
        password=password,
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {staging_table}")
    finally:
        conn.close()

    df.select(*columns).write.mode("append").jdbc(
        url=jdbc_url,
        table=staging_table,
        properties=jdbc_props,
    )

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=user,
        password=password,
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {target_table}{cascade_sql}")
                cur.execute(
                    f"""
                    INSERT INTO {target_table} ({column_sql})
                    SELECT {column_sql}
                    FROM {staging_table}
                    """
                )
                cur.execute(f"TRUNCATE TABLE {staging_table}")
    finally:
        conn.close()

    return count
