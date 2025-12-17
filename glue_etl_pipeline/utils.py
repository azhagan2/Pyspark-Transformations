import logging
import boto3
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from datetime import datetime
from pyspark.sql.functions import col, lit


def get_glue_logger():
    """
    Returns a logger configured for AWS Glue.
    """
    logger = logging.getLogger("glue_etl_pipeline")
    logger.setLevel(logging.INFO)
    return logger




def read_from_s3(glue_context: GlueContext, s3_path: str, format="csv", options=None) -> DataFrame:
    """
    Reads data from an S3 location.
    """
    options = options or {"header": "true"}
    return glue_context.read.format(format).options(**options).load(s3_path)

def write_to_s3(df: DataFrame, s3_path: str, format="parquet", mode="overwrite"):
    """
    Writes a Spark DataFrame to an S3 location.
    """
    print(f"Write data to S3 Started: {s3_path}")
    df.show(10)
    print(df.count())
    df.write.mode(mode).format(format).save(s3_path)
    print(f"Write data to S3 Completed: {s3_path}")


def write_to_s3_create_table(df: DataFrame,s3_path: str,output_db: str, tableName: str, format="parquet", mode="overwrite"):
    """
    Writes a Spark DataFrame to an S3 location.
    """
    print(f"Write data to S3 Started: {s3_path}")
    df.show(10)
    print(df.count())
    df.write.mode(mode).format(format).save(s3_path)
    df.write.format(format) .mode(mode) .option("path", s3_path).saveAsTable(f"{output_db}.{tableName}")
    print(f"Write data to S3 Completed: {s3_path}")

def validate_data_quality(df, table_name, key_column=None):
    record_count = df.count()
    null_count = df.filter(col(key_column).isNull()).count() if key_column else 0
    if record_count == 0:
        raise Exception(f"[DQ CHECK] {table_name} has 0 records.")
    if key_column and null_count > 0:
        raise Exception(f"[DQ CHECK] {table_name} has {null_count} NULLs in {key_column}.")
    return record_count

def write_audit_log(spark, job_name,s3_output_path, status, customer_count, start_time, end_time):
    audit_df = spark.createDataFrame([
        (job_name, status, customer_count, start_time, end_time, datetime.now())
    ], ["job_name", "status", "record_count", "start_time", "end_time", "log_ts"])
    #audit_df.write.mode("append").insertInto("enterprise_db.audit_log")
    audit_df.write \
    .mode("append") \
    .parquet(f"{s3_output_path}/audit_log")
