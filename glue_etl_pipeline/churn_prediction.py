import sys
import boto3
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from glue_etl_pipeline.utils import get_glue_logger, write_to_s3, write_audit_log
from datetime import datetime


def getSparkContext():
    # Parse job arguments
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TARGET_PATH', 'INPUT_DB'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    s3_output_path = args['S3_TARGET_PATH'] + args["JOB_NAME"]
    bronze_db = args['INPUT_DB']

    # Initialize Logger
    logger = get_glue_logger()

    return spark, job, args, s3_output_path, bronze_db, logger


def run_etl():
    spark, job, args, s3_output_path, bronze_db, logger = getSparkContext()

    start_time = datetime.now()
    try:
        order_df = spark.read.table(f"{bronze_db}.orders")
        order_df.createOrReplaceTempView("orders")

        # Common transformation
        churn_risk = transform_sql(spark)
        # churn_risk = transform_dataframe(order_df)

        write_to_s3(churn_risk, s3_output_path)

        end_time = datetime.now()
        write_audit_log(spark, args['JOB_NAME'],s3_output_path, "SUCCESS", churn_risk.count(), start_time, end_time)
        print("ETL Job Completed Successfully")

    except Exception as e:
        end_time = datetime.now()
        print(f"ETL Job Failed: {str(e)}")
        write_audit_log(spark, args['JOB_NAME'],s3_output_path, "FAILURE", 0, start_time, end_time)
        
        raise e
    finally:
        job.commit()


def transform_sql(spark):
    # Run Spark SQL Query
    churn_risk = spark.sql(""" 
        WITH customer_activity AS (
            SELECT
                customer_id,
                order_id,
                order_date,
                LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_order_date
            FROM orders
        ),
        churn_risk AS (
            SELECT
                customer_id,
                COUNT(order_id) AS total_orders,
                MAX(order_date) AS last_order_date,
                DATEDIFF(current_date, MAX(order_date)) AS days_since_last_purchase,  
                AVG(DATEDIFF(order_date, prev_order_date)) AS avg_order_gap  
            FROM customer_activity
            GROUP BY customer_id
        )
        SELECT *
        FROM churn_risk
        WHERE days_since_last_purchase > (avg_order_gap * 2)  
        ORDER BY days_since_last_purchase DESC
    """)
    return churn_risk


def transform_dataframe(order_df):
    # Define window specification for LAG function
    window_spec = Window.partitionBy("customer_id").orderBy("order_date")

    # Add previous order date using LAG function
    customer_activity = order_df.withColumn(
        "prev_order_date", F.lag("order_date").over(window_spec)
    )

    # Compute total orders, last order date, days since last purchase, and average order gap
    churn_risk = (
        customer_activity.groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.max("order_date").alias("last_order_date"),
            F.datediff(F.current_date(), F.max("order_date")).alias("days_since_last_purchase"),
            F.avg(F.datediff("order_date", "prev_order_date")).alias("avg_order_gap"),
        )
    )

    # Filter customers who are inactive for more than twice their average order gap
    churn_risk_filtered = churn_risk.filter(
        F.col("days_since_last_purchase") > (F.col("avg_order_gap") * 2)
    ).orderBy(F.desc("days_since_last_purchase"))

    churn_risk_filtered.show(10)
    return churn_risk_filtered
