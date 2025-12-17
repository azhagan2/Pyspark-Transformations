import sys
import boto3
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from transformations.customer_ranking import transform_top_customers_sql, transform_dataframe    
from glue_etl_pipeline.utils import get_glue_logger,write_to_s3,write_to_s3_create_table,write_audit_log
from datetime import datetime


# Parse job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TARGET_PATH', 'INPUT_DB','OUTPUT_DB'])



# Initialize Spark and Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


s3_output_path =args['S3_TARGET_PATH'] +args["JOB_NAME"]
bronze_db = args['INPUT_DB']
output_db = args['OUTPUT_DB']


# Initialize Logger
logger = get_glue_logger()

def run_etl():
    try:
        start_time = datetime.now()
        print("Staring ETL Job " +args["JOB_NAME"])

        print(f"{bronze_db}.customers")

        customer_df = spark.read.table(f"{bronze_db}.customers")
        order_df = spark.read.table(f"{bronze_db}.orders")
        customer_df.createOrReplaceTempView("customer_raw")
        order_df.createOrReplaceTempView("orders_raw")

        customer_df.show()
        

        customer_clean_df=clean_customer_data(spark)
        customer_clean_df.createOrReplaceTempView("customer_clean")
        write_to_s3_create_table(customer_clean_df,args['S3_TARGET_PATH'] +args["JOB_NAME"]+'/customers_clean',output_db,'customers')

        orders_clean_df=clean_order_data(spark)
        write_to_s3_create_table(orders_clean_df,args['S3_TARGET_PATH'] +args["JOB_NAME"]+'/orders_clean',output_db,'orders')
        


        #top_customers=transform_dataframe(order_df,customer_df)
        product_df = spark.read.table(f"{bronze_db}.products")
        order_items_df = spark.read.table(f"{bronze_db}.order_items")
        loginhistory_df = spark.read.table(f"{bronze_db}.loginhistory")
        usage_history_df = spark.read.table(f"{bronze_db}.usage_history")

        customersupport_df = spark.read.table(f"{bronze_db}.customersupport")

        enterprisecampaigns_df = spark.read.table(f"{bronze_db}.enterprisecampaigns")
        historical_customer_sales_df = spark.read.table(f"{bronze_db}.historical_customer_sales")

        

        write_to_s3_create_table(order_items_df,args['S3_TARGET_PATH'] +args["JOB_NAME"]+'/order_items_clean',output_db,'order_items')
        
        write_to_s3_create_table(product_df,args['S3_TARGET_PATH'] +args["JOB_NAME"]+'/product_clean',output_db,'products')

        write_to_s3_create_table(loginhistory_df,args['S3_TARGET_PATH'] +args["JOB_NAME"]+'/loginhistory_clean',output_db,'loginhistory')
        
        write_to_s3_create_table(usage_history_df,args['S3_TARGET_PATH'] +args["JOB_NAME"]+'/usage_history_clean',output_db,'usage_history')
        
        write_to_s3_create_table(customersupport_df,args['S3_TARGET_PATH'] +args["JOB_NAME"]+'/customersupport_clean',output_db,'customersupport')
        write_to_s3_create_table(enterprisecampaigns_df,args['S3_TARGET_PATH'] +args["JOB_NAME"]+'/enterprisecampaigns_clean',output_db,'enterprisecampaigns')

        write_to_s3_create_table(historical_customer_sales_df,args['S3_TARGET_PATH'] +args["JOB_NAME"]+'/historical_customer_sales_clean',output_db,'historical_customer_sales')


        print("ETL Job Completed Successfully")
        end_time = datetime.now()
        write_audit_log(spark, args['JOB_NAME'],s3_output_path, "SUCCESS", customer_clean_df.count(), start_time, end_time)

    except Exception as e:
        end_time = datetime.now()
        print(f"ETL Job Failed: {str(e)}")
        write_audit_log(spark, args['JOB_NAME'],s3_output_path, "FAILURE", 0, start_time, end_time)
        raise e
    job.commit()


def clean_customer_data(spark):
    # Clean Customers
    customer_clean_sql = """
    SELECT DISTINCT
        customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,
        REGEXP_REPLACE(phone, '[^0-9]', '') AS phone,
        TRIM(address) AS address,
        city,
        state,
        CAST(zip_code AS STRING) AS zip_code,
        country
    FROM customer_raw
    WHERE customer_id IS NOT NULL
    AND email RLIKE '^[^@]+@[^@]+\\.[^@]+$'
    """

    customer_clean_df = spark.sql(customer_clean_sql)
    return customer_clean_df


def clean_order_data(spark):
    # Clean Orders
    orders_clean_sql = """
    SELECT DISTINCT
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        CAST(o.total_amount AS DECIMAL(10,2)) AS total_amount
    FROM orders_raw o
    JOIN customer_clean c
    ON o.customer_id = c.customer_id
    WHERE o.order_id IS NOT NULL
    AND o.customer_id IS NOT NULL
    AND o.total_amount > 0
    AND o.order_date <= current_date()
    """
    orders_clean_df = spark.sql(orders_clean_sql)
    return orders_clean_df