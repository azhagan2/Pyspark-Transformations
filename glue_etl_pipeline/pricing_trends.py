import sys
import boto3
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.functions import col, avg, sum, date_format, year, month,count,when,max
from pyspark.sql.window import Window
from pyspark.sql.functions import year,month
from glue_etl_pipeline.utils import get_glue_logger,write_to_s3


# Parse job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TARGET_PATH', 'INPUT_DB'])
bronze_db = args['INPUT_DB']

# Initialize Spark and Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
s3_output_path =args['S3_TARGET_PATH'] +args["JOB_NAME"]

# Initialize Logger
logger = get_glue_logger()

def run_etl():
    try:
        customer_df = spark.read.table(f"{bronze_db}.customers")
        order_df = spark.read.table(f"{bronze_db}.orders")
        products_df = spark.read.table(f"{bronze_db}.products")
        order_items_df = spark.read.table(f"{bronze_db}.order_items")

        products_df.createOrReplaceTempView("product")
        customer_df.createOrReplaceTempView("customers")
        order_df.createOrReplaceTempView("orders")
        order_items_df.createOrReplaceTempView("order_items") 
        (peak_sales_months,top_customers,customer_purchase_trend_df,retention_rate_pct,month_over_month_growth_pct,yoy_comparison_df,customer_segmented) = transform_sql(customer_df,order_df,products_df,order_items_df)

        write_to_s3(peak_sales_months,s3_output_path + "/peak_sales_months")
        write_to_s3(top_customers,s3_output_path + "/top_customers")
        write_to_s3(customer_purchase_trend_df,s3_output_path + "/customer_purchase_trend")
        write_to_s3(retention_rate_pct,s3_output_path + "/retention_rate_pct")
        write_to_s3(month_over_month_growth_pct,s3_output_path + "/month_over_month_growth_pct")
        write_to_s3(yoy_comparison_df,s3_output_path + "/yoy_comparison")
        write_to_s3( customer_segmented,s3_output_path + "/customer_segmented")
        print("ETL Job Completed Successfully")
    except Exception as e:
        print(f"ETL Job Failed: {str(e)}")
        raise e
    job.commit()



def transform_sql(customer_df,order_df,products_df,order_items_df):
    sales_data = order_df.join(order_items_df, "order_id", "inner") \
                        .join(products_df, "product_id", "inner") \
                        .select(
                            order_df.order_id,
                            order_df.customer_id,
                            order_df.order_date,
                            order_items_df.product_id,
                            order_items_df.quantity,
                            order_items_df.unit_price,
                            (order_items_df.quantity * order_items_df.unit_price).alias("sales_amount")
                        )
    sales_data.show(truncate=False)
    sales_data = sales_data.withColumn("order_year", year(col("order_date"))) .withColumn("order_month", month(col("order_date"))) 

    sales_data.printSchema()
    historical_customer_sales_df=spark.read.parquet("s3://customer360-az/bronze/mysql-data/UserService/historical_customer_sales/").repartition(4)
    # historical_customer_sales_df = spark.read.table(f"{bronze_db}.historical_customer_sales")
    historical_customer_sales_df.printSchema()
    historical_customer_sales_df.count()

    historical_casted = historical_customer_sales_df.select(
        col("order_id").cast("integer"),
        col("customer_id").cast("integer"),
        col("order_date").cast("timestamp"),  # convert date to timestamp
        col("product_id").cast("integer"),
        col("quantity").cast("integer"),
        col("unit_price").cast("decimal(10,2)"),
        col("sales_amount").cast("decimal(21,2)"),
        col("order_year").cast("integer"),
        col("order_month").cast("integer")
    )

    sales_data_union = sales_data.union(historical_casted)
    sales_data_union.createOrReplaceTempView("historical_sales_table")
    sales_data_union.cache()
    sales_data_union.count() 
    sales_data_union.show()

    peak_sales_months = spark.sql(""" 
                        SELECT
                        date_format(order_date, 'yyyy-MM') AS year_month,
                        SUM(sales_amount) AS monthly_product_sales
                        FROM historical_sales_table
                        GROUP BY  year_month
                        ORDER BY  monthly_product_sales desc
                """)
    peak_sales_months.show(5)

    top_customers = spark.sql("""
                        WITH top_customers AS (
                            SELECT customer_id
                            FROM historical_sales_table
                            GROUP BY customer_id
                            ORDER BY SUM(sales_amount) DESC
                            LIMIT 5
                        )
                            SELECT
                            s.customer_id,
                            date_format(order_date, 'yyyy-MM') AS year_month,
                            COUNT(s.product_id),
                            p.name AS product_name,
                            p.category,
                            SUM(s.sales_amount) AS monthly_sales
                            FROM historical_sales_table s
                            JOIN top_customers t ON s.customer_id = t.customer_id
                            JOIN product p ON s.product_id = p.product_id
                            GROUP BY s.customer_id, year_month, s.product_id, p.name, p.category
                            ORDER BY s.customer_id, monthly_sales desc;

    """)
    top_customers.show(10)

    customer_purchase_trend_df = spark.sql(""" 
                    WITH customer_monthly AS (
                            SELECT
                            customer_id,
                            order_year,
                            order_month,
                            COUNT(DISTINCT order_id) AS orders_per_month,
                            SUM(sales_amount) AS monthly_spent,
                            AVG(sales_amount) AS avg_order_value
                        FROM historical_sales_table
                        GROUP BY customer_id, order_year, order_month
                    ),  

         customer_growth AS (
            SELECT *,
                LAG(monthly_spent) OVER (
                PARTITION BY customer_id ORDER BY order_year, order_month
                ) AS previous_month_spent
            FROM customer_monthly
            )

    SELECT
    customer_id,
    order_year,
    order_month,
    orders_per_month,
    monthly_spent,
    previous_month_spent,
    ROUND(100 * (monthly_spent - previous_month_spent) / NULLIF(previous_month_spent, 0), 2)
        AS spending_growth_pct,
    avg_order_value
    FROM customer_growth
    ORDER BY customer_id, spending_growth_pct;

    """)
    customer_purchase_trend_df.filter("customer_id==2865").show(100)


    retention_rate_pct = spark.sql("""WITH distinct_customers_per_year AS (
    SELECT DISTINCT customer_id, order_year
    FROM historical_sales_table
    ),

    retention_pairs AS (
    SELECT
        curr.order_year AS current_year,
        COUNT(DISTINCT curr.customer_id) AS total_customers_current_year,
        COUNT(DISTINCT CASE WHEN prev.customer_id IS NOT NULL THEN curr.customer_id END) AS retained_customers
    FROM distinct_customers_per_year curr
    LEFT JOIN distinct_customers_per_year prev
        ON curr.customer_id = prev.customer_id
        AND curr.order_year = prev.order_year + 1
    GROUP BY curr.order_year
    )

    SELECT
    current_year,
    total_customers_current_year,
    retained_customers,
    ROUND(100.0 * retained_customers / NULLIF(total_customers_current_year, 0), 2) AS retention_rate_pct
    FROM retention_pairs
    ORDER BY current_year;

    """)
    retention_rate_pct.show(100)

    month_over_month_growth_pct = spark.sql("""WITH monthly_summary AS (
    SELECT
        order_year,
        order_month,
        product_id,
        SUM(quantity) AS total_quantity_sold,
        SUM(sales_amount) AS total_sales_amount
    FROM historical_sales_table
    GROUP BY order_year, order_month, product_id
    ),

    growth_calc AS (
    SELECT *,
        LAG(total_sales_amount) OVER (
        PARTITION BY product_id
        ORDER BY order_year, order_month
        ) AS previous_month_sales
    FROM monthly_summary
    )

    SELECT
    g.order_year,
    g.order_month,
    g.product_id,
    p.name AS product_name,
    p.category,
    g.total_quantity_sold,
    g.total_sales_amount,
    g.previous_month_sales,
    ROUND(
        100 * (g.total_sales_amount - g.previous_month_sales) / NULLIF(g.previous_month_sales, 0),
        2
    ) AS month_over_month_growth_pct
    FROM growth_calc g
    JOIN product p ON g.product_id = p.product_id
    ORDER BY g.product_id, g.order_year, g.order_month;


    """)
    month_over_month_growth_pct.show(10)


    yoy_comparison_df = spark.sql(""" 
    WITH top_customers AS (
    SELECT customer_id
    FROM historical_sales_table
    GROUP BY customer_id
    ORDER BY SUM(sales_amount) DESC
    LIMIT 5
    ),

    sales_by_month AS (
    SELECT
        s.customer_id,
        s.product_id,
        p.name AS product_name,
        p.category,
        EXTRACT(YEAR FROM s.order_date) AS sales_year,
        EXTRACT(MONTH FROM s.order_date) AS sales_month,
        SUM(s.sales_amount) AS monthly_sales
    FROM historical_sales_table s
    JOIN top_customers t ON s.customer_id = t.customer_id
    JOIN product p ON s.product_id = p.product_id
    GROUP BY s.customer_id, s.product_id, p.name, p.category, EXTRACT(YEAR FROM s.order_date), EXTRACT(MONTH FROM s.order_date)
    ),

    yoy_comparison AS (
    SELECT
        customer_id,
        product_id,
        product_name,
        category,
        sales_year,
        sales_month,
        monthly_sales,
        LAG(monthly_sales) OVER (
        PARTITION BY customer_id, product_id, sales_month
        ORDER BY sales_year
        ) AS prev_year_sales
    FROM sales_by_month
    )

    SELECT
    customer_id,
    product_id,
    product_name,
    category,
    sales_year,
    sales_month,
    monthly_sales,
    prev_year_sales,
    ROUND(
        CASE
        WHEN prev_year_sales IS NULL OR prev_year_sales = 0 THEN NULL
        ELSE ((monthly_sales - prev_year_sales) / prev_year_sales) * 100
        END, 2
    ) AS yoy_growth_percent
    FROM yoy_comparison
    ORDER BY customer_id,  yoy_growth_percent desc;


    """)
    yoy_comparison_df.show(10)

    customer_segmented = sales_data.groupBy("customer_id").agg(
        count("order_id").alias("purchase_count"),   # Total number of orders
        sum("sales_amount").alias("total_spent"),max("order_date").alias("last_order_date")) # Most recent order date
    # collect_list("product_id").alias("product_ids")  # List of purchased product IDs


    customer_segmented = customer_segmented.withColumn(
        "customer_segment",
        when((col("purchase_count") >= 10) & (col("total_spent") >= 1000), "High-Value Customer")
        .when((col("purchase_count") >= 5) & (col("total_spent") >= 500), "Regular Customer")
        .otherwise("Occasional Buyer")
    )
    customer_segmented.show()

    return (peak_sales_months,top_customers,customer_purchase_trend_df,retention_rate_pct,month_over_month_growth_pct,yoy_comparison_df,customer_segmented)


def transform_dataframe(order_df,products_df,order_items_df):

    return ()