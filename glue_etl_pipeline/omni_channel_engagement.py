import sys
import boto3
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, sum, when, coalesce, desc, date_sub, current_date
from pyspark.sql.window import Window
from glue_etl_pipeline.utils import get_glue_logger,write_to_s3


# Parse job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TARGET_PATH', 'INPUT_DB'])

# Initialize Spark and Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
s3_output_path =args['S3_TARGET_PATH'] +args["JOB_NAME"]
bronze_db = args['INPUT_DB']

# Initialize Logger
logger = get_glue_logger()

def run_etl():
    
    try:
        
        enterprise_campaign_raw = spark.read.table(f"{bronze_db}.enterprisecampaigns")
        customer_support_raw = spark.read.table(f"{bronze_db}.customersupport")
        orders_df = spark.read.table(f"{bronze_db}.orders")
        orders_df.createOrReplaceTempView("orders")

        exploded_df = enterprise_campaign_raw.withColumn("Interaction", F.explode(F.col("Interactions")))

        # Select exploded fields
        engagement_df  = exploded_df.select(
            F.col("CustomerID"),
            F.col("CampaignID"),
            F.col("Engagement.Opens"),
            F.col("Engagement.Clicks"),
            F.col("Interaction.InteractionID").alias("InteractionID"),
            F.col("Interaction.Timestamp").alias("Timestamp"),
            F.col("Interaction.Channel").alias("Channel"),
            F.col("Interaction.Action").alias("Action")
        )

        engagement_df .show()
        engagement_df .printSchema()
        engagement_df .createOrReplaceTempView("engagement")

        support_df=customer_support_raw.select("*","Issue.Status")
        support_df.createOrReplaceTempView("SupportTickets")
        support_df.printSchema()


        #common tranformation 
        result_df=transform_sql()
        #result_df=transform_dataframe(order_df,engagement_df,support_df)

        write_to_s3(result_df,s3_output_path)

        print("ETL Job Completed Successfully")
    except Exception as e:
        print(f"ETL Job Failed: {str(e)}")
        raise e
    job.commit()



def transform_sql():
    query = """
        WITH email_engagement AS (
            SELECT
                CustomerID,
                COUNT(CASE WHEN Action = 'Opened' AND Channel = 'Email' THEN 1 END) AS EmailsOpened,
                COUNT(CASE WHEN Action = 'Clicked' AND Channel = 'Email' THEN 1 END) AS EmailsClicked
            FROM engagement
            WHERE Timestamp >= date_add(current_date(), -90)
            GROUP BY CustomerID
        ), 
        ad_engagement AS (
            SELECT
                CustomerID,
                COUNT(CASE WHEN Action = 'Clicked' AND Channel IN ('Web', 'App') THEN 1 END) AS AdClicks,
                COUNT(CASE WHEN Action = 'Opened' AND Channel IN ('Web', 'App') THEN 1 END) AS AdViews
            FROM engagement
            WHERE Timestamp >= date_add(current_date(), -90)
            GROUP BY CustomerID
        ),
        purchase_activity AS (
            SELECT
                customer_id,
                COUNT(order_id) AS purchases,
                SUM(total_amount) AS total_spent
            FROM Orders
            WHERE order_date >= date_add(current_date(), -90)
            GROUP BY customer_id
        ), 
        support_analysis AS (
            SELECT
                CustomerID,
                COUNT(TicketID) AS TotalTickets,
                COUNT(CASE WHEN Status = 'Open' THEN 1 END) AS OpenTickets,
                COUNT(CASE WHEN Status = 'Resolved' THEN 1 END) AS ResolvedTickets
            FROM SupportTickets
            WHERE Timestamp >= date_add(current_date(), -90)
            GROUP BY CustomerID
        )
        SELECT
            customer_id,
            COALESCE(EmailsOpened, 0) AS emails_opened,
            COALESCE(EmailsClicked, 0) AS emails_clicked,
            COALESCE(AdClicks, 0) AS ad_clicks,
            COALESCE(AdViews, 0) AS ad_views,
            COALESCE(purchases, 0) AS purchases,
            COALESCE(total_spent, 0) AS total_spent,
            COALESCE(TotalTickets, 0) AS total_ticket,
            COALESCE(OpenTickets, 0) AS open_ticket,
            COALESCE(ResolvedTickets, 0) AS resolved_ticjet
        FROM email_engagement e
        FULL OUTER JOIN ad_engagement a ON e.CustomerID = a.CustomerID
        FULL OUTER JOIN purchase_activity p ON a.CustomerID = p.customer_id
        FULL OUTER JOIN support_analysis s ON a.CustomerID = s.CustomerID ORDER BY total_spent DESC;
        """

    result_df = spark.sql(query)
    result_df.show(100)

    return result_df



def transform_dataframe(order_df,engagement_df,support_df):


    # Email Engagement
    email_engagement_df = (
        engagement_df
        .filter(col("Timestamp") >= date_sub(current_date(), 90))
        .groupBy("CustomerID")
        .agg(
            count(when((col("Action") == "Opened") & (col("Channel") == "Email"), 1)).alias("EmailsOpened"),
            count(when((col("Action") == "Clicked") & (col("Channel") == "Email"), 1)).alias("EmailsClicked")
        )
    )

    # Ad Engagement
    ad_engagement_df = (
        engagement_df
        .filter(col("Timestamp") >= date_sub(current_date(), 90))
        .groupBy("CustomerID")
        .agg(
            count(when((col("Action") == "Clicked") & (col("Channel").isin("Web", "App")), 1)).alias("AdClicks"),
            count(when((col("Action") == "Opened") & (col("Channel").isin("Web", "App")), 1)).alias("AdViews")
        )
    )

    # Purchase Activity
    purchase_activity_df = (
        order_df
        .filter(col("order_date") >= date_sub(current_date(), 90))
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("purchases"),
            sum("total_amount").alias("total_spent")
        )
    )

    # Support Ticket Analysis
    support_analysis_df = (
        support_df
        .filter(col("Timestamp") >= date_sub(current_date(), 90))
        .groupBy("CustomerID")
        .agg(
            count("TicketID").alias("TotalTickets"),
            count(when(col("Status") == "Open", 1)).alias("OpenTickets"),
            count(when(col("Status") == "Resolved", 1)).alias("ResolvedTickets")
        )
    )

    # Full Outer Joins
    final_df = (
        email_engagement_df.alias("e")
        .join(ad_engagement_df.alias("a"), col("e.CustomerID") == col("a.CustomerID"), "fullouter")
        .join(purchase_activity_df.alias("p"), col("a.CustomerID") == col("p.customer_id"), "fullouter")
        .join(support_analysis_df.alias("s"), col("a.CustomerID") == col("s.CustomerID"), "fullouter")
        .select(
            coalesce(col("e.CustomerID"), col("a.CustomerID"), col("p.customer_id"), col("s.CustomerID")).alias("customer_id"),
            coalesce(col("EmailsOpened"), col("EmailsOpened"), col("EmailsOpened"), col("EmailsOpened")).alias("emails_opened"),
            coalesce(col("EmailsClicked"), col("EmailsClicked"), col("EmailsClicked"), col("EmailsClicked")).alias("emails_clicked"),
            coalesce(col("AdClicks"), col("AdClicks"), col("AdClicks"), col("AdClicks")).alias("ad_clicks"),
            coalesce(col("AdViews"), col("AdViews"), col("AdViews"), col("AdViews")).alias("ad_views"),
            coalesce(col("purchases"), col("purchases")).alias("purchases"),
            coalesce(col("total_spent"), col("total_spent")).alias("total_spent"),
            coalesce(col("TotalTickets"), col("TotalTickets")).alias("total_ticket"),
            coalesce(col("OpenTickets"), col("OpenTickets")).alias("open_ticket"),
            coalesce(col("ResolvedTickets"), col("ResolvedTickets")).alias("resolved_ticket")
        )
        .orderBy(desc("total_spent"))
    )

    # Show results
    final_df.show(10)


    return final_df