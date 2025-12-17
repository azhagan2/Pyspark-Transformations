import pytest
from pyspark.sql import SparkSession
from glue_etl_pipeline.churn_prediction import transform_sql
from glue_etl_pipeline.churn_prediction import transform_dataframe
import pyspark.sql.functions as F

def get_test_spark_session(app_name="unit-tests"):
    return SparkSession.builder \
        .master("local[*]") \
        .appName(app_name) \
        .getOrCreate()

@pytest.fixture(scope="session")
def spark():
    return get_test_spark_session()

def test_transform_sql_churn_prediction(spark):
    # Sample orders data (customer_id, order_id, order_date)
    orders_data = [
        ("cust1", "ord1", "2024-01-01"),
        ("cust1", "ord2", "2024-02-01"),
        ("cust1", "ord3", "2024-03-01"),
        ("cust2", "ord4", "2024-01-01"),   # only 1 order
        ("cust3", "ord5", "2024-01-01"),
        ("cust3", "ord6", "2024-01-10"),
    ]

    orders_df = spark.createDataFrame(
        orders_data, ["customer_id", "order_id", "order_date"]
    )

    # Register as temp view because transform_sql expects Spark SQL table `orders`
    orders_df.createOrReplaceTempView("orders")

    # Call churn prediction transformation
    result_df = transform_sql(spark)

    # Collect results
    result = result_df.select("customer_id", "days_since_last_purchase", "avg_order_gap").collect()

    # Basic checks
    assert result_df.count() >= 1  # at least 1 churn candidate
    assert "customer_id" in result_df.columns
    assert "days_since_last_purchase" in result_df.columns
    assert "avg_order_gap" in result_df.columns

    # Verify churn condition logic: days_since_last_purchase > avg_order_gap * 2
    for row in result:
        assert row.days_since_last_purchase > (row.avg_order_gap * 2)



def test_transform_dataframe_churn_prediction(spark):
    orders_data = [
        ("cust1", "ord1", "2024-01-01"),
        ("cust1", "ord2", "2024-02-01"),
        ("cust1", "ord3", "2024-03-01"),
        ("cust2", "ord4", "2024-01-01"),
    ]

    orders_df = spark.createDataFrame(
        orders_data, ["customer_id", "order_id", "order_date"]
    ).withColumn("order_date", F.col("order_date").cast("date"))

    result_df = transform_dataframe(orders_df)

    assert "customer_id" in result_df.columns
    assert result_df.count() >= 1
