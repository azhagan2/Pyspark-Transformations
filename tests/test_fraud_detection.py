
# tests/test_fraud_detection.py
import pytest
from pyspark.sql import SparkSession
from datetime import date, timedelta
from glue_etl_pipeline.fraud_detection import transform_sql, transform_dataframe


def get_test_spark_session(app_name="unit-tests"):
    return SparkSession.builder \
        .master("local[*]") \
        .appName(app_name) \
        .getOrCreate()

@pytest.fixture(scope="session")
def spark():
    return get_test_spark_session()


def test_transform_sql_high_risk(spark):
    today = date.today()
    thirty_days_ago = today - timedelta(days=10)

    # Orders data (customer_id, order_id, order_date, total_amount)
    orders_data = [
        ("cust1", "ord1", str(thirty_days_ago), 6000.0),   # high spent
        ("cust2", "ord2", str(thirty_days_ago), 100.0),
        ("cust3", "ord3", str(thirty_days_ago), 2000.0),
    ]

    # Login history data (customer_id, login_date, ip_address)
    logins_data = [
        ("cust1", str(thirty_days_ago), "1.1.1.1"),
        ("cust1", str(thirty_days_ago), "2.2.2.2"),
        ("cust1", str(thirty_days_ago), "3.3.3.3"),
        ("cust1", str(thirty_days_ago), "4.4.4.4"),  # >3 IPs
        ("cust2", str(thirty_days_ago), "5.5.5.5"),
    ]

    orders_df = spark.createDataFrame(
        orders_data, ["customer_id", "order_id", "order_date", "total_amount"]
    )
    logins_df = spark.createDataFrame(
        logins_data, ["customer_id", "login_date", "ip_address"]
    )

    # Register as temp views for transform_sql
    orders_df.createOrReplaceTempView("Orders")
    logins_df.createOrReplaceTempView("LoginHistory")

    result_df = transform_sql(spark)
    result = [r.customer_id for r in result_df.collect()]

    assert "cust1" in result  # high spent + suspicious logins
    assert "cust2" not in result
    assert "cust3" not in result


def test_transform_dataframe_high_risk(spark):
    today = date.today()
    thirty_days_ago = today - timedelta(days=5)

    orders_data = [
        ("cust10", "ord10", str(thirty_days_ago), 6000.0),
        ("cust20", "ord20", str(thirty_days_ago), 100.0),
    ]
    logins_data = [
        ("cust10", str(thirty_days_ago), "11.1.1.1"),
        ("cust10", str(thirty_days_ago), "22.2.2.2"),
        ("cust10", str(thirty_days_ago), "33.3.3.3"),
        ("cust10", str(thirty_days_ago), "44.4.4.4"),  # >3 IPs
    ]

    orders_df = spark.createDataFrame(
        orders_data, ["customer_id", "order_id", "order_date", "total_amount"]
    )
    logins_df = spark.createDataFrame(
        logins_data, ["customer_id", "login_date", "ip_address"]
    )

    result_df = transform_dataframe(orders_df, logins_df)
    result = [r.customer_id for r in result_df.collect()]

    assert "cust10" in result
    assert "cust20" not in result
