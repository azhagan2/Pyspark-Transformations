# tests/test_purchase_behavior.py
import pytest
from pyspark.sql import SparkSession
from transformations.customer_ranking import transform_top_customers_sql,transform_dataframe


def get_test_spark_session(app_name="unit-tests"):
    return SparkSession.builder \
        .master("local[*]") \
        .appName(app_name) \
        .getOrCreate()


@pytest.fixture(scope="session")
def spark():
    return get_test_spark_session()


def test_transform_top_customers_sql(spark):
    orders_data = [
        ("cust1", "2024-06-01", 100.0, "ord1"),
        ("cust11", "2025-03-01", 150.0, "ord2"),
        ("cust211122", "2025-01-01", 2040.0, "ord31")
    ]
    customers_data = [
        ("cust11", "John", "Doe", "john11@example.com", "United States"),
        ("cust211122", "Jane", "Smith", "jane222@example.com", "United States")
    ]

    orders_df = spark.createDataFrame(orders_data, ["customer_id", "order_date", "total_amount", "order_id"])
    customers_df = spark.createDataFrame(customers_data, ["customer_id", "first_name", "last_name", "email", "country"])
    customers_df.createOrReplaceTempView("customers")
    orders_df.createOrReplaceTempView("orders")
    
    result_df = transform_top_customers_sql(spark)
    result = result_df.select("customer_id", "spending_rank").collect()
    print(result)
    print("test_transform_top_customers_sql completed")
    assert len(result) == 2
    assert all(row.spending_rank <= 10 for row in result)


""" def test_transform_top_customers_df(spark):
    orders_data = [
        ("cust1", "2024-06-01", 100.0, "ord1"),
        ("cust1", "2025-03-01", 150.0, "ord2"),
        ("cust2", "2025-01-01", 200.0, "ord3")
    ]
    customers_data = [
        ("cust1", "John", "Doe", "john@example.com", "United States"),
        ("cust2", "Jane", "Smith", "jane@example.com", "United States")
    ]

    orders_df = spark.createDataFrame(orders_data, ["customer_id", "order_date", "total_amount", "order_id"])
    customers_df = spark.createDataFrame(customers_data, ["customer_id", "first_name", "last_name", "email", "country"])
    customers_df.createOrReplaceTempView("customers")
    orders_df.createOrReplaceTempView("orders")
    result_df = transform_dataframe(orders_df,customers_df)
    result = result_df.select("customer_id", "spending_rank").collect()

    assert len(result) == 2
    assert all(row.spending_rank <= 10 for row in result) """
