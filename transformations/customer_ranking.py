from pyspark.sql import functions as F
from pyspark.sql.window import Window



def transform_top_customers_sql(spark):
    return spark.sql("""
                WITH customer_spending AS (
                    SELECT
                        o.customer_id,
                        SUM(o.total_amount) AS total_spent,
                        COUNT(o.order_id) AS total_orders,
                        MAX(o.order_date) AS last_purchase_date
                    FROM orders o
                    WHERE o.order_date >= date_add(current_date(), -365)  -- Last 1 year
                    GROUP BY o.customer_id
                ),
                customer_ranking AS (
                    SELECT
                        c.country,
                        c.customer_id,
                        CONCAT(c.first_name, ' ', c.last_name) AS full_name,
                        c.email,
                        cs.total_spent,
                        cs.total_orders,
                        cs.last_purchase_date,
                        RANK() OVER (PARTITION BY c.country ORDER BY cs.total_spent DESC) AS spending_rank
                    FROM customer_spending cs
                    JOIN customers c ON cs.customer_id = c.customer_id
                )
                SELECT * FROM customer_ranking WHERE spending_rank <= 10 and country like 'United %';
                     


    """)



def transform_dataframe(order_df,customer_df):
    # Filter last 1 year of data
    one_year_ago = F.date_add(F.current_date(), -365)
    filtered_orders = order_df.filter(F.col("order_date") >= one_year_ago)

    # Aggregate customer spending
    customer_spending = (
        filtered_orders.groupBy("customer_id")
        .agg(
            F.sum("total_amount").alias("total_spent"),
            F.count("order_id").alias("total_orders"),
            F.max("order_date").alias("last_purchase_date")
        )
    )

    # Join with customers table
    customer_data = customer_spending.join(customer_df, "customer_id")

    # Define window specification for ranking
    window_spec = Window.partitionBy("country").orderBy(F.desc("total_spent"))

    # Add ranking column
    customer_ranking = customer_data.withColumn("spending_rank", F.rank().over(window_spec))

    # Filter top 100 customers
    top_customers = customer_ranking.filter( (F.col("spending_rank") <= 10) &  (F.col("country").like("United %")))

    # Show results
    top_customers.select("country","customer_id","first_name","email","total_spent","total_orders","spending_rank").show(20)

    return top_customers
