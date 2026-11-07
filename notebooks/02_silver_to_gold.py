# Databricks notebook source
from pyspark.sql import functions as F

storage_account = "stecdataengkk2026"

silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/"
gold_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/"

print("Silver and Gold paths configured!")

# COMMAND ----------

silver_tables = [
    "olist_customers_dataset",
    "olist_orders_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "product_category_name_translation"
]

silver_dfs = {}

for name in silver_tables:
    silver_dfs[name] = (
        spark.read
        .format("delta")
        .load(silver_path + name)
    )

print("All Silver Delta tables loaded successfully!")

# COMMAND ----------

from pyspark.sql import functions as F

orders = silver_dfs["olist_orders_dataset"]
items = silver_dfs["olist_order_items_dataset"]
customers = silver_dfs["olist_customers_dataset"]
products = silver_dfs["olist_products_dataset"]
payments = silver_dfs["olist_order_payments_dataset"]
reviews = silver_dfs["olist_order_reviews_dataset"]
translations = silver_dfs["product_category_name_translation"]

# Aggregate item values at order level first.
# This prevents revenue from being duplicated when joining payments.
order_totals = (
    items.groupBy("order_id")
    .agg(
        F.sum("price").alias("product_revenue"),
        F.sum("freight_value").alias("freight_revenue")
    )
)

# 1. Daily sales performance
daily_sales = (
    orders.join(order_totals, "order_id", "left")
    .groupBy(F.to_date("order_purchase_timestamp").alias("order_date"))
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("product_revenue").alias("product_revenue"),
        F.sum("freight_revenue").alias("freight_revenue"),
        F.countDistinct("customer_id").alias("unique_customers")
    )
)

# 2. Product category performance
category_data = (
    items.join(products, "product_id", "left")
    .join(translations, "product_category_name", "left")
)

category_performance = (
    category_data
    .withColumn(
        "category",
        F.coalesce(
            F.col("product_category_name_english"),
            F.col("product_category_name"),
            F.lit("Unknown")
        )
    )
    .groupBy("category")
    .agg(
        F.count("order_item_id").alias("units_sold"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("price").alias("product_revenue"),
        F.avg("price").alias("average_item_price")
    )
)

# 3. Customer state performance
customer_state_sales = (
    orders
    .join(customers, "customer_id", "left")
    .join(order_totals, "order_id", "left")
    .groupBy("customer_state")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("product_revenue").alias("product_revenue"),
        F.sum("freight_revenue").alias("freight_revenue")
    )
)

# 4. Payment method analysis
payment_analysis = (
    payments.groupBy("payment_type")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("payment_value").alias("total_payment_value"),
        F.avg("payment_value").alias("average_payment_value")
    )
)

# 5. Customer review analysis
review_analysis = (
    reviews.groupBy("review_score")
    .agg(
        F.count("review_id").alias("total_reviews")
    )
)

gold_dataframes = {
    "gold_daily_sales": daily_sales,
    "gold_category_performance": category_performance,
    "gold_customer_state_sales": customer_state_sales,
    "gold_payment_analysis": payment_analysis,
    "gold_review_analysis": review_analysis
}

print("All five Gold analytical datasets created!")

# COMMAND ----------

for name, df in gold_dataframes.items():

    output_path = gold_path + name

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(output_path)

    print(f"Saved: {name}")

print("All Gold Delta tables saved successfully!")

# COMMAND ----------

(
    gold_dataframes["gold_review_analysis"]
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_path + "gold_review_analysis")
)

print("Gold review analysis updated successfully!")

# COMMAND ----------

for name in gold_dataframes.keys():

    df = (
        spark.read
        .format("delta")
        .load(gold_path + name)
    )

    print(f"\n{name}")
    print(f"Rows: {df.count()}")
    print(f"Columns: {len(df.columns)}")

    display(df.limit(5))

# COMMAND ----------



# COMMAND ----------

for name in gold_dataframes.keys():

    df = (
        spark.read
        .format("delta")
        .load(gold_path + name)
    )

    print(f"\n===== {name} =====")
    print(f"Rows: {df.count()}")
    print(f"Columns: {len(df.columns)}")

    display(df.limit(5))
