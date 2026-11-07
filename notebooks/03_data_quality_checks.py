# Databricks notebook source
from pyspark.sql import functions as F

storage_account = "stecdataengkk2026"

silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/"
gold_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/"

print("Data Quality Check environment configured!")

# COMMAND ----------

gold_tables = [
    "gold_daily_sales",
    "gold_category_performance",
    "gold_customer_state_sales",
    "gold_payment_analysis",
    "gold_review_analysis"
]

gold_dfs = {}

for name in gold_tables:
    gold_dfs[name] = (
        spark.read
        .format("delta")
        .load(gold_path + name)
    )

    print(f"Loaded: {name}")

print("All five Gold tables loaded successfully!")

# COMMAND ----------

display(
    gold_dfs["gold_review_analysis"]
    .select("review_score", "total_reviews")
    .orderBy("review_score")
)

# COMMAND ----------

for name, df in gold_dfs.items():

    total_rows = df.count()
    distinct_rows = df.dropDuplicates().count()
    duplicate_rows = total_rows - distinct_rows

    print(f"\n===== {name} =====")
    print(f"Total rows: {total_rows}")
    print(f"Distinct rows: {distinct_rows}")
    print(f"Duplicate rows: {duplicate_rows}")

# COMMAND ----------

for name, df in gold_dfs.items():

    print(f"\n===== {name} =====")

    null_counts = df.select([
        F.count(
            F.when(F.col(c).isNull(), c)
        ).alias(c)
        for c in df.columns
    ])

    display(null_counts)

# COMMAND ----------

# 1. Daily sales validation
print("DAILY SALES VALIDATION")

display(
    gold_dfs["gold_daily_sales"]
    .filter(
        (F.col("total_orders") < 0) |
        (F.col("product_revenue") < 0) |
        (F.col("freight_revenue") < 0)
    )
)

# 2. Category performance validation
print("CATEGORY VALIDATION")

display(
    gold_dfs["gold_category_performance"]
    .filter(
        (F.col("units_sold") < 0) |
        (F.col("product_revenue") < 0)
    )
)

# 3. Payment analysis validation
print("PAYMENT VALIDATION")

display(
    gold_dfs["gold_payment_analysis"]
    .filter(
        (F.col("total_orders") < 0) |
        (F.col("total_payment_value") < 0)
    )
)


print("Business validation checks completed!")

# COMMAND ----------

for name, df in gold_dfs.items():

    print(f"\n===== {name} =====")
    df.printSchema()

# COMMAND ----------

print("REVIEW SCORE VALUES")

display(
    gold_dfs["gold_review_analysis"]
    .groupBy("review_score")
    .agg(
        F.sum("total_reviews").alias("total_reviews")
    )
    .orderBy("review_score")
)

# COMMAND ----------

gold_dfs["gold_daily_sales"].select(
    F.min("order_date").alias("first_order_date"),
    F.max("order_date").alias("last_order_date"),
    F.countDistinct("order_date").alias("unique_order_dates")
).display()

# COMMAND ----------

silver_reviews = (
    spark.read
    .format("delta")
    .load(silver_path + "olist_order_reviews_dataset")
)

silver_reviews.printSchema()

display(
    silver_reviews.select(
        "review_id",
        "review_score",
        "review_creation_date",
        "review_answer_timestamp"
    ).limit(20)
)

# COMMAND ----------

storage_account = "stecdataengkk2026"

bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"

raw_reviews = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("mode", "PERMISSIVE")
    .load(bronze_path + "olist_order_reviews_dataset.csv")
)

raw_reviews.printSchema()

display(raw_reviews.limit(15))

# COMMAND ----------

