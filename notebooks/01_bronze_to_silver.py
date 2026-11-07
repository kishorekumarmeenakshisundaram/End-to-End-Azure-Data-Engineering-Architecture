# Databricks notebook source
print("Hello, E-Commerce Data Engineering!")

# COMMAND ----------

storage_account = "stecdataengkk2026"

bronze_path = (
    f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
)

silver_path = (
    f"abfss://silver@{storage_account}.dfs.core.windows.net/"
)

gold_path = (
    f"abfss://gold@{storage_account}.dfs.core.windows.net/"
)

print("Storage paths configured successfully!")

# COMMAND ----------

from pyspark.sql.functions import *

files = [
    "olist_customers_dataset",
    "olist_orders_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "product_category_name_translation"
]

dataframes = {}

for file in files:
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(bronze_path + file + ".csv")
    )

    dataframes[file] = df

    print(f"{file}: {df.count()} rows")

# COMMAND ----------

review_df = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", False)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("mode", "PERMISSIVE")
    .load(bronze_path + "olist_order_reviews_dataset.csv")
)

review_df.printSchema()

display(review_df.limit(10))
dataframes["olist_order_reviews_dataset"] = review_df

# COMMAND ----------

bronze_path = "abfss://bronze@stecdataengkk2026.dfs.core.windows.net/"

display(dbutils.fs.ls(bronze_path))

# COMMAND ----------

silver_path = "abfss://silver@stecdataengkk2026.dfs.core.windows.net/"
gold_path = "abfss://gold@stecdataengkk2026.dfs.core.windows.net/"

print("Bronze, Silver and Gold paths configured!")

# COMMAND ----------

display(dbutils.fs.ls(silver_path))
display(dbutils.fs.ls(gold_path))

# COMMAND ----------

for name, df in dataframes.items():
    print("\n" + "=" * 70)
    print(f"DATASET: {name}")
    print("=" * 70)

    df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, count, when, isnan

for name, df in dataframes.items():
    print(f"\nDATASET: {name}")
    print(f"Rows: {df.count()}")
    print(f"Columns: {len(df.columns)}")

    null_counts = df.select([
        count(
            when(col(c).isNull(), c)
        ).alias(c)
        for c in df.columns
    ])

    display(null_counts)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Business keys for duplicate removal
primary_keys = {
    "olist_customers_dataset": ["customer_id"],
    "olist_orders_dataset": ["order_id"],
    "olist_order_items_dataset": ["order_id", "order_item_id"],
    "olist_order_payments_dataset": ["order_id", "payment_sequential"],
    "olist_order_reviews_dataset": ["review_id"],
    "olist_products_dataset": ["product_id"],
    "olist_sellers_dataset": ["seller_id"],
    "product_category_name_translation": ["product_category_name"]
}

silver_dataframes = {}

for name, df in dataframes.items():

    # 1. Standardize column names
    for old_col in df.columns:
        new_col = old_col.strip().lower().replace(" ", "_")
        if old_col != new_col:
            df = df.withColumnRenamed(old_col, new_col)

    # 2. Trim whitespace in string columns
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                F.trim(F.col(field.name))
            )

   
    # 3. Safely convert date and timestamp columns
    for column in df.columns:

        if column.endswith("_timestamp"):
            df = df.withColumn(
            column,
            F.expr(f"try_cast(`{column}` AS TIMESTAMP)")
            )

        elif column.endswith("_date"):
            df = df.withColumn(
            column,
            F.expr(f"try_cast(`{column}` AS DATE)")
        )
    # 4. Remove duplicates using business keys
    keys = primary_keys.get(name, [])
    if keys and all(key in df.columns for key in keys):
        df = df.dropDuplicates(keys)
    else:
        df = df.dropDuplicates()

    # 5. Add processing timestamp
    df = df.withColumn(
        "silver_processed_at",
        F.current_timestamp()
    )

    silver_dataframes[name] = df

    print(f"Transformed: {name} | Rows: {df.count()}")

print("All eight datasets transformed successfully!")

# COMMAND ----------

silver_dataframes["olist_order_reviews_dataset"].select(
    "review_id",
    "order_id",
    "review_score",
    "review_creation_date",
    "review_answer_timestamp"
).show(10, truncate=False)

# COMMAND ----------

for name, df in silver_dataframes.items():

    output_path = silver_path + name

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(output_path)

    print(f"Saved Silver Delta table: {name}")

print("All Silver tables saved successfully!")

# COMMAND ----------

for name in silver_dataframes.keys():

    path = silver_path + name

    df = spark.read.format("delta").load(path)

    print(f"{name}: {df.count()} rows | {len(df.columns)} columns")

# COMMAND ----------

display(
    silver_dataframes["olist_order_reviews_dataset"]
    .select(
        "review_id",
        "order_id",
        "review_score",
        "review_creation_date",
        "review_answer_timestamp"
    )
    .limit(15)
)

# COMMAND ----------

(
    silver_dataframes["olist_order_reviews_dataset"]
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_path + "olist_order_reviews_dataset")
)

print("Silver reviews table updated successfully!")

# COMMAND ----------

updated_reviews = (
    spark.read
    .format("delta")
    .load(silver_path + "olist_order_reviews_dataset")
)

display(
    updated_reviews.select(
        "review_id",
        "order_id",
        "review_score",
        "review_creation_date",
        "review_answer_timestamp"
    ).limit(10)
)
