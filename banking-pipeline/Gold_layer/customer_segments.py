# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Load silver tables
transactions_df = spark.table("silver.transactions")
cust_df = spark.table("silver.cust_acct_open_date")

# Aggregate per customer
agg_df = transactions_df.groupBy("customer_id").agg(
    sum("amount").alias("total_spent"),
    count("*").alias("txn_count"),
    min("transaction_time").alias("first_txn"),
    max("transaction_time").alias("last_txn")
)

# Calculate active days and avg txn frequency
agg_df = agg_df.withColumn(
    "active_days",
    datediff(col("last_txn"), col("first_txn")) + 1
).withColumn(
    "avg_txn_per_day",
    col("txn_count") / col("active_days")
).withColumn(
    "days_since_last_txn",
    datediff(current_date(), col("last_txn"))
)

# Define segmentation logic
agg_df = agg_df.withColumn(
    "segment",
    when((col("total_spent") > 10000) & (col("avg_txn_per_day") > 1), "High Value")
    .when((col("total_spent").between(2000, 10000)) | (col("avg_txn_per_day").between(0.2, 1)), "Moderate")
    .otherwise("Low Activity")
)

# Join with customer info if needed
customer_info_df = spark.table("bronze.customers_raw").select("customer_id", "full_name", "city")

final_df = agg_df.join(customer_info_df, "customer_id", "left").select(
    "customer_id", "full_name", "city", "total_spent", "avg_txn_per_day", "days_since_last_txn", "segment"
)
final_df.show()



# COMMAND ----------

# Write to gold
final_df.write.format("delta").mode("overwrite").saveAsTable("gold.customer_segments")