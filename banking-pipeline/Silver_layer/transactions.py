# Databricks notebook source
# MAGIC %md
# MAGIC Convert timestamp to standard format
# MAGIC
# MAGIC Remove duplicates
# MAGIC
# MAGIC Drop rows with null customer_id or transaction_id

# COMMAND ----------

spark.catalog.setCurrentDatabase("silver")

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

transactions_df = spark.table("bronze.transactions_raw")

cleaned_transactions = transactions_df \
    .withColumn("transaction_time", to_timestamp("transaction_time")) \
    .dropDuplicates(["transaction_id"]) \
    .dropna(subset=["customer_id", "transaction_id"])

# Save to Silver layer
cleaned_transactions.write.format("delta").mode("overwrite").saveAsTable("silver.transactions")


# COMMAND ----------

transactions_df = spark.table("bronze.transactions_raw")
transactions_df.printSchema()