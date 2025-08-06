# Databricks notebook source
spark.catalog.setCurrentDatabase("silver")

# COMMAND ----------

from pyspark.sql.functions import *
customers_df = spark.table("bronze.customers_raw")
transactions_df = spark.table("bronze.transactions_raw")

# transactions_df.printSchema()

acct_open_date_df = transactions_df.groupBy("customer_id") \
    .agg(min("transaction_time").alias("account_open_date"))

bofa_cust_acct_open = customers_df.join(
    acct_open_date_df,
    on="customer_id",
    how="left"
)

# Save to Silver layer
bofa_cust_acct_open.write.format("delta").mode("overwrite").saveAsTable("silver.cust_acct_open_date")
