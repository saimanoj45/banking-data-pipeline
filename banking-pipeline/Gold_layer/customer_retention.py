# Databricks notebook source
# Create the gold database if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS gold")
# Use the gold schema
spark.catalog.setCurrentDatabase("gold")

# COMMAND ----------

from pyspark.sql.functions import col, datediff, current_date, when, sum, count, round, max

# Load silver layer data
transactions_df = spark.table("silver.transactions")
cust_df = spark.table("silver.cust_acct_open_date")

# Compute most recent transaction per customer
recent_txn_df = transactions_df.groupBy('customer_id') \
    .agg(max('transaction_time').alias('recent_transaction'))

# Join with customer data
cust_df = cust_df.join(recent_txn_df, on='customer_id', how='left')

# Calculate number of days since last transaction
cust_df = cust_df.withColumn("days_inactive", datediff(current_date(), col("recent_transaction")))

# Consider retained if recent activity within last 365 days
cust_df = cust_df.withColumn("retained", when(col("days_inactive") <= 365, 1).otherwise(0))

# Aggregate to get retention stats
retention_df = cust_df.agg(
    round((sum("retained") / count("*")) * 100, 2).alias("retention_rate_percentage"),
    count("*").alias("total_customers"),
    sum("retained").alias("retained_customers")
)

# Show or save the result
retention_df.show()


# COMMAND ----------

# Step 6: Save to gold layer
retention_df.write.format("delta").mode("overwrite").saveAsTable("gold.customer_retention_gold")