# Databricks notebook source
Average Transaction Value (bofa_avg_transaction_value_gold)
✔️ Correct: Using total transaction amount divided by count is standard.

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, count, round

# Load transactions data from silver layer
df = spark.table('silver.transactions')

# Calculate average transaction value
avg_transaction_value_df = df.agg(
    round(_sum("amount") / count("transaction_id"), 2).alias("average_transaction_value")
)


avg_transaction_value_df.show()


# COMMAND ----------

# Save as gold table
avg_transaction_value_df.write.format("delta").mode("overwrite").saveAsTable("gold.avg_transaction_value")
