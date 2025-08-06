# Databricks notebook source
from pyspark.sql.functions import col, to_date, sum as _sum, lag, round
from pyspark.sql.window import Window

# Load transactions from silver layer
transactions_df=spark.table('silver.transactions')
transactions_df.show()

# COMMAND ----------

# Convert transaction_time to date
transactions_daily_df = transactions_df.withColumn("transaction_date", to_date(col("transaction_time")))
# Aggregate transaction volume by day
daily_volume_df = transactions_daily_df.groupBy("transaction_date") \
    .agg(_sum("amount").alias("daily_transaction_volume"))

# Define window spec ordered by transaction_date
window_spec = Window.orderBy("transaction_date")
# Calculate previous day's transaction volume
daily_volume_df = daily_volume_df.withColumn("prev_day_volume", lag("daily_transaction_volume").over(window_spec))

# Calculate day-over-day % growth ((current - previous)/previous)*100
daily_volume_df = daily_volume_df.withColumn(
    "volume_growth_pct",
    round(
        ((col("daily_transaction_volume") - col("prev_day_volume")) / col("prev_day_volume")) * 100, 2
    )
)

# Replace null (for first day) with 0 or NULL as needed
daily_volume_df = daily_volume_df.fillna({"volume_growth_pct": 0})



daily_volume_df.show(10, False)


# COMMAND ----------

# Save as gold table
daily_volume_df.write.format("delta").mode("overwrite").saveAsTable("gold.daily_transaction_volume_growth")