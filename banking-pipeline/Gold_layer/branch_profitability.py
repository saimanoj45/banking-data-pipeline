# Databricks notebook source
from pyspark.sql.functions import sum, round,col

# Load data from silver layer
transactions_df = spark.table("silver.transactions")
costs_df = spark.table("bronze.costs_raw")
branches_df = spark.table("silver.branches")

# Step 1: Aggregate total transaction amount per branch
txn_branch_df = transactions_df.groupBy("branch_id") \
    .agg(sum("amount").alias("total_revenue"))

# Step 2: Aggregate total cost per branch
cost_branch_df = costs_df.groupBy("branch_id") \
    .agg(sum("cost_amount").alias("total_cost"))

# Step 3: Join revenue and cost
profit_df = txn_branch_df.join(cost_branch_df, on="branch_id", how="outer") \
    .fillna(0)

# Step 4: Calculate profitability
profit_df = profit_df.withColumn(
    "profit", round(col("total_revenue") - col("total_cost"), 2)
)

# Step 5: Enrich with branch name
final_df = profit_df.join(branches_df.select("branch_id", "branch_name"), on="branch_id", how="left")

final_df.show()


# COMMAND ----------

# Step 6: Save to gold layer
final_df.write.format("delta").mode("overwrite").saveAsTable("gold.branch_profitability")