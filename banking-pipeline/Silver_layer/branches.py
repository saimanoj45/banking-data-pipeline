# Databricks notebook source
# Create the silver database if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
# Use the Silver schema
spark.catalog.setCurrentDatabase("silver")


# COMMAND ----------

branches_df = spark.table("bronze.branches_raw")
timezones_df = spark.table("bronze.branches_timezones_raw")

bofa_branches = branches_df.join(
    timezones_df,
    on="branch_id",
    how="left"
)

# Save to Silver layer
bofa_branches.write.format("delta").mode("overwrite").saveAsTable("silver.branches")


# COMMAND ----------

