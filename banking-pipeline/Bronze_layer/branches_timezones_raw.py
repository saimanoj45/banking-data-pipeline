# Databricks notebook source
spark.catalog.setCurrentDatabase("bronze")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
# Mapping for city → timezone
city_timezone = {
    "New York": "EST",
    "Chicago": "CST",
    "Houston": "CST",
    "Dallas": "CST",
    "Phoenix": "MST",
    "Seattle": "PST"
}

# Get city from previously created branch table
branches_df = spark.table("branches_raw")
branches_with_timezone = branches_df.withColumn("timezone", expr(f"CASE city " +
    "WHEN 'New York' THEN 'EST' " +
    "WHEN 'Chicago' THEN 'CST' " +
    "WHEN 'Houston' THEN 'CST' " +
    "WHEN 'Dallas' THEN 'CST' " +
    "WHEN 'Phoenix' THEN 'MST' " +
    "WHEN 'Seattle' THEN 'PST' " +
    "ELSE 'EST' END"))

branches_with_timezone.select("branch_id", "timezone") \
    .write.format("delta").mode("overwrite").saveAsTable("branches_timezones_raw")
