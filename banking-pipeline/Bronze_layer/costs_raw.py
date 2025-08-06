# Databricks notebook source
spark.catalog.setCurrentDatabase("bronze")

# COMMAND ----------

from pyspark.sql.types import *
import random

# Read existing branch_id values from branches_raw
branch_ids = [row['branch_id'] for row in spark.table("bronze.branches_raw").select("branch_id").collect()]

cost_types = ["Maintenance", "Utilities", "Salaries", "Security"]

# Generate costs using valid branch IDs
cost_data = [
    (i, random.choice(branch_ids), random.choice(cost_types), round(random.uniform(1000.0, 10000.0), 2))
    for i in range(1, 501)
]

cost_schema = StructType([
    StructField("cost_id", IntegerType(), False),
    StructField("branch_id", IntegerType(), False),
    StructField("cost_type", StringType(), True),
    StructField("cost_amount", FloatType(), True)
])

cost_df = spark.createDataFrame(cost_data, cost_schema)
cost_df.write.format("delta").mode("overwrite").saveAsTable("costs_raw")
