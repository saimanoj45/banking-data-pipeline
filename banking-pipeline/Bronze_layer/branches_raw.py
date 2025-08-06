# Databricks notebook source
from pyspark.sql.types import *
import random

# Sample branch cities and names
branch_cities = ["New York", "Chicago", "Houston", "Dallas", "Phoenix", "Seattle", "Boston", "Denver"]
branch_codes = ["NY001", "CH001", "HT001", "DL001", "PX001", "SE001", "BS001", "DN001"]

# Generate branches
def generate_branches():
    data = []
    for i in range(len(branch_cities)):
        branch_id = i + 1
        name = f"{branch_cities[i]} Branch"
        city = branch_cities[i]
        code = branch_codes[i]
        manager = f"Manager_{i+1}"
        opened_on = f"20{random.randint(10,20)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
        data.append((branch_id, name, city, code, manager, opened_on))
    return data

schema = StructType([
    StructField("branch_id", IntegerType(), False),
    StructField("branch_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("branch_code", StringType(), True),
    StructField("branch_manager", StringType(), True),
    StructField("opened_on", StringType(), True)
])

branch_df = spark.createDataFrame(generate_branches(), schema)
branch_df.write.format("delta").mode("overwrite").saveAsTable("bronze.branches_raw")
