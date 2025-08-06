# Databricks notebook source
spark.catalog.setCurrentDatabase("bronze")

# COMMAND ----------

from pyspark.sql.types import *
import random
platforms = ["Mobile App", "Website", "ATM", "IVR"]
digital_usage = [
    (i, random.randint(1, 5000), random.choice(platforms), random.randint(1, 50), random.choice(["Success", "Failure"]))
    for i in range(1, 10001)
]

digital_schema = StructType([
    StructField("event_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("platform", StringType(), True),
    StructField("usage_count", IntegerType(), True),
    StructField("status", StringType(), True)
])

digital_df = spark.createDataFrame(digital_usage, digital_schema)
digital_df.write.format("delta").mode("overwrite").saveAsTable("digital_platform_raw")
