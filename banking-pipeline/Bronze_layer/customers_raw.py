# Databricks notebook source
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")


# COMMAND ----------

spark.catalog.setCurrentDatabase("bronze")


# COMMAND ----------

spark.sql("SHOW TABLES").show()


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import random

# Sample data for names and cities
first_names = ["John", "Jane", "Robert", "Emily", "Michael", "Laura", "David", "Sophia"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia"]
cities = ["New York", "Chicago", "Houston", "Dallas", "Phoenix", "Seattle"]

# Create customer data
def generate_customers(n=5000):
    data = []
    for i in range(1, n+1):
        full_name = f"{random.choice(first_names)} {random.choice(last_names)}"
        email = f"user{i}@mail.com"
        city = random.choice(cities)
        age = random.randint(18, 75)
        gender = random.choice(["Male", "Female"])
        customer_since = f"20{random.randint(10, 22)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
        data.append((i, full_name, email, city, age, gender, customer_since))
    return data

# Define schema
schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("full_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("customer_since", StringType(), True)
])

# Create DataFrame
customer_df = spark.createDataFrame(generate_customers(5000), schema)

# Save as Delta table in bronze layer
customer_df.write.format("delta").mode("overwrite").saveAsTable("bronze.customers_raw")
