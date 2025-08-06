# Databricks notebook source
# Simulate employee performance
from pyspark.sql.types import *
import random
def generate_employees(n=1000):
    data = []
    for i in range(1, n+1):
        emp_id = i
        branch_id = random.randint(1, 8)
        emp_name = f"Emp_{i}"
        kpi_score = round(random.uniform(1.0, 5.0), 2)
        rating = random.choice(["Excellent", "Good", "Average", "Below Average", "Poor"])
        review_date = f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
        data.append((emp_id, branch_id, emp_name, kpi_score, rating, review_date))
    return data

schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("branch_id", IntegerType(), True),
    StructField("employee_name", StringType(), True),
    StructField("kpi_score", FloatType(), True),
    StructField("rating", StringType(), True),
    StructField("review_date", StringType(), True)
])

emp_df = spark.createDataFrame(generate_employees(), schema)
emp_df.write.format("delta").mode("overwrite").saveAsTable("bronze.employee_performance_raw")
