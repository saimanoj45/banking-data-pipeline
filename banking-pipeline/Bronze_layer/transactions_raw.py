# Databricks notebook source
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# Simulate transactions
def generate_transactions(n=100000):
    data = []
    for i in range(1, n+1):
        txn_id = i
        customer_id = random.randint(1, 5000)
        branch_id = random.randint(1, 8)
        amount = round(random.uniform(10, 10000), 2)
        txn_type = random.choice(["Credit", "Debit"])
        status = random.choice(["Success", "Failed"])
        txn_time = (datetime(2024, 1, 1) + timedelta(minutes=random.randint(0, 300000))).strftime("%Y-%m-%d %H:%M:%S")
        data.append((txn_id, customer_id, branch_id, amount, txn_type, status, txn_time))
    return data

schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("branch_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("transaction_time", StringType(), True)
])

txn_df = spark.createDataFrame(generate_transactions(), schema)
txn_df.write.format("delta").mode("overwrite").saveAsTable("bronze.transactions_raw")
