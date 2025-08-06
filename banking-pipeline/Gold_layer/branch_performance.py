# Databricks notebook source
bofa_digital_platform_uptime_gold – Simulated uptime % over 30 days.

# COMMAND ----------

df=spark.table('bronze.employee_performance_raw')
df.show()

# COMMAND ----------

from pyspark.sql.functions import col, when, count, sum as _sum, desc

df=spark.table('bronze.employee_performance_raw')
branches_df = spark.table("silver.branches")

# Step 1: Group by branch and rating
rating_counts_df = df.groupBy("branch_id", "rating") \
    .agg(count("*").alias("rating_count"))

# Step 2: Pivot so each rating becomes a column
pivot_df = rating_counts_df.groupBy("branch_id") \
    .pivot("rating", ["Excellent", "Good", "Average", "Below Average", "Poor"]) \
    .sum("rating_count") \
    .na.fill(0)
# Step 3: Add total reviews and performance indicators
performance_df = pivot_df \
    .withColumn("total_reviews",
                col("Excellent") + col("Good") + col("Average") + col("Below Average") + col("Poor")) \
    .withColumn("positive_score", col("Excellent") + col("Good")) \
    .withColumn("negative_score", col("Poor") + col("Below Average")) \
    .withColumn("performance_ratio", round(col("positive_score") / col("total_reviews"), 2)) \
    .withColumn("performance_tag", when(col("performance_ratio") >= 0.6, "Top Performer")
                                 .when(col("performance_ratio") >= 0.3, "Average Performer")
                                 .otherwise("Needs Improvement"))

# performance_df.show()

# Best performing branch
performance_df.orderBy(desc("performance_ratio")).limit(1).display()

# Worst performing branch
performance_df.orderBy("performance_ratio").limit(1).display()




# COMMAND ----------

branches_df = spark.table("silver.bofa_branches")
employee_perf_enriched_df = performance_df.alias("perf") \
    .join(branches_df.alias("br"), col("perf.branch_id") == col("br.branch_id"), "left") \
    .select(
        col("perf.branch_id"),
        col("br.branch_name"),
        "Excellent", "Good", "Average", "Below Average", "Poor",
        "total_reviews", "positive_score", "negative_score",
        "performance_ratio", "performance_tag"
    )
employee_perf_enriched_df_clean = employee_perf_enriched_df \
    .withColumnRenamed("Below Average", "Below_Average") \
    .withColumnRenamed("total reviews", "total_reviews") \
    .withColumnRenamed("positive score", "positive_score") \
    .withColumnRenamed("negative score", "negative_score") \
    .withColumnRenamed("performance ratio", "performance_ratio") \
    .withColumnRenamed("performance tag", "performance_tag")

employee_perf_enriched_df_clean.write.format("delta").mode("overwrite").saveAsTable("gold.branch_performance_enriched")


# COMMAND ----------

