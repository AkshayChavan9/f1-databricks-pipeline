# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_session_results"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Source Tables

# COMMAND ----------

results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
         .withColumn("session_type", F.lit("RACE"))
         .drop("race_name", "race_date", "ingestion_timestamp", "source_file")
)

# COMMAND ----------

sprints_df = (
    spark.table(f"{catalog_name}.{silver_schema}.sprints")
         .withColumn("session_type", F.lit("SPRINT"))
         .drop("race_name", "race_date", "ingestion_timestamp", "source_file")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION `results` and `sprints`

# COMMAND ----------

results_sprints_df = results_df.unionByName(sprints_df)

# COMMAND ----------

results_sprints_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add dervied columns

# COMMAND ----------

fact_session_results_df = (
    results_sprints_df
        .withColumn("is_win", F.col("final_position") == 1)
        .withColumn("is_podium", F.col("final_position").between(1, 3))
        .withColumn("has_points", F.col("points") > 0)
)

# COMMAND ----------

display(fact_session_results_df.filter("season = 2025"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the transformed data to the `gold` `fact_session_results` table
# MAGIC

# COMMAND ----------

(
    fact_session_results_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))