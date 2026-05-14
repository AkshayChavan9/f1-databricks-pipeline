# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Bronze results Table

# COMMAND ----------

results_df = spark.table(bronze_table)

# COMMAND ----------

results_df.display()

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Keep only the columns required for analytics (drop url column)

# COMMAND ----------

results_selected_df = (
  results_df.select("season",
                    "round",
                    "constructorId",
                    "driverId",
                    "date",
                    "raceName",
                    "grid",
                    "laps",
                    "number",
                    "points",
                    "position",
                    "status",
                    "ingestion_timestamp",
                    "source_file")
)

# COMMAND ----------

from pyspark.sql import functions as F 

# COMMAND ----------

results_selected_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardise Column Names

# COMMAND ----------

results_renamed_df = (
    results_selected_df
        .withColumnsRenamed({
            "constructorId": "constructor_id",
            "driverId": "driver_id",
            "raceName": "race_name",
            "date": "race_date",
            "grid": "grid_position",
            "laps": "completed_laps",
            "number": "car_number",
            "position": "final_position",
            "positionText": "final_position_text"
        })
)

# COMMAND ----------

results_renamed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter out rows where `season`, `round`, `custructor_id` or `driver_id` is null (business key validation)
# MAGIC

# COMMAND ----------

results_valid_df = (
    results_renamed_df
        .filter(
            F.col("season").isNotNull() &
            F.col("round").isNotNull() &
            F.col("constructor_id").isNotNull() &
            F.col("driver_id").isNotNull() 
        )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove duplicate records

# COMMAND ----------

results_distinct_df = results_valid_df.dropDuplicates(["season", "round", "constructor_id", "driver_id"])

# COMMAND ----------

results_distinct_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform values of columns `race_name` to Title Case

# COMMAND ----------

results_final_df = (
    results_distinct_df
        .withColumn('race_name', F.initcap(F.col("race_name")))
)

# COMMAND ----------

results_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Write the transformed data to silver `results` table

# COMMAND ----------

(
    results_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))