# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.sprints"
silver_table = f"{catalog_name}.{silver_schema}.sprints"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Bronze sprints Table

# COMMAND ----------

sprints_df = spark.table(bronze_table)

# COMMAND ----------

sprints_df.display()

# COMMAND ----------

sprints_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Keep only the columns required for analytics (drop url column)

# COMMAND ----------

sprints_selected_df = (
  spark.table(bronze_table)
       .select("season",
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

sprints_selected_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardise Column Names

# COMMAND ----------

sprints_renamed_df = (
    sprints_selected_df
        .withColumnsRenamed({
            "constructorId": "constructor_id",
            "driverId": "driver_id",
            "raceName": "race_name",
            "date": "race_date",
            "grid": "grid_position",
            "laps": "completed_laps",
            "number": "car_number",
            "position": "final_position",
     \
        })
)

# COMMAND ----------

sprints_renamed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter out rows where `season`, `round`, `custructor_id` or `driver_id` is null (business key validation)

# COMMAND ----------

sprints_valid_df = (
    sprints_renamed_df
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

sprints_distinct_df = sprints_valid_df.dropDuplicates(["season", "round", "constructor_id", "driver_id"])

# COMMAND ----------

sprints_distinct_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform values of columns `race_name` to Title Case

# COMMAND ----------

sprints_final_df = (
    sprints_valid_df
        .withColumn('race_name', F.initcap(F.col("race_name")))
)

# COMMAND ----------

sprints_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Write the transformed data to silver `sprints` table

# COMMAND ----------

(
    sprints_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))