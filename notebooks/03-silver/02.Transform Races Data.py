# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Bronze Races Table

# COMMAND ----------

# races_df = spark.read.table(bronze_table)

# COMMAND ----------

races_df = spark.table(bronze_table)

# COMMAND ----------

races_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Keep only the columns required for analytics (drop url column)

# COMMAND ----------

races_selected_df= races_df.select(
    "season",
    "round",
    "raceName",
    "date",
    "circuitId",
    "ingestion_timestamp",
    "source_file"
)

# COMMAND ----------

from pyspark.sql import functions as F 

# COMMAND ----------

races_selected_df = races_df.select(
    F.col("season"),
    F.col("round"),
    F.col("raceName"),
    F.col("date"),
    F.col("circuitId"),
    F.col("ingestion_timestamp"),
    F.col("source_file")
)

# COMMAND ----------

races_selected_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardise Column Names

# COMMAND ----------

races_renamed_df = (
    races_selected_df
        .withColumnsRenamed({
            "circuitId": "circuit_id",
            "raceName": "race_name",
            "date": "race_date"
        })
)

# COMMAND ----------

races_renamed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove duplicate records

# COMMAND ----------

races_distinct_df = races_renamed_df.dropDuplicates(["season","round"])

# COMMAND ----------

races_distinct_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform values of columns `race_name` to Title Case

# COMMAND ----------

races_final_df = (
    races_distinct_df
        .withColumn('race_name', F.initcap(F.col("race_name")))
)

# COMMAND ----------

races_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Write the transformed data to silver `races` table

# COMMAND ----------

(
    races_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))