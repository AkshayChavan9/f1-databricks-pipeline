# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Bronze Circuits Table

# COMMAND ----------

# circuits_df = spark.read.table(bronze_table)

# COMMAND ----------

circuits_df = spark.table(bronze_table)

# COMMAND ----------

circuits_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Keep only the columns required for analytics (drop url column)

# COMMAND ----------

circuits_selected_df= circuits_df.select(
    "circuitId",
    "circuitName",
    "lat",
    "lon",
    "locality",
    "country",
    "ingestion_timestamp",
    "source_file"
)

# COMMAND ----------

from pyspark.sql import functions as F 

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    F.col("circuitId"),
    F.col("circuitName"),
    F.col("lat"),
    F.col("lon"),
    F.col("locality"),
    F.col("country"),
    F.col("ingestion_timestamp"),
    F.col("source_file")
)

# COMMAND ----------

circuits_selected_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardise Column Names

# COMMAND ----------

circuits_renamed_df = (
    circuits_selected_df
        .withColumnsRenamed({
            "circuitId": "circuit_id",
            "circuitName": "circuit_name",
            "lat": "latitude",
            "lon": "longitude"
        })
)

# COMMAND ----------

circuits_renamed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter out rows where circuit_id is null (business key validation)

# COMMAND ----------

circuits_valid_df = circuits_renamed_df.filter(
    F.col("circuit_id").isNotNull()
)

# COMMAND ----------

circuits_valid_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove duplicate records

# COMMAND ----------

circuits_distinct_df = circuits_valid_df.dropDuplicates(["circuit_id"])

# COMMAND ----------

circuits_distinct_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform values of columns `circuit_name` and `locality` to Title Case

# COMMAND ----------

circuits_final_df = (
    circuits_distinct_df
        .withColumn('circuit_name', F.initcap(F.col("circuit_name")))
        .withColumn('locality', F.initcap(F.col("locality")))
)

# COMMAND ----------

circuits_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Write the transformed data to silver `circuits` table

# COMMAND ----------

(
    circuits_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))