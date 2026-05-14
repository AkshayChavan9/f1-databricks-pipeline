# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_races"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Source Tables

# COMMAND ----------

circuits_df = spark.table(f"{catalog_name}.{silver_schema}.circuits")
races_df = spark.table(f"{catalog_name}.{silver_schema}.races")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Join `races` with `circuits` using `circuit_id`

# COMMAND ----------

dim_races_df = (
            races_df
                .join(
                    circuits_df,
                    races_df.circuit_id == circuits_df.circuit_id,
                    "inner"
                )
                .select (
                    races_df.season,
                    races_df.round,
                    races_df.race_name,
                    races_df.race_date,
                    circuits_df.circuit_name,
                    circuits_df.locality,
                    circuits_df.country
                )
        )


# COMMAND ----------

dim_races_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the transformed data to the `gold` `dim_races` table

# COMMAND ----------

(
    dim_races_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))