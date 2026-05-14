# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Source Tables

# COMMAND ----------

drivers_df = spark.table(f"{catalog_name}.{silver_schema}.drivers")
ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join `drivers` with `nationality_region_df` using `nationality`

# COMMAND ----------

dim_drivers_df = (
    drivers_df
        .join(
            ref_nationality_region_df,
            drivers_df.nationality == ref_nationality_region_df.nationality,
            "left"
        )
        .select(
            drivers_df.driver_id,
            drivers_df.driver_name,
            drivers_df.date_of_birth,
            drivers_df.nationality,
            ref_nationality_region_df.region.alias("nationality_region")
        )
)

# COMMAND ----------

dim_drivers_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the transformed data to the `gold` `dim_drivers` table

# COMMAND ----------

(
    dim_drivers_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))