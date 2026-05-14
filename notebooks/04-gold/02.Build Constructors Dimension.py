# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Source Tables

# COMMAND ----------

constructors_df = spark.table(f"{catalog_name}.{silver_schema}.constructors")
ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Join `constructors` with `nationality_region_df` using `nationality`

# COMMAND ----------

dim_constructors_df = (
    constructors_df
        .join(
            ref_nationality_region_df,
            constructors_df.nationality == ref_nationality_region_df.nationality,
            "left"
            )
        .select (
            constructors_df.constructor_id,
            constructors_df.constructor_name,
            constructors_df.nationality,
            ref_nationality_region_df.region.alias("nationality_region")
        )
)


# COMMAND ----------

dim_constructors_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the transformed data to the `gold` `dim_constructors` table

# COMMAND ----------

(
    dim_constructors_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))