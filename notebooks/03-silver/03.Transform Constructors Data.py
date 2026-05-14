# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Bronze Constructors Table

# COMMAND ----------

# circuits_df = spark.read.table(bronze_table)

# COMMAND ----------

constructors_df = spark.table(bronze_table)

# COMMAND ----------

constructors_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Keep only the columns required for analytics (drop url column)

# COMMAND ----------

from pyspark.sql import functions as F 

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

constructors_dropped_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardise Column Names

# COMMAND ----------

constructors_renamed_df = (
    constructors_dropped_df
        .withColumnsRenamed({
            "constructorId": "constructor_id",
            "name": "constructor_name"
        })
)


# COMMAND ----------

constructors_renamed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove duplicate records

# COMMAND ----------

constructors_distinct_df = constructors_renamed_df.dropDuplicates(["constructor_id"])

# COMMAND ----------

constructors_distinct_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform values of columns `nationality` to Title Case

# COMMAND ----------

constructors_final_df = (
    constructors_distinct_df
        .withColumn('nationality', F.initcap(F.col("nationality")))
)

# COMMAND ----------

constructors_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Write the transformed data to silver `constructors` table

# COMMAND ----------

(
    constructors_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))