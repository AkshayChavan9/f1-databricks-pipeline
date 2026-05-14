# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Bronze Drivers Table

# COMMAND ----------

drivers_df = spark.table(bronze_table)

# COMMAND ----------

drivers_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Keep only the columns required for analytics (drop url column)

# COMMAND ----------

drivers_dropped_df = drivers_df.drop("url")

# COMMAND ----------

from pyspark.sql import functions as F 

# COMMAND ----------

drivers_dropped_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardise Column Names

# COMMAND ----------

drivers_renamed_df = (
    drivers_dropped_df
        .withColumnsRenamed({
            "driverId": "driver_id",
            "dateOfBirth": "date_of_birth"
        })
)

# COMMAND ----------

drivers_renamed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Concatenate name.givenName and name.familyName to create a new column called driver_name
# MAGIC

# COMMAND ----------

drivers_concatenated_df = (
  drivers_renamed_df
       .withColumn("driver_name", 
                   F.initcap(F.concat_ws(" ", F.col("name.givenName"), F.col("name.familyName"))))
       .drop("name")
)

# COMMAND ----------

drivers_concatenated_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove duplicate records

# COMMAND ----------

drivers_distinct_df = drivers_concatenated_df.dropDuplicates(["driver_id"])

# COMMAND ----------

drivers_distinct_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform values of columns `nationality` to Title Case

# COMMAND ----------

drivers_final_df = (
    drivers_distinct_df
        .withColumn('nationality', F.initcap(F.col("nationality")))
)

# COMMAND ----------

drivers_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Write the transformed data to silver `drivers` table

# COMMAND ----------

(
    drivers_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))