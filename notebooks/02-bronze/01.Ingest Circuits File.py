# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/circuits.csv"
table_name = f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

circuits_schema = StructType([
    StructField("circuitId",    StringType()),
    StructField("url",          StringType()),
    StructField("circuitName",  StringType()),
    StructField("lat",          DoubleType()),
    StructField("lon",          DoubleType()),
    StructField("locality",     StringType()),
    StructField("country",      StringType()),
])

# COMMAND ----------

circuits_df = (
    spark.read
    .format('csv')
    .option('header', 'true')
#   .option('inferSchema', 'true')
    .option('mode', 'FAILFAST')
    .schema(circuits_schema)
    .load(source_file)
)

# COMMAND ----------

circuits_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add metadata columns

# COMMAND ----------

circuits_final_df = add_ingestion_metadata(circuits_df)


# COMMAND ----------

circuits_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to bronze delta table

# COMMAND ----------

(
    circuits_final_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from formula1.bronze.circuits;