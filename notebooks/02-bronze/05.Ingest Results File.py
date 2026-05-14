# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file =f"{landing_folder_path}/results"
table_name = f"{catalog_name}.{bronze_schema}.results"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read JSON file using the dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

results_schema = StructType([
    StructField('date',         DateType()),
    StructField('raceName',     StringType()),
    StructField('round',        IntegerType()),
    StructField('season',       IntegerType()),
    StructField('url',          StringType()),
    StructField('constructorId',StringType()),
    StructField('driverId',     StringType()),
    StructField('grid',         IntegerType()),
    StructField('laps',         IntegerType()),
    StructField('number',       IntegerType()),
    StructField('points',       FloatType()),
    StructField('position',     IntegerType()),
    StructField('status',       StringType()),
])

# COMMAND ----------

results_df = (
    spark.read
        .format('json')
        .schema(results_schema)
        .option('mode', 'FAILFAST')
        .load(source_file)
)


# COMMAND ----------

results_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add metadata columns

# COMMAND ----------

results_final_df = add_ingestion_metadata(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to bronze delta table

# COMMAND ----------

(

    results_final_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(table_name)

)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC