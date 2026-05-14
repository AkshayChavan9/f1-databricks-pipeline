# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file =f"{landing_folder_path}/sprints"
table_name = f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read JSON file using the dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

sprints_schema = StructType([
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

sprints_df = (
    spark.read
        .format('json')
        .schema(sprints_schema)
        .option('mode', 'FAILFAST')
        .option('multiLine', 'true')
        .load(source_file)
)


# COMMAND ----------

sprints_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add metadata columns

# COMMAND ----------

sprints_final_df = add_ingestion_metadata(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to bronze delta table

# COMMAND ----------

(

    sprints_final_df
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