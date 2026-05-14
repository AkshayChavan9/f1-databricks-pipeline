# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file =f"{landing_folder_path}/drivers.json"
table_name = f"{catalog_name}.{bronze_schema}.drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 1 - Read JSON file using the dataframe reader API

# COMMAND ----------

# DBTITLE 1,Cell 5
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

name_schema = StructType([
    StructField('givenName',    StringType()),
    StructField('familyName',   StringType()),
])

drivers_schema = StructType([
    StructField('driverId',     StringType()),
    StructField('name',         name_schema),
    StructField('dateOfBirth',  DateType()),
    StructField('nationality',  StringType()),
    StructField('url',          StringType()),
])

# COMMAND ----------

# DBTITLE 1,Cell 6
drivers_df = (
    spark.read
        .schema(drivers_schema)
        .option('mode', 'FAILFAST')
        .json(source_file)
)


# COMMAND ----------

drivers_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add metadata columns

# COMMAND ----------

# DBTITLE 1,Cell 9
from pyspark.sql import functions as f

drivers_final_df = add_ingestion_metadata(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to bronze delta table

# COMMAND ----------

(

    drivers_final_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(table_name)

)

# COMMAND ----------

display(spark.table(table_name))