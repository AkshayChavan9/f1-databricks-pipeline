# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file =f"{landing_folder_path}/constructors.json"
table_name = f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read JSON file using the dataframe reader API

# COMMAND ----------

constructors_schema = """constructorId STRING, 
                         name STRING, 
                         nationality STRING, 
                         url STRING"""

# COMMAND ----------

constructors_df = (
    spark.read
        .format('json')
        .schema(constructors_schema)
        .option('mode', 'FAILFAST')
        .load(source_file)
)


# COMMAND ----------

constructors_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add Metadata Columns

# COMMAND ----------

constructors_df = add_ingestion_metadata(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to bronze delta table

# COMMAND ----------

(
    constructors_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))