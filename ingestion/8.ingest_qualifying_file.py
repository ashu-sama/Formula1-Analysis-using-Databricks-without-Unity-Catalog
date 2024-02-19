# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying json files

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                       StructField("raceId", IntegerType(), True),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("constructorId", IntegerType(), True),
                                       StructField("number", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("q1", StringType(), True),
                                       StructField("q2", StringType(), True),
                                       StructField("q3", StringType(), True)])

# COMMAND ----------

qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/qualifying", multiLine=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename qualifyingId, driverId, constructorId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

renamed_cols_dict = {"qualifyId": "qualify_id",
                     "raceId": "race_id",
                     "driverId": "driver_id",
                     "constructorId": "constructor_id"
                     }

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnsRenamed(renamed_cols_dict) \
                            .withColumn("data_source", lit(v_data_source)) \
                            .withColumn("file_date", lit(v_file_date))

final_df = add_ingestion_date(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

# incremental_load(final_df, "race_id", "f1_processed", "qualifying")

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_tbl(final_df, "f1_processed", "qualifying", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")