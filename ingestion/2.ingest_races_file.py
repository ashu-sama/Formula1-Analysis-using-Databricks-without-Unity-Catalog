# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

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
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)])

# COMMAND ----------

races_df = spark.read \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv", header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step to Transform time column with null values to "00:00:00"

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# COMMAND ----------

races_df = races_df.withColumn("time", regexp_replace(races_df.time, r'\\N', '00:00:00'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, col, lit, when

# COMMAND ----------

add_col = {"race_timestamp": to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'),
           "data_source": lit(v_data_source),
           "file_date": lit(v_file_date)}

# COMMAND ----------

races_with_timestamp_df = races_df.withColumns(add_col)

# COMMAND ----------

races_with_ingestion_date_df = add_ingestion_date(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3: Select the required columns and rename

# COMMAND ----------

races_final_df = races_with_ingestion_date_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"),
                                    col("round"), col("circuitId").alias("circuit_id"), col("name"), 
                                    col("race_timestamp"), col("data_source"), col("ingestion_date"), col("file_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the output to processed container in parquet format

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")