# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def select_order(input_df, partitionBy):
    select = input_df.schema.names;
    final_select = [col for col in select if col != partitionBy] + [partitionBy]
    output_df = input_df.select(final_select)
    return output_df

# COMMAND ----------

def incremental_load(dataFrame, partitionBy, db_name, tbl_name):
    # Re-Order the dataframe so that partitionBy column is at the end
    results_final_df = select_order(dataFrame, f"{partitionBy}")
    
    # Setting Script for Incremental Load:-
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{tbl_name}"):
        results_final_df.write.mode("overwrite").insertInto(f"{db_name}.{tbl_name}")
    else:
        results_final_df.write.mode("overwrite").partitionBy(f"{partitionBy}").format(
            "parquet"
        ).saveAsTable(f"{db_name}.{tbl_name}")

# COMMAND ----------

def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
  
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list

# COMMAND ----------

def merge_delta_tbl(input_df, db_name, tbl_name, folder_path, merge_condition, partition_col):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", True)

    from delta.tables import DeltaTable
    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{tbl_name}"):     
        delta_table = DeltaTable.forPath(spark, f"{folder_path}/{tbl_name}")
        delta_table.alias("tgt") \
            .merge(input_df.alias("src"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        input_df.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy(partition_col) \
                .saveAsTable(f"{db_name}.{tbl_name}")

