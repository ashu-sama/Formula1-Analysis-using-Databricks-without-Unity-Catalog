# Databricks notebook source
# MAGIC %md
# MAGIC ##### Produce constructor standings

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'") 

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import col, when, count, sum

constructors_df = race_results_df.groupBy(col("race_year"), col("team")).agg(
    sum(col("points")).alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins"),
)

# COMMAND ----------

constructors_df = race_results_df.groupBy(col("race_year"), col("team")).agg(
    sum(col("points")).alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins"),
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

constructors_spec = Window.partitionBy("race_year").orderBy(
    desc("total_points"), desc("wins")
)

final_df = constructors_df.withColumn(
    "rank", rank().over(constructors_spec)
)

# COMMAND ----------

merge_condition = "tgt.race_year = src.race_year AND tgt.team = src.team"
merge_delta_tbl(final_df, "f1_presentation", "constructor_standings", presentation_folder_path, merge_condition, "race_year")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM   f1_presentation.constructor_standings
# MAGIC -- WHERE race_year = 2021;  

# COMMAND ----------

