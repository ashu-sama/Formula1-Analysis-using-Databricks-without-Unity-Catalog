# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Access Keys
# MAGIC   1. Set up Spark Config fs.azure.account.key
# MAGIC   2. List files from demo container
# MAGIC   3. read circuits.csv file in demo folder

# COMMAND ----------

formula1_account_key = dbutils.secrets.get(scope="f1-scope", key="f1azdls-acc-key")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.f1azdls.dfs.core.windows.net",
    formula1_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1azdls.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1azdls.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

