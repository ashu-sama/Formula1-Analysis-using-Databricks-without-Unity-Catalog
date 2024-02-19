# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS token
# MAGIC   1. Set up Spark Config for SAS Token
# MAGIC   2. List files from demo container
# MAGIC   3. read circuits.csv file in demo folder

# COMMAND ----------

formula1_sas_token = dbutils.secrets.get(scope="f1-scope", key="demo-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1azdls.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.f1azdls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.f1azdls.dfs.core.windows.net", formula1_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1azdls.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1azdls.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

