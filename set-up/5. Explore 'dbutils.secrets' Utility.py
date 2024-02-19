# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="f1-scope")

# COMMAND ----------

dbutils.secrets.get(scope="f1-scope", key="f1azdls-acc-key")

# COMMAND ----------

