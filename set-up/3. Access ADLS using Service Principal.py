# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake.

# COMMAND ----------

client_id = dbutils.secrets.get('f1-scope', 'sp-client-id')
tenant_id = dbutils.secrets.get('f1-scope', 'sp-tenant-id')
client_secret = dbutils.secrets.get('f1-scope', 'sp-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1azdls.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1azdls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1azdls.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1azdls.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1azdls.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1azdls.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1azdls.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

