-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/f1azdls/processed"

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED f1_processed;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("Success")