-- Databricks notebook source
-- MAGIC %python
-- MAGIC %run ./Init-Global-Vars

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE bronze_wikipedia_changes
AS SELECT input_file_name() as source_file, *
FROM cloud_files('${LOCAL_STORAGE_SPARK_API_FORMAT}', 'json')
