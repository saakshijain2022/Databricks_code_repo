-- Databricks notebook source
-- MAGIC %python 
-- MAGIC
-- MAGIC dbutils.fs.mount(
-- MAGIC   source = "wasbs://covidsource@saakshistorage123.blob.core.windows.net",
-- MAGIC   mount_point = "/mnt/test_db_2.0/outputs",
-- MAGIC   extra_configs = 
-- MAGIC   {"fs.azure.account.key.saakshistorage123.blob.core.windows.net":"GgJo/4f/e9NjcZI4jX6TwGWZzrsI21khB8Sb+GMM+RWhUeacBmeQQu2KMwvtuGABixwdoqh3ihmU+AStJhUo1w=="})
-- MAGIC

-- COMMAND ----------


