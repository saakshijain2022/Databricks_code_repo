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

-- MAGIC
-- MAGIC %python 
-- MAGIC from pyspark.sql.functions import *
-- MAGIC import urllib

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.format("csv").option("sep",",").option("header","true").option("InferSchema","true").load("/mnt/test_db_2.0/outputs")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df2 = df.withColumnRenamed("Sessions","No of sessions")
-- MAGIC display(df2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df2.columns
-- MAGIC # df.write.mode("overwrite").saveAsTable("employee")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("delta").option("delta.columnMapping.mode", "name").option("path", "delta.columnMapping.mode").saveAsTable("method_2_table")
-- MAGIC

-- COMMAND ----------

-- -- Now I can either query it with SQL or load it into a Spark dataframe:
SELECT * FROM method_2_table;
delta2_df = spark.read.format("delta").load("/mnt/test_db_2.0/outputs")
display(delta2_df)

-- COMMAND ----------

-- will rectify this error

-- COMMAND ----------

CREATE TABLE table2_csv
  USING CSV
  OPTIONS (path '/mnt/test_db_2.0/outputs', 'header' 'true', 'mode' 'FAILFAST');
-- Then create a Delta table using the CSV-backed table:

-- COMMAND ----------

CREATE TABLE delta_tables
  USING DELTA
  TBLPROPERTIES ("delta.columnMapping.mode" = "name")
  AS SELECT * FROM table2_csv;

SELECT * FROM delta_tables;

-- COMMAND ----------

-- finding total_dose completed
SELECT sum(Total_doses) FROM delta_tables

-- COMMAND ----------

SELECT sum(Total_doses),State FROM delta_tables
group by 2

-- COMMAND ----------

-- top 10 state dose completed 
SELECT sum(Total_doses),State FROM delta_tables
where State not in ('India')
group by 2
order by sum(Total_doses) desc
limit 10

-- COMMAND ----------

-- gender wise dose
select sum(Male_dose) from delta_tables

-- COMMAND ----------

-- gender wise dose
select sum(Male_dose) ,sum(Female_dose) ,sum(Transgender_dose) from delta_tables

-- COMMAND ----------


