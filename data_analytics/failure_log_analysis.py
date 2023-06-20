# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

failure_log_table_df = spark.read.format("delta").load("/mnt/lake/prepared/failure_log_table")
failure_log_table_df.createOrReplaceTempView("equipment_failures")

display(failure_log_table_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # Failure Log Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Total equipment failures that happened ?
# MAGIC
# MAGIC Starting from the premise that all the data composing the log file are errors, it is simply a matter of performing a count operation on the entire table. Therefore, we have a total of `5,000,001` errors.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_failures 
# MAGIC FROM equipment_failures 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Which equipment name had most failures?
# MAGIC
# MAGIC As we saw in the previous question that all the data are errors, to determine which equipment had the most errors, it is enough to group the data, count how many times the equipment is repeated, and sort the number of errors in descending order.
# MAGIC
# MAGIC Hence, the equipment that has the highest number of errors is `98B84035` with `358,414` errors.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT equipment_name, COUNT(*) as failures 
# MAGIC FROM equipment_failures 
# MAGIC GROUP BY equipment_name 
# MAGIC ORDER BY failures DESC 
# MAGIC LIMIT 1
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Average amount of failures across equipment group, ordered by the number of failures in ascending order?
# MAGIC
# MAGIC This SQL query is used to find the average number of failures for each equipment group.
# MAGIC
# MAGIC Here's the breakdown:
# MAGIC
# MAGIC 1. The inner SELECT statement is grouping the data from the `equipment_failures` table by `equipment_group_name` and `equipment_name`, and it is counting the number of rows (which represent failures) for each combination of `equipment_group_name` and `equipment_name`. The count is stored in a new column called `failures`.
# MAGIC
# MAGIC 2. The outer SELECT statement is taking the result of the inner query and grouping it by `equipment_group_name`. For each group, it calculates the average number of failures (`average_failures`) by taking the average of the `failures` column.
# MAGIC
# MAGIC 3. Finally, the results are ordered by `average_failures` in ascending order. So, the equipment groups with the lowest average number of failures will appear first.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT equipment_group_name, AVG(failures) as average_failures 
# MAGIC FROM (
# MAGIC     SELECT equipment_group_name, equipment_name, COUNT(*) as failures 
# MAGIC     FROM equipment_failures 
# MAGIC     GROUP BY equipment_group_name, equipment_name
# MAGIC ) 
# MAGIC GROUP BY equipment_group_name 
# MAGIC ORDER BY average_failures

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 4. Rank the sensors which present the most number of errors by equipment name in an equipment group.
# MAGIC
# MAGIC The query accomplishes this by performing the following steps:
# MAGIC
# MAGIC 1. It selects the relevant fields from the `equipment_failures` table, which are `equipment_group_name`, `equipment_name`, and `sensor_id`. 
# MAGIC
# MAGIC 2. It counts the number of errors (referred to as `failures` in the query) for each unique combination of `equipment_group_name`, `equipment_name`, and `sensor_id`.
# MAGIC
# MAGIC 3. Then, within each unique `equipment_group_name` and `equipment_name` combination, it assigns a rank to each unique `sensor_id` based on the number of errors it has encountered. This rank is in descending order, so the sensor with the most errors gets the highest rank (1).
# MAGIC
# MAGIC In summary, the query is generating a ranked list of sensors based on the number of errors they have encountered. The ranking is done separately for each piece of equipment within each equipment group. The output will give you the equipment group name, equipment name, sensor id, the number of errors for that sensor, and the sensor's rank based on the number of errors.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT equipment_group_name, equipment_name, sensor_id, COUNT(*) as failures,
# MAGIC        RANK() OVER (PARTITION BY equipment_group_name, equipment_name ORDER BY COUNT(*) DESC) as rank 
# MAGIC FROM equipment_failures 
# MAGIC GROUP BY equipment_group_name, equipment_name, sensor_id
# MAGIC
