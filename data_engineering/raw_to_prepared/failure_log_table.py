# Databricks notebook source
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark = SparkSession.builder.appName("Raw to Prepared").getOrCreate()

# COMMAND ----------

PATH_FROM = "/mnt/lake/raw/"
PATH_TO = "/mnt/lake/prepared/"

# COMMAND ----------

equipment_failure_sensors_df = spark.read.parquet(
    os.path.join(PATH_FROM, "equipment_failure_sensors")
)
equipment_sensors_df = spark.read.parquet(os.path.join(PATH_FROM, "equipament_sensors"))
equipament_df = spark.read.parquet(os.path.join(PATH_FROM, "equipament"))

# COMMAND ----------

fact_table_df = (
    equipment_failure_sensors_df.join(equipment_sensors_df, "sensor_id", "left")
    .join(equipament_df, "equipment_id", "left")
    .select(
        col("sensor_id"),
        col("name").alias("equipment_name"),
        col("group_name").alias("equipment_group_name"),
        "timestamp",
        "level",
    )
)

fact_table_df = fact_table_df.repartition(
    "equipment_group_name", "equipment_name", "sensor_id"
)

fact_table_df.write.mode("overwrite").partitionBy(
    "equipment_group_name", "equipment_name", "sensor_id"
).format("delta").save(os.path.join(PATH_TO, "failure_log_table"))

display(fact_table_df.limit(10))
