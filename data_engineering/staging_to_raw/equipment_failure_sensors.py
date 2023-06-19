# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

PATH_FROM = "/mnt/source/equpment_failure_sensors.txt.gz"
PATH_TO = "/mnt/lake/raw/equipment_failure_sensors"

# COMMAND ----------

spark = SparkSession.builder.appName("Equipament Failure Sensors - Landing to Raw").getOrCreate()

# COMMAND ----------

equipment_failure_sensors_df = spark.read.text(PATH_FROM)

display(equipment_failure_sensors_df.limit(10))

# COMMAND ----------

equipment_failure_sensors_df = equipment_failure_sensors_df.withColumn(
    "value", regexp_replace("value", r"(\d{4})/(\d{2})/(\d{1,2})", "$1-$2-$3 00:00:00"))

equipment_failure_sensors_df = equipment_failure_sensors_df.select(
    regexp_extract("value", r"^\[(.+)\]\t", 1).alias("timestamp"),
    regexp_extract("value", r"\]\t(\w+)\t", 1).alias("level"),
    regexp_extract("value", r"sensor\[(\d+)\]", 1).alias("sensor_id"),
    regexp_extract("value", r"temperature\t(-?[\d\.]+)", 1).alias("temperature"),
    regexp_extract("value", r"vibration\t(-?[\d\.]+)", 1).alias("vibration"),
)

display(equipment_failure_sensors_df.limit(10))

# COMMAND ----------

equipment_failure_sensors_df = equipment_failure_sensors_df.withColumn("level", col("level").cast(StringType())) \
    .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
    .withColumn("sensor_id", col("sensor_id").cast(IntegerType())) \
    .withColumn("temperature", col("temperature").cast(DoubleType())) \
    .withColumn("vibration", col("vibration").cast(DoubleType())) \
    .withColumn("year", year("timestamp")) \
    .withColumn("month", month("timestamp")) \
    .withColumn("day", dayofmonth("timestamp")) \
    .withColumn("ts_load", current_timestamp())

equipment_failure_sensors_df.printSchema()

# COMMAND ----------

equipment_failure_sensors_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(PATH_TO, compression="snappy")
