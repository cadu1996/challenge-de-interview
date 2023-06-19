# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

PATH_FROM = "/mnt/source/equipment_sensors.csv"
PATH_TO = "/mnt/lake/raw/equipament_sensors"

# COMMAND ----------

spark = SparkSession.builder.appName("Equipament Sensors - Landing to Raw").getOrCreate()

# COMMAND ----------

equipament_sensors_df = spark.read.csv(PATH_FROM, header=True, sep=",")

display(equipament_sensors_df.limit(10))

# COMMAND ----------

equipament_sensors_df = equipament_sensors_df.withColumn("equipment_id", col("equipment_id").cast(IntegerType())) \
    .withColumn("sensor_id", col("sensor_id").cast(IntegerType())) \
    .withColumn("ts_load", current_timestamp())

equipament_sensors_df.printSchema()

# COMMAND ----------

equipament_sensors_df.write.mode("overwrite").parquet(PATH_TO, compression="snappy")
