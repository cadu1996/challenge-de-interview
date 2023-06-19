# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark = SparkSession.builder.appName("ETL").getOrCreate()

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/mnt/source")

# COMMAND ----------

compressed_file_path = "/mnt/source/equpment_failure_sensors.txt.gz"
equipament_sensors_path = "/mnt/source/equipment_sensors.csv"
equipament_path = "/mnt/source/equipment.json"

# COMMAND ----------

equipment_failure_sensors_df = spark.read.text(compressed_file_path)
equipament_sensors_df = spark.read.csv(equipament_sensors_path, header=True, sep=",")
equipament_df = spark.read.json(equipament_path, multiLine=True)

display(equipment_failure_sensors_df)
display(equipament_sensors_df)
display(equipament_df)

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



display(equipment_failure_sensors_df)

# COMMAND ----------

equipment_failure_sensors_df = equipment_failure_sensors_df.withColumn("level", col("level").cast(StringType())) \
    .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
    .withColumn("sensor_id", col("sensor_id").cast(IntegerType())) \
    .withColumn("temperature", col("temperature").cast(DoubleType())) \
    .withColumn("vibration", col("vibration").cast(DoubleType()))

equipment_failure_sensors_df.printSchema()

# COMMAND ----------

equipament_sensors_df = equipament_sensors_df.withColumn("equipment_id", col("equipment_id").cast(IntegerType())) \
    .withColumn("sensor_id", col("sensor_id").cast(IntegerType()))

equipament_sensors_df.printSchema()

# COMMAND ----------

equipament_df = equipament_df.withColumn("equipment_id", col("equipment_id").cast(IntegerType())) \
    .withColumn("group_name", col("group_name").cast(StringType())) \
    .withColumn("name", col("name").cast(StringType()))

equipament_df.printSchema()

# COMMAND ----------

equipment_failure_sensors_df.write.mode("overwrite").parquet("/mnt/lake/raw/equipment_failure_sensors.parquet")
equipament_sensors_df.write.mode("overwrite").parquet("/mnt/lake/raw/equipament_sensors.parquet")
equipament_df.write.mode("overwrite").parquet("/mnt/lake/raw/equipament.parquet")

# COMMAND ----------

equipment_failure_sensors_df = spark.read.parquet("raw/equipment_failure_sensors.parquet")
equipament_sensors_df = spark.read.parquet("raw/equipament_sensors.parquet")
equipament_df = spark.read.parquet("raw/equipament.parquet")

# COMMAND ----------

# Join three tables
fact_table = equipment_failure_sensors_df.join(equipament_sensors_df, "sensor_id", "left") \
    .select("sensor_id", "equipment_id", "timestamp", "level", "temperature", "vibration")

fact_table.show(5, truncate=False)

fact_table.write.mode("overwrite").partitionBy("equipment_id", "sensor_id").parquet("prepared/equipment_failure.parquet")

# COMMAND ----------

equipament_dim = equipament_df.select("equipment_id", "group_name", "name")
equipament_dim.show(5, truncate=False)
equipament_dim.write.mode("overwrite").parquet("prepared/equipament.parquet")
