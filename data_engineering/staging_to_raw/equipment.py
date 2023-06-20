# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

PATH_FROM = "/mnt/source/equipment.json"
PATH_TO = "/mnt/lake/raw/equipament"

# COMMAND ----------

spark = SparkSession.builder.appName("Equipament - Landing to Raw").getOrCreate()

# COMMAND ----------

equipament_df = spark.read.json(PATH_FROM, multiLine=True)

display(equipament_df.limit(10))

# COMMAND ----------

equipament_df = (
    equipament_df.withColumn("equipment_id", col("equipment_id").cast(IntegerType()))
    .withColumn("group_name", col("group_name").cast(StringType()))
    .withColumn("name", col("name").cast(StringType()))
    .withColumn("ts_load", current_timestamp())
)

equipament_df.printSchema()

# COMMAND ----------

equipament_df.write.mode("overwrite").parquet(PATH_TO, compression="snappy")
