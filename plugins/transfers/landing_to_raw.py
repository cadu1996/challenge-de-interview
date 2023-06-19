

from typing import Dict, Any, Optional, List

from airflow.models import BaseOperator
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


class LandingToRawOperator(BaseOperator):
    def __init__(self,
                 *,
                landing_path: str,
                raw_path: str,
                data_type: str = "json",
                schema: T.StructType = None,
                regex_patterns: Dict[str, str] = None,
                multi_line: bool = True,
                header: bool = True,
                mode: str = "overwrite",
                 **kwargs
                 ):
        super().__init__(**kwargs)

        self.landing_path = landing_path
        self.raw_path = raw_path
        self.data_type = data_type
        self.schema = schema
        self.regex_patterns = regex_patterns
        self.multi_line = multi_line
        self.header = header
        self.mode = mode

    def execute(self, context):
        spark = SparkSession.builder.appName("Landing to Raw").getOrCreate()

        read = spark.read
        if self.schema is not None:
            read = read.schema(self.schema)

        if self.data_type.upper() == "JSON":
            dataframe = read.schema(self.schema).json(self.landing_path, multiLine=self.multi_line)
        
        elif self.data_type.upper() == "CSV":
            dataframe = read.schema(self.schema).csv(self.landing_path, header=self.header)

        elif self.data_type.upper() == "TEXT":
            dataframe = spark.read.text(self.landing_path)

            if self.regex_patterns:
                for col, regex in self.regex_patterns.items():
                    dataframe = dataframe.withColumn(col, F.regexp_extract("value", regex, 1))

            if self.schema:
                for field in self.schema:
                    dataframe = dataframe.withColumn(field.name, dataframe[field.name].cast(field.dataType))


        else:
            raise ValueError("Data type not supported")
        
        dataframe.write.parquet(self.raw_path, mode=self.mode, compression="snappy")



