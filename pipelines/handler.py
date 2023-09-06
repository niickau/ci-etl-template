import json
from os.path import join, abspath, dirname

import pyspark.sql.functions as f
import pyspark.sql.types as t


JSON_SCHEMA_PATH = abspath(join(dirname(abspath(__file__)), "../json_schemas"))

'''
A template for the data processing class, Handler. 
The names, list, and contents of the functions can be changed as needed.
'''
class Handler:
    def __init__(self, spark):
        self.spark = spark
        self.source_dir = ""
        self.hive_schema = ""
        self.json_schemas_path = JSON_SCHEMA_PATH

    def _load_json_schema(self):
        with open(self.json_schemas_path + "/kafka_schema.json", 'r') as file:
            kafka_schema = t.StructType.fromJson(json.load(file))

        return kafka_schema

    def prepare_source_dataframe(self, df):
        return df

    def _make_partitioned(self, df):
        res = (
            df
                .withColumn("op_year", f.year(f.col("timestamp")))
                .withColumn("op_month", f.month(f.col("timestamp")))
                .withColumn("op_day", f.dayofmonth(f.col("timestamp")))
        )
        return res

    def save(self, df, table_name):
        df = self._make_partitioned(df)

        (
            df
                .write
                .format("orc")
                .mode("append")
                .partitionBy("op_year", "op_month", "op_day")
                .saveAsTable(f"{self.hive_schema}.{table_name}")
        )
