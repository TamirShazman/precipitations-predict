import findspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import os

findspark.init()

database_name = "tmyr"
url = 'jdbc:sqlserver://technionddscourse.database.windows.net:1433;database=tmyr;'

table_name = "test"
username = "tmyr"
password = "Qwerty12!"

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 pyspark-shell"

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'
    noaa_schema = StructType([StructField('StationId', StringType(), False),
                              StructField('Date', IntegerType(), False),
                              StructField('Variable', StringType(), False),
                              StructField('Value', IntegerType(), False),
                              StructField('M_Flag', StringType(), True),
                              StructField('Q_Flag', StringType(), True),
                              StructField('S_Flag', StringType(), True),
                              StructField('ObsTime', StringType(), True)])

    df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", "IS") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json(F.col("value"), schema=noaa_schema).alias('json')) \
        .select("json.*")

    try:
        df.write \
            .format("com.microsoft.sqlserver.jdbc.spark") \
            .mode("overwrite") \
            .option("url", url) \
            .option("dbtable", "a") \
            .option("user", "tmyr") \
            .option("password", "Qwerty12!") \
            .option("tableLock", "true") \
            .option("batchsize", "500") \
            .option("reliabilityLevel", "BEST_EFFORT") \
            .save()
    except ValueError as error:
        print("Connector write failed", error)





