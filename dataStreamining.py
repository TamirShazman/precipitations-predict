from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import to_date, col, month, year

database_name = "tmyr"
url = 'jdbc:sqlserver://technionddscourse.database.windows.net:1433;database=tmyr;'

table_name = "test"
username = "tmyr"
password = "Qwerty12!"
cor_var = ["PRCP", "SNOW", "SNWD", "TMAX", "TMIN"]

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'
    noaa_schema = StructType([StructField('StationId', StringType(), False),
                              StructField('Date', StringType(), False),
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
        .option("subscribe", "ES") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json(F.col("value"), schema=noaa_schema).alias('json')) \
        .select("json.*") \
        .filter(~col("Q_Flag").isNull() | col("Variable").isin(cor_var) == True) \
        .withColumn("Date", to_date(col("date"), 'yyyyMMdd')) \
        .groupby("StationId", "Date", "Variable").max("Value") \
        .groupby("StationId", year("Date").alias("Year"), month("Date").alias("Month"), "Variable").mean("max(Value)").\
        alias("Mean")
    df.show(20)
    dfUSA = df.filter(col("Sta") == "PRCP").drop("Variable").withColumnRenamed("max(Value)", "PRCP")
    #dfSNOW = df.filter(col("Variable") == "SNOW").drop("Variable").withColumnRenamed("max(Value)", "SNOW")
    #dfSNWD = df.filter(col("Variable") == "SNWD").drop("Variable").withColumnRenamed("max(Value)", "SNWD")
    #dfTMAX = df.filter(col("Variable") == "TMAX").drop("Variable").withColumnRenamed("max(Value)", "TMAX")
    #dfTMIN = df.filter(col("Variable") == "TMIN").drop("Variable").withColumnRenamed("max(Value)", "TMIN")
    #dfFinal = dfPRCP.join(dfSNOW, on=[((dfPRCP.StationId == dfSNOW.StationId) & (dfPRCP.Date == dfSNOW.Date))],

    #                    how='leftsemi').join()
    #dfFinal.show(20)
    """
    try:
        df.write \
            .format("com.microsoft.sqlserver.jdbc.spark") \
            .mode("overwrite") \
            .option("url", url) \
            .option("dbtable", "test") \
            .option("user", "tmyr") \
            .option("password", "Qwerty12!") \
            .save()
    except ValueError as error:
        print("Connector write failed", error)
    """
