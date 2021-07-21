from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import to_date, col, month, year, mean

"""
In this script, the data we'll be downloaded and modify for insertion to DB. The countries that we selected to download
their data are England, Germany, France, Spain and Italy.First of all the script fetch Station table from the DB 
(table that already been inserted to DB with information about station location).After transform the data from json file 
structure to Spark DF and cleans the the data from outliers (Q_FLAG).Then it's filter all the old data 
(older then 1.1.2000) and the keeps only samples from the core variables. After that it's aggregates the sample from
each day to one day (by using the max value of samples from the day), and then aggregate the mean (of each year) of each
month. Finally it's insert each county to it Table and calculates statistics of each station (the mean of each variable
for each month for the whole period that inserted to DB.
"""

# information for connection
kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'
username = "tmyr"
password = "Qwerty12!"
server_name = "jdbc:sqlserver://technionddscourse.database.windows.net:1433"
database_name = "tmyr"
url = server_name + ";" + "databaseName=" + database_name + ";"
# core variable names
cor_var = ["PRCP", "SNOW", "SNWD", "TMAX", "TMIN"]
# countries and their FIPS code
# my_countries = [('GB', 'England'), ('GM', 'Germany'), ('FR', 'France'), ('SP', 'Spain'), ('IT', 'Italy')]
# main DF scheme
noaa_schema = StructType([StructField('StationId', StringType(), False),
                          StructField('Date', StringType(), False),
                          StructField('Variable', StringType(), False),
                          StructField('Value', IntegerType(), False),
                          StructField('M_Flag', StringType(), True),
                          StructField('Q_Flag', StringType(), True),
                          StructField('S_Flag', StringType(), True),
                          StructField('ObsTime', StringType(), True)])

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    # fetch the station information
    StationDF = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("dbtable", "Stations") \
        .option("user", username) \
        .option("password", password).load() \
        .drop("state")

    # download from kafka the data and modify it.
    df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", "GB, GM, FR, SP, IT") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json(col("value"), schema=noaa_schema).alias('json')) \
        .select("json.*") \
        .filter("CAST(Date AS INT) > 20000101") \
        .filter(~col("Q_Flag").isNull() | col("Variable").isin(cor_var) == True) \
        .withColumn("Date", to_date(col("date"), 'yyyyMMdd')) \
        .groupby("StationId", "Date", "Variable").max("Value") \
        .groupby("StationId", year("Date").alias("Year"), month("Date").alias("Month"), "Variable"). \
        agg(mean("max(Value)").alias("Mean"))
    df.show(20)
    # loops on each county and takes it's statistic
    for fipsCode, countyName in my_countries:
        # filter the current county
        finalDf = df.filter("StationId LIKE '" + str(fipsCode) + "%'")
        # create a DF for statistics information
        statDf = finalDf.drop("Year", "Month", "Mean").distinct().join(StationDF, on="StationId", how='inner')
        # agg for each month
        temp = finalDf.groupby("StationId", "Month", "Variable").agg(mean("Mean").alias("Mean"))
        # loops on month
        for month in range(1, 13):
            monthDf = temp.filter(col("Month") == month).drop("Month")
            monthDf.show(20)
            monthDf.createOrReplaceTempView("M")
            statDf.createOrReplaceTempView("S")
            statDf = statDf.join(monthDf, on=['StationId', 'Variable'], how='leftouter')
            statDf.show(20)
            statDf = statDf.withColumnRenamed("Mean", "Mean of month" + str(month))
        statDf.show(20)
        try:
            # insert county table
            finalDf.write \
                .format("com.microsoft.sqlserver.jdbc.spark") \
                .mode("overwrite") \
                .option("url", url) \
                .option("dbtable", countyName + "_table") \
                .option("user", username) \
                .option("password", password) \
                .save()
            # append to statistic table
            finalDf.write \
                .format("com.microsoft.sqlserver.jdbc.spark") \
                .mode("append") \
                .option("url", url) \
                .option("dbtable", "StationStat") \
                .option("user", username) \
                .option("password", password) \
                .save()
        except ValueError as error:
            print("Connector write failed", error)
