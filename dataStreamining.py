from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import to_date, col, month, year, mean, variance

"""
In this script, the data we'll be downloaded and modify for insertion to DB. The countries that we selected to download
their data are England, Germany, France, Spain and Italy. After transform the data from json file 
structure to Spark DF and cleans the the data from outliers (Q_FLAG).Then we filter all the old data 
(older then 1.1.2000) and the keeps only samples from the core variables. After that it's aggregates the sample from
each day to one day (by using the max value of samples from the day). Finally it's insert each county to it Table and 
calculates. 
"""

# information for connection
kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'
username = "tmyr"
password = "Qwerty12!"
server_name = "jdbc:sqlserver://technionddscourse.database.windows.net:1433"
database_name = "tmyr"
url = server_name + ";" + "databaseName=" + database_name + ";"
# core variable names
cor_var = ["PRCP", "SNOW", "SNWD", "TMAX", "TMIN", "TAVG"]
# countries and their FIPS code
country_list = ['GB', 'GM', 'FR', 'SP', 'IT']
# main DF scheme
basic_schema = StructType([StructField('StationId', StringType(), False),
                           StructField('Date', StringType(), False),
                           StructField('Variable', StringType(), False),
                           StructField('Value', IntegerType(), False),
                           StructField('M_Flag', StringType(), True),
                           StructField('Q_Flag', StringType(), True),
                           StructField('S_Flag', StringType(), True),
                           StructField('ObsTime', StringType(), True)])
# DB scheme
myDB_schema = StructType([StructField('StationId', StringType(), True),
                          StructField('Date', DateType(), True),
                          StructField('Variable', StringType(), True),
                          StructField('Value', IntegerType(), True)])


def write_to_DB(myDf, epochId, country_list):
    """
    :param myDf:Static DF to write to DB
    :param epochId: number of epoch
    :param country_list: list of county
    """
    print("batch ID:", epochId)
    # filter the DF according the current country, and insert it to the right table in DB
    for country in country_list:
        print(country)
        myDf.filter("StationId LIKE '" + country + "%'") \
            .write \
            .format("com.microsoft.sqlserver.jdbc.spark") \
            .mode('append') \
            .option("url", url) \
            .option("dbtable", country + "_Table") \
            .option("user", username) \
            .option("password", password) \
            .save()


if __name__ == "__main__":
    # start a spark session
    spark = SparkSession.builder.getOrCreate()
    # create the proper table in the DB
    rdd = spark.sparkContext.emptyRDD()
    Df = spark.createDataFrame(rdd, myDB_schema)
    # initialize database
    for country in country_list:
        Df.write \
            .format("com.microsoft.sqlserver.jdbc.spark") \
            .mode("overwrite") \
            .option("url", url) \
            .option("dbtable", country + "_Table") \
            .option("user", username) \
            .option("password", password) \
            .save()

    # defining spark streaming
    firstDF = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_server)
        .option("subscribe", "GB, GM, FR, SP, IT")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(F.from_json(col("value"), schema=basic_schema).alias('json'))
        .select("json.*")
        .drop("M_Flag", "S_Flag", "ObsTime")
        .filter("CAST(Date AS INT) > 20000101")
        .filter(col("Q_Flag").isNull())
        .filter(col("Variable").isin(cor_var) == True)
        .withColumn("Date", to_date(col("date"), 'yyyyMMdd'))
        .groupby("StationId", "Date", "Variable").agg(F.max("Value").alias("Value"))
    )
    # start spark streaming
    query = firstDF.writeStream.foreachBatch(lambda df, epoch_id: write_to_DB(df, epoch_id, country_list)). \
        trigger(processingTime='60 seconds').outputMode("update").start()
    query.awaitTermination()
