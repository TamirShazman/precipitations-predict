from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql.functions import year, mean, month, col


"""
After the streaming stop, this script aplly couple of transformation to the data. The transformation are aggregation of
each station by month and adding statistic table for each station.
Furthermore the script add another table for GPS coordination of each station.
"""


# information for connection
kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'
username = "tmyr"
password = "Qwerty12!"
server_name = "jdbc:sqlserver://technionddscourse.database.windows.net:1433"
database_name = "tmyr"
url = server_name + ";" + "databaseName=" + database_name + ";"
# countries and their FIPS code
country_list = ['GB', 'GM', 'FR', 'SP', 'IT']

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    # downloading coordination file that holds the coordination of each station
    path = 'http://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-stations.txt'
    spark.sparkContext.addFile(path)
    url_df = spark.read.text(SparkFiles.get('ghcnd-stations.txt'))
    url_df = url_df.select(
        url_df.value.substr(0, 11).alias('StationId'),
        url_df.value.substr(12, 9).cast("float").alias('latitude'),
        url_df.value.substr(22, 9).cast("float").alias('longitude'),
        url_df.value.substr(39, 2).alias('state')
    )
    # filter it to relevant station
    url_df = url_df.filter("where StationId Like 'GB%' or StationId Like 'GM%' or StationId Like 'FR% or "
                           "StationId Like 'SP% or StationId Like 'IT%")

    try:
        url_df.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url", url) \
            .option("dbtable", "Stations") \
            .option("user", username) \
            .option("password", password) \
            .option("tableLock", "true") \
            .option("batchsize", "500") \
            .option("reliabilityLevel", "BEST_EFFORT") \
            .save()
    except ValueError as error:
        print("Connector write failed", error)

    # Aggregate by month and create a statistic table that hold the mean of the month for the whole 20 year period
    for country in country_list:
        StationDF = spark.read \
            .format("com.microsoft.sqlserver.jdbc.spark") \
            .option("url", url) \
            .option("dbtable", country + "_Table") \
            .option("user", username) \
            .option("password", password) \
            .load() \
            .groupby("StationId", year("Date").alias("Year"), month("Date").alias("Month"), "Variable") \
            .agg(mean("max(Value)").alias("Mean"))

        statDf = StationDF.drop("Year", "Month", "Mean").distinct().join(url_df, on="StationId", how='inner')
        # agg for each month
        temp = StationDF.groupby("StationId", "Month", "Variable").agg(mean("Mean").alias("Mean"))
        # loops on month
        for month in range(1, 13):
            monthDf = temp.filter(col("Month") == month).drop("Month")
            statDf = statDf.join(monthDf, on=['StationId', 'Variable'], how='leftouter')
            statDf = statDf.withColumnRenamed("Mean", "Mean of month " + str(month))
        try:
            # insert county table
            StationDF.write \
                .format("com.microsoft.sqlserver.jdbc.spark") \
                .mode("overwrite") \
                .option("url", url) \
                .option("dbtable", country + "_table_Month") \
                .option("user", username) \
                .option("password", password) \
                .save()
            # append to statistic table
            statDf.write \
                .format("com.microsoft.sqlserver.jdbc.spark") \
                .mode("append") \
                .option("url", url) \
                .option("dbtable", "StationStat") \
                .option("user", username) \
                .option("password", password) \
                .save()
        except ValueError as error:
            print("Connector write failed", error)
