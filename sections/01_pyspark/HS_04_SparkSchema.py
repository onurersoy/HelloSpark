from pyspark.sql import SparkSession
from lib import Log4J

# 12.0
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchema") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    logger = Log4J(spark)

    # CSV:
    flightTimeCsvDF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/flight*.csv")

    flightTimeCsvDF.show(5)
    # Log level is set to warn below due to a local Log4J config issue. Should be edited as 'info' once it's fixed.
    logger.warn("CSV Schema:" + flightTimeCsvDF.schema.simpleString())
    # 11.1: Without this ".option("inferSchema", "true") \", all schema looks string, but with this option,
    # the result is:
        # 23/08/21 15:01:35 WARN SparkSchema: CSV Schema:struct<FL_DATE:string,OP_CARRIER:string,
        # OP_CARRIER_FL_NUM:int,ORIGIN:string,ORIGIN_CITY_NAME:string,DEST:string,DEST_CITY_NAME:string,CRS_DEP_TIME:int,
        # DEP_TIME:int,WHEELS_ON:int,TAXI_IN:int,CRS_ARR_TIME:int,ARR_TIME:int,CANCELLED:int,DISTANCE:int>
    # You can see int fields, even bigint fields and column names displayed in alphabetical order if you are working on
    # json (check the below code). But date fields still look as string.

    # 11.2: You can interfere the scheme 2 ways:
    # i. Explicit: You can define/create schema
    # ii. Implicit: You can use already existed schema along with your source data if you are f.e. working on a parquet
    # file (which usually holds the schema details along with the data itself). So, the DataFrame reader for the parquet
    # source loaded it with the same schema.

    # JSON:
    flightTimeJsonDF = spark.read \
        .format("json") \
        .load("data/flight*.json")

    flightTimeJsonDF.show(5)
    logger.warn("JSON Schema:" + flightTimeJsonDF.schema.simpleString())

    # PARQUET:
    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    flightTimeJsonDF.show(5)
    logger.warn("Parquet Schema:" + flightTimeParquetDF.schema.simpleString())
    # 11.3: Now if you check it, you will see that date fields are marked as date "Parquet Schema:struct<FL_DATE:date"
    # since our parquet file contains the well-defined correct schema along with the data.

    # You should prefer using the Parquet file format as long as it is possible.
    # Remember that parquet file format is the recommended and the default file format for Apache Spark
