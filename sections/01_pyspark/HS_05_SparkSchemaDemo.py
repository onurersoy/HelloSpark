from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType

from lib import Log4J

# 13.0: Pretty much the same with SparkSchema.py file (without the comments etc.). On this one, we will learn creating
# our own schema, that's why we copied the code to keep this one clean as much as we can.
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    logger = Log4J(spark)

    ######################################
    # Programmatically Defining Spark Schema:
    # StructType = A List of Struct Fields
    # Struct field takes 2 mandatory arguments: 'Column name' and 'data types'

    # So, the StructType represents a DataFrame row structure;
    # and the StructField is a column definition.
    ######################################
    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    ######################################
    # Using DDL Script for Defining Spark Schema:
    # COLUMN_NAME_1 DATA_TYPE_1, COLUMN_NAME_2 DATA_TYPE_2, ...
    ######################################
    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
          ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
          WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""


    flightTimeCsvDF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(flightSchemaStruct) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/y") \
        .load("data/flight*.csv")
    # .option("inferSchema", "true") \
    # We commented out the above code and load our own schema struct instead.

    flightTimeCsvDF.show(5)
    logger.warn("CSV Schema:" + flightTimeCsvDF.schema.simpleString())


    flightTimeJsonDF = spark.read \
        .format("json") \
        .schema(flightSchemaDDL) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/y") \
        .load("data/flight*.json")

    flightTimeJsonDF.show(5)
    logger.warn("JSON Schema:" + flightTimeJsonDF.schema.simpleString())


    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    flightTimeJsonDF.show(5)
    logger.warn("Parquet Schema:" + flightTimeParquetDF.schema.simpleString())
