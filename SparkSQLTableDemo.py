from pyspark.sql import *

from lib.logger import Log4J

# 15.0
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSQLTableDemo") \
        .enableHiveSupport() \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    logger = Log4J(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")


    flightTimeParquetDF.write \
        .mode("overwrite") \
        .saveAsTable("flight_data_tbl")
    # To save as a managed table on the default Spark db which its name itself is 'default' ^^

    flightTimeParquetDF.write \
        .mode("overwrite") \
        .partitionBy("ORIGIN", "OP_CARRIER") \
        .saveAsTable("flight_data_tbl")

    flightTimeParquetDF.write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5, "ORIGIN", "OP_CARRIER") \
        .saveAsTable("flight_data_tbl")

    flightTimeParquetDF.write \
        .mode("overwrite") \
        .bucketBy(5, "ORIGIN", "OP_CARRIER") \
        .sortBy("ORIGIN", "OP_CARRIER") \
        .saveAsTable("flight_data_tbl")

    # To save it on another db, there are 2 approaches:
    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")

    # 1st:
    flightTimeParquetDF.write \
        .mode("overwrite") \
        .saveAsTable("AIRLINE_DB.flight_data_tbl")

    # 2nd: Access the catalog and set the current database for this session:
    spark.catalog.setCurrentDatabase("AIRLINE_DB")
    flightTimeParquetDF.write \
        .mode("overwrite") \
        .saveAsTable("flight_data_tbl")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))






