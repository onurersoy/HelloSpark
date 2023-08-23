import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4J

# 14.0
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("DataSinkDemo") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    logger = Log4J(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    # flightTimeParquetDF.write \
    #     .format("parquet") \
    #     .mode("overwrite") \
    #     .option("path", "dataSink/parquet/") \
    #     .save()

    # .format("parquet") >> We were going with this one but due to local spark-defaults.conf file issue, we could not
    # include avro, that's why it is parquet for now. It is because Spark AVRO doesn't come bundled with Apache Spark,
    # so if you want to work with Avro data, then you must include an additional Scala package in your project.
    # We are working in Python IDE, and it doesn't offer you a method to include Scala or Java dependencies.
    # Remember the spark-defaults.conf file? You can use the same file to add some Scala packages.

    # How many Avro files do you expect? One, two, or ten? It depends upon the number of DataFrame
    # "flightTimeParquetDF" partitions. So, if this DataFrame has got five partitions, you can expect five output
    # files, and each of those files will be written in parallel by five executers. If you have three executors and
    # five partitions, then they are going to share the five partitions among three executors. So it could be 2+2+1.

    logger.warn("Num Partitions before: " + str(flightTimeParquetDF.rdd.getNumPartitions()))
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()

    partitionedDF = flightTimeParquetDF.repartition(5)
    logger.warn("Num Partitions after: " + str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupBy(spark_partition_id()).count().show()

    partitionedDF.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", "dataSink/parquet/") \
        .save()

    flightTimeParquetDF.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "datasink/json/") \
        .option("maxRecordsPerFile", 10000) \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .save()
    # Repartitioning on a given column requires shuffling all the records. So, it is going to take some time comparing
    # with the previous approach.

    # So, my dataset had ten different flight carriers. And I got one directory for each carrier. Now, if you are
    # reading this dataset and want to filter it only for one carrier such as 'HP', then your Spark SQL engine can
    # optimize this read and filter operation, ignoring other directories and only reading the HP directory.

    # So we have a second level of directories. And this is caused by the second column in the partitionBy() method.
    # So, The carrier AA has got these many origins. And each origin becomes a subdirectory. If you go inside these
    # directories, then you will see the related data file.

    # Your data is partitioned by the unique combination of your partition columns. And these partitions are written
    # down in a separate directory. Each data file in these directories will hold data only for the given combination.
    # For example, this data file contains 1400 plus records, and all these records are for carrier=AA and origin=BOS.
    # In fact, those two columns are not even recorded in the data file. Because that would be redundant information.
    # If you look at the directory name, these are named as carrier=AA, and origin=BOS and these two columns are removed
    # from the data file.

    # So my largest data file is roughly 4 MB. And this one is created for OP_CAREER=DL and ORIGIN=ATL.
    # In fact, a 4MB file is too small for Spark. In real scenarios, you should have a much larger file.
    # I prefer managing file sizes between 500 MB to a couple of GBs. Not too big and not too small. To alter it,
    # use ".option("maxRecordsPerFile", 10000) \" option.

