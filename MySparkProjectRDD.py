import sys
from collections import namedtuple

from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from lib.logger import Log4J

# 11.0

# To create a schema:
SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])

if __name__ == "__main__":
    conf = SparkConf() \
        .setMaster("local[3]") \
        .setAppName("MySparkProjectRDD")
    # sc = SparkContext(conf=conf)

    # SparkSession is a higher-level object which was created in the newer versions of Spark as an improvement over
    # the SparkContext. However, The SparkSession still holds the SparkContext and uses it internally. This is why we
    # will create a SparkSession then use it to get a SparkContext.
    spark = SparkSession \
        .builder \
        .config(conf=conf)\
        .getOrCreate()


    ###### CREATING RDD ######
    sc = spark.sparkContext
    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

        linesRDD = sc.textFile(sys.argv[1])
    ##########################


    ###### PROCESSING RDD ######
    linesRDD = sc.textFile(sys.argv[1])
    partitionedRDD = linesRDD.repartition(2)

    colsRDD = partitionedRDD.map(lambda line: line.replace('"', '').split(","))
    selectRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4]))
    filteredRDD = selectRDD.filter(lambda r: r.Age < 40)
    kvRDD = filteredRDD.map(lambda r: (r.Country, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)

    colsList = countRDD.collect()
    for x in colsList:
        logger.info(x)
    ##########################
