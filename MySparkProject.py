# 1.0: Include it all the time on all the Spark projects
import sys

import spark as spark
from pyspark import SparkConf
from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df

# 3.0: SparkSession is a Singleton object; so each Spark application can have one and only one active Spark
# -session. Because the SparkSession is your driver, and you cannot have more than one driver in a Spark application.

if __name__ == "__main__":

    # 4.5: SparkConf Object (for Spark Session Configs)
    # myConf = SparkConf()
    # myConf.set("spark.app.name", "Hello Spark")
    # myConf.set("spark.master", "local[3]")
    # 4.6: Hard-coding them is an issue, we will create a separate file 'spark.conf' and will load it on the run time

    myConf = get_spark_app_config()

    # 3.1: Let's get or create a Spark session:
    spark = SparkSession.builder \
        .config(conf=myConf) \
        .config("spark.driver.bindAddress", "localhost") \
        .getOrCreate()
    # .appName("Hello Spark") \
    # .master("local[3]") \
    # .config("spark.driver.bindAddress", "127.0.0.1") \

    spark.sparkContext.setLogLevel("INFO")

    # 4.3: We can now get a logger like this:
    logger = Log4J(spark)

    # 5.0:
    # if len(sys.argv) != 2:
    #     logger.error("Usage: HelloSpark <filename>")
    #     sys.exit(-1)
    # above??

    # 4.4: We can now use it wherever we want:
    logger.info("Starting HelloSpark Application")
    # Insert your processing code here
    ########################## INSERT YOUR PROCESSING CODE HERE ##########################

    # 4.7: Let's read our config:
    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())

    # 5.1: Data Frame Read + Options:
    # survey_df = spark.read \
    #     .option("header", "true") \
    #     .option("inferSchema", "true") \
    #     .csv(sys.argv[1])
    # 5.2: Let's put it in utils.py as a new method^^
    # survey_df = load_survey_df(spark, sys.argv[1])
    # above??
    survey_df = load_survey_df(spark, 'data/sample.csv')
    survey_df.show()

    ######################################################################################
    logger.info("Finished HelloSpark Application")

    spark.stop()
    