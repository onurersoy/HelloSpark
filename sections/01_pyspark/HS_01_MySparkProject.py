# 1.0: Include it all the time on all the Spark projects
import sys

from pyspark.sql import *

from lib import Log4J
from lib import get_spark_app_config, load_survey_df, count_by_country

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

    # spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("INFO")

    # 4.3: We can now get a logger like this:
    logger = Log4J(spark)

    # 4.4: We can now use it wherever we want:
    logger.info("Starting HelloSpark Application")
    # Insert your processing code here
    ########################## INSERT YOUR PROCESSING CODE HERE ##########################

    # 4.7: Let's read our config:
    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())

    # 5.0: Data Frame Read + Options:
    # 5.1:
    if len(sys.argv) != 2:
        logger.info(sys.argv)
        logger.error("Usage Error: HelloSpark/data/sample.csv")
        sys.exit(-1)

    # survey_df = spark.read \
    #     .option("header", "true") \
    #     .option("inferSchema", "true") \
    #     .csv(sys.argv[1])
    # 5.2: Let's put it in utils.py as a new method^^

    # 5.3:
    survey_df = load_survey_df(spark, sys.argv[1])
    # survey_df = load_survey_df(spark, 'data/sample.csv')

    # 7.0: It's nice to have a control over partition number as we could have a lot bigger file to test on local
    # comparing with the current one; more partition could be better for unit testing on some use cases etc.
    partitioned_survey_df = survey_df.repartition(2)
    # 7.1^^: The repartition() is a transformation. So it takes a DataFrame as input and produces another DataFrame.

    # survey_df.show()

    # 6.0:
    # 6.1: Let's put them on a separate function on utils.py
    # filtered_survey_df = survey_df \
    #     .where("Age < 40") \
    #     .select("Age", "Gender", "Country", "state")
    # grouped_df = filtered_survey_df.groupBy("Country")
    # count_df = grouped_df.count()

    # count_df.show()
    
    # 7.2:
    count_df = count_by_country(partitioned_survey_df)
    # 8.0: We can now either use .show() or .collect() to get the results.
    # 8.1: The collect action returns the DataFrame as Python List. However, the show() method is a utility function
    # to print the DataFrame. So, you are more likely to use the collect() action, whereas the show() is useful for
    # debugging purposes. The second reason is, the show() method compiles down to complex internal code.
    # It might create unnecessary confusion when we are trying to learn a complex topic.
    logger.info(count_df.collect())

    # 9.0: For local debugging (not for production): So that the application won't finish, and we can keep checking
    # the Spark UI as it's only available during the lifetime of the application.
    input("Press Enter")

    ######################################################################################
    logger.info("Finished HelloSpark Application")

    spark.stop()
