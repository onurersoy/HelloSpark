import sys
from pyspark.sql import SparkSession
from lib.logger import Log4J

# 10.0
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("MySparkProjectSQL") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    surveyDF = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv('data/sample.csv')

    # 10.1: We create a tempView before using .sql
    surveyDF.createOrReplaceTempView("survey_tbl")
    countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")
    countDF.show()
