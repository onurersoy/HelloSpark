import sys
import pandas
import re
import subprocess

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Misc Demo") \
        .master("local[3]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    logger = Log4J(spark)

    data_list = [("Ravi", 28, 1, "2002"),
                 ("Abdul", 23, 5, "81"),
                 ("John", 12, 12, "6"),
                 ("Rosy", 7, 8, "63"),
                 ("Abdul", 23, 5, "81")
                 ]

    ############################################################################################################
    # CREATING DATAFRAME:
    # raw_df = spark.createDataFrame(data_list)
    raw_df = spark.createDataFrame(data_list).toDF("name", "day", "month", "year")
    # raw_df = spark.createDataFrame(data_list, ["name", "day", "month", "year"])  # It is the same^^
    raw_df.printSchema()

    ############################################################################################################
    # MONOTONICALLY/UNIQUELY INCREASING ID:
    df1 = raw_df.withColumn("id", monotonically_increasing_id())
    # withColumn(): We know that it is normally for transforming an existing column. You can also use this method to
    # add a new column into the data frame.
    # This 'monotonically_increasing_id()' is a built-in function that randomly generates unique numbers

    ############################################################################################################
    # CASE WHEN (SWITCH-CASE):
    # SQL-LIKE EXPRESSION:
    # Now let's correct the years:
    df2 = df1.withColumn("year", expr("""
    case when year < 21 then year + 2000
    when year < 100 then year + 1900
    else year
    end"""))
    df2.show()  # Notice that 'year' returns as decimal

    ############################################################################################################
    # HOW TO CAST YOUR FIELD:
    # i. Inline Cast
    df3 = df1.withColumn("year", expr("""
    case when year < 21 then cast(year as int) + 2000
    when year < 100 then cast(year as int) + 1900
    else year
    end"""))
    df3.show()
    # ii. Change the Schema
    df4 = df1.withColumn("year", expr("""
    case when year < 21 then year + 2000
    when year < 100 then year + 1900
    else year
    end""").cast(IntegerType()))
    df4.show()
    df4.printSchema()

    # PS. The better way is casting the field to the correct type in the beginning:
    df1.show()
    df1.printSchema()
    df5 = df1.withColumn("year", col("year").cast(IntegerType()))
    df5 = df1.withColumn("year", expr("""
    case when year < 21 then year + 2000
    when year < 100 then year + 1900
    else year
    end"""))
    df5.show()
    df5.printSchema()

    ############################################################################################################
    # COLUMN OBJECT EXPRESSION - ALTERNATIVE VERSION FOR CASE WHEN:
    df7 = df5.withColumn("year", when(col("year") < 21, col("year") + 2000) \
                         .when(col("year") < 100, col("year") + 1900) \
                         .otherwise(col("year")))
    df7.show()

    ############################################################################################################
    # ADDING & REMOVING COLUMNS:
    df8 = df7.withColumn("our_new_date_field", expr("to_date(concat(day,'/',month,'/',year), 'd/M/y')"))
    df8.show()
    # OR:
    df9 = df7.withColumn("our_new_date_field", to_date(expr("concat(day,'/',month,'/',year)"), 'd/M/y')) \
    .drop("day", "month", "year")  # Dropping the no longer necessary columns from df
    df9.show()

    ############################################################################################################
    # DROPPING DUPLICATES AND SORTING DESCENDING:
    df9 = df9.dropDuplicates(["name", "our_new_date_field"]) \
    .sort(expr("our_new_date_field desc"))
    df9.show()
