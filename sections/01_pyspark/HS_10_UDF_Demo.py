import sys
import pandas
import re
import subprocess

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from lib.logger import Log4J

print("PYTHON VERSION:", sys.version)

try:
    import pyarrow
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyarrow>=4.0.0"])
    import pyarrow


def parse_gender(gender):
    # we will use regular expressions:
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return("Female")
    elif re.search(male_pattern, gender.lower()):
        return("Male")
    else:
        return "Unknown"


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("UDF Demo") \
        .master("local[2]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    logger = Log4J(spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/survey.csv")

    print(survey_df.count())
    survey_df.show(10)

    # 1ST APPROACH: COLUMN OBJECT EXPRESSION
    # If you want to use your UDF in a DF Column Expression, you need to register it like this:
    parse_gender_udf = udf(parse_gender, StringType())  # 'StringType()' is the return type, and it was optional to
    # define it here
    survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    survey_df2.show(10)

    # You could replace the UDF with a native Spark expression:
    # survey_df2 = survey_df.withColumn(
    #     "Gender",
    #     when(col("Gender").rlike(r"(?i)^f$|f.m|w.m"), "Female")
    #     .when(col("Gender").rlike(r"(?i)^m$|ma|m.l"), "Male")
    #     .otherwise("Unknown")
    # )
    # This version will be much faster, especially on large datasets, as it avoids Python serialization overhead.

    # 2ND APPROACH: SQL EXPRESSION
    # ıf you want to use your UDF in SQL Expression, you need to register it like this:
    spark.udf.register("parse_gender_udf2", parse_gender, StringType())
    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf2(Gender)"))
    survey_df3.show(10)
