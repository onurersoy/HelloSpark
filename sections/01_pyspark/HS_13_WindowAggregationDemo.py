from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Agg Demo") \
        .master("local[2]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

    logger = Log4J(spark)

    ############################################################################################################
    # WINDOW AGGREGATIONS:
    # i. Identify your partitioning columns: Country
    # ii. Identify your ordering requirement: Order by week number
    # iii. Define your windows start and end: All dataset, starting from the 1st week

    # Please be noted that the below data files are not available in this repository
    summary_df = spark.read.parquet("data/summary.parquet")

    running_total_window = Window.partitionBy("Country") \
        .orderBy("WeekNumber") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)  # 'unboundedPreceding' means take all the rows
    # from the beginning

    summary_df.withColumn("RunningTotal",
                          f.sum("InvoiceValue").over(running_total_window)) \
        .show()
