from pyspark.sql import SparkSession
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

    # Please be noted that the below data files are not available in this repository
    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/invoices.csv")

    ############################################################################################################
    # SIMPLE AGGREGATIONS: They always give you one-line summary
    # The below aggregates are functions, so we can use them in Column Object Expression:
    invoice_df.select(
        f.count("*").alias("Count *"),
        f.sum("Quantity").alias("TotalQuantity"),
        f.avg("UnitPrice").alias("AvgPrice"),
        f.countDistinct("InvoiceNo").alias("CountDistinct")
    ).show()

    # We can also use them in SQL-Like String Expression:
    invoice_df.selectExpr(
        "count(1) as `count 1`",  # count(*) and count(1) are the same; they count all the records even if they have
        # null values on all the columns
        "count(StockCode) as `count field`",  # It does not count the null values
        "sum(Quantity) as TotalQuantity",
        "avg(UnitPrice) as AvgPrice"
    ).show()

    ############################################################################################################
    # GROUPING AGGREGATIONS:
    invoice_df.createOrReplaceTempView("sales")
    summary_sql = spark.sql("""
    SELECT Country, InvoiceNo, 
    sum(Quantity) as TotalQuantity,
    round(sum(Quantity * UnitPrice),2) as InvoiceValue
    FROM sales
    GROUP BY Country, InvoiceNo""")
    summary_sql.show()

    # We can do the same above by using dataframe expressions also:
    summary_df = invoice_df \
        .groupBy("Country", "InvoiceNo") \
        .agg(f.sum("Quantity").alias("TotalQuantity"),
             f.round(f.sum(f.expr("Quantity * UnitPrice")),2).alias("InvoiceValue"),
             f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue2")  # << We can also do it like this
             )
    summary_df.show()

    ############################################################################################################
    # Example!:
    TotalQuantity = f.sum("Quantity").alias("TotalQuantity")
    InvoiceValue = f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue")

    exampleSummary_df = invoice_df \
        .withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy H.mm")) \
        .where("year(InvoiceDate) == 2010") \
        .withColumn("WeekNumber", f.weekofyear(f.col("InvoiceDate"))) \
        .groupBy("Country", "WeekNumber") \
        .agg(f.countDistinct("InvoiceNo").alias("NumInvoices"), TotalQuantity, InvoiceValue)

    exampleSummary_df.coalesce(1) \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save("output")
    exampleSummary_df.sort("Country", "WeekNumber").show()
