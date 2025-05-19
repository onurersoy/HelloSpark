from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Spark Join Demo") \
        .master("local[3]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    logger = Log4J(spark)

    orders_list = [("01", "02", 350, 1),
                   ("01", "04", 580, 1),
                   ("01", "07", 320, 2),
                   ("02", "03", 450, 1),
                   ("02", "06", 220, 1),
                   ("03", "01", 195, 1),
                   ("04", "09", 270, 3),
                   ("04", "08", 410, 2),
                   ("05", "02", 350, 1)]

    order_df = spark.createDataFrame(orders_list).toDF("order_id", "prod_id", "unit_price", "qty")

    product_list = [("01", "Scroll Mouse", 250, 20),
                    ("02", "Optical Mouse", 350, 20),
                    ("03", "Wireless Mouse", 450, 50),
                    ("04", "Wireless Keyboard", 580, 50),
                    ("05", "Standard Keyboard", 360, 10),
                    ("06", "16 GB Flash Storage", 240, 100),
                    ("07", "32 GB Flash Storage", 320, 50),
                    ("08", "64 GB Flash Storage", 430, 25)]

    product_df = spark.createDataFrame(product_list).toDF("prod_id", "prod_name", "list_price", "qty")

    order_df.show()
    product_df.show()

    join_expr = order_df.prod_id == product_df.prod_id

    # We will re-name one of 'quantity' fields because it exists on both dfs otherwise we get an error.
    # There would be no error if we would use 'select("*")' instead of explicitly defining the fields. We would get
    # all the fields even that they are ambiguous, but why?:
    # Every Dataframe column has a unique ID in the catalog, and the Spark engine always works using those internal ids.
    # These ids are not shown to us, and we are expected to work with the column names. However, the spark engine will
    # translate these column names to ids during the analysis phase. When I refer the column name in my expression,
    # then the Spark engine will internally translate the column names to the column id. And that's where it complains
    # about ambiguity. But when we are using select *, it takes all the column ids and shows them.

    # We have two approaches to avoid such an ambiguity:
    # Let's rename one of the ambiguous fields and drop one of the other ambiguous fields:
    product_renamed_df = product_df.withColumnRenamed("qty", "reorder_qty")

    # Default join is inner join, so we do not really have to specify it, but we still can:
    order_df.join(product_renamed_df, join_expr, "inner") \
        .drop(product_renamed_df.prod_id) \
        .select("order_id", "prod_id", "prod_name", "unit_price", "list_price", "qty") \
        .withColumn("prod_name", expr("coalesce(prod_name, prod_id)")) \
        .withColumn("list_price", expr("coalesce(list_price, unit_price)")) \
        .show()
    # Join types:
    # inner
    # outer(full outer)
    # left(left outer)
    # right(right outer)
