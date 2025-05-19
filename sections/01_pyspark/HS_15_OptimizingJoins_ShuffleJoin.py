from pyspark.sql import SparkSession

from lib.logger import Log4J

# Let's confire the session to have 3 threads:
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Shuffle Join Demo") \
        .master("local[3]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.executorEnv.SPARK_LOCAL_IP", "127.0.0.1") \
    .getOrCreate()

    logger = Log4J(spark)

    flight_time_df1 = spark.read.json("data/d1/")
    flight_time_df2 = spark.read.json("data/d2/")

    # To get 3 partitions after the shuffle (which means 3 reduce exchanges):
    spark.conf.set("spark.sql.shuffle.partitions", 3)

    join_expr = flight_time_df1.id == flight_time_df2.id
    join_df = flight_time_df1.join(flight_time_df2, join_expr, "inner")

    # Join is a transformation, so nothing will really happen until we take an action, so let's add this:
    # PS. localhost:4040/jobs
    join_df.collect()
    input("press a key to stop...")
