from pyspark.sql import SparkSession

from lib.logger import Log4J

# Shuffle join is almost unavoidable in case of a large to large dataset joins.
# However, in some scenarios, you can prepare yourself in advance and avoid the shuffle at the time of joining.
# Now let's learn about the bucketing your dataset and avoiding shuffle altogether at the join time.

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Bucket Join Demo") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .master("local[3]") \
    .getOrCreate()

    logger = Log4J(spark)

    # I am assuming both of these datasets are large, and none could fit into the memory of a single executor. So,
    # I cannot apply for the broadcast join. However, I know that I am going to join these two datasets. And
    # possibly, I will be joining it several times for different types of queries. Can we plan the join in advance
    # and layout my dataset so that the join can be done quickly? The goal is to avoid shuffle. That's where Spark
    # bucketing could be handy.

    # So, if you have two datasets that you already know you are going to join in the future, then it is advisable to
    # bucket both of your datasets using your join key. Bucketing your dataset may also require a shuffle. However,
    # that shuffle is needed only once when you create your bucket. Once you have a bucket, you can join these
    # datasets without a shuffle, and you can do it as many times as you need. So, bucketing is helpful to prepone
    # the shuffling activity and do it only once. You can avoid the shuffle at the time of joining.
    df1 = spark.read.json("data/d1/")
    df2 = spark.read.json("data/d2/")

    # Colease it to a single partition and then write it to get the DataFrameWriter. Then we use the bucketBy(). The
    # first argument is the number of buckets. How many buckets do you want to create? I am going to create three
    # buckets. Because I will run it using three threads, and I want three partitions or buckets to achieve the maximum
    # possible parallelism on my three-node cluster.
    spark.sql("CREATE DATABASE IF NOT EXISTS MY_DB")
    spark.sql("USE MY_DB")

    df1.coalesce(1).write \
        .bucketBy(3, "id") \
        .mode("overwrite") \
        .saveAsTable("MY_DB.flight_data1")

    df2.coalesce(1).write \
        .bucketBy(3, "id") \
        .mode("overwrite") \
        .saveAsTable("MY_DB.flight_data2")

    # The number of buckets or the number of partitions is a critical decision. You can decide this number if you
    # understand your dataset, and you also know something about the available cluster capacity.

    # For example, Your dataset is 100 Gigs, and you are creating 10 partitions. That means you are planning to
    # process it in 10 executors in parallel. And in an ideal case, you are expecting each partition to have 10GB of
    # data. However, you may not get 10GB equal partitions. Because your partition key might have a skew. We want a
    # predictable performance, which we can scale by adding more executors to do it faster. And scaling your spark
    # application is more of maximizing the parallelism and minimizing the skew.

    df3 = spark.read.table("MY_DB.flight_data1")
    df4 = spark.read.table("MY_DB.flight_data2")

    # We are reading a managed table. And these tables are actually small. So, Spark might automatically pick up a
    # broadcast join. I don't want Spark to apply a broadcast join here. So, let me set the
    # auto-broadcast-join-threshold. So, I am setting this value to -1. This setting will disable the broadcast join.
    spark.conf.set("spark.sql.autoBroadcastJoinThrehsold", -1)
    join_expr = df3.id == df4.id
    join_df = df3.join(df4, join_expr, "inner")

    # A plain sort-merge join without a shuffle^^. So we learned to avoid shuffle in our joins. However, you need to
    # plan it ahead. Creating buckets is a design-time decision. The idea is straightforward. Understand your data.
    # And also understand what and how you are going to use it.

    join_df.collect()
    input("press a key to stop...")
