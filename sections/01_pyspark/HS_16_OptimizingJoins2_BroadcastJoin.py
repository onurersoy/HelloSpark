from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

from lib.logger import Log4J


"""JOIN INTERNALS:
1. Large to Large (large enough to not fit on a memory of a single executor)
    > Shuffle join! We do not have any other option
2. Large to Small (small enough to fit on a memory of a single executor)
    > Broadcast join, which often works faster than the shuffle joins
PS. However, if your dataset is more than a few GBs, it should be considered large and must be broken down into
multiple partitions to achieve parallel processing."""

############################################################################################################

"""DON’T CODE LIKE A NOVICE:
Look for all the possible opportunities to reduce the Dataframe size.
The recommendation is to look for opportunities where you can aggregate even before joining the Dataframes.
The objective is almost always to cut down the size of your Dataframes as early as possible."""


"""SHUFFLE PARTITIONS AND PARALLELISM: The second thing is to look out for the number of shuffle partitions and
the number of executors. These two are going to determine the degree of parallelism. You should start by asking
this question. What is the maximum possible parallelism for my join operation?: The first and most common limit
is the number of executors. If you can run your job with 500 executors, then that's your maximum limit. The
second limit comes from the number of shuffle partitions. If you have 500 executors, but you configured to have
400 shuffle partitions, then your maximum is limited to 400. Why? Because you have only 400 partitions to
process in parallel. The third limit comes from the number of unique join keys. If you have only 200 unique
keys, then you can have only 200 shuffle partitions. Even if you define 400 shuffle partitions, only 200 of
those will have data, and others will remain blank.

Consider the following Dataframes: You are joining these two Dataframes on product_id. And you have 200 unique
products. You can have a maximum of 200 reduce exchanges where each unique key goes to its dedicated exchange.
I mean, one reduce exchange should get records for only one unique key. And you already know that each exchange
is equal to one shuffle partition. That means, your joined Dataframe can have up to 200 partitions. And you can
take full advantage of 200 partitions by running your join operation on a 200 node Spark cluster. So,
each node is going to process only one partition, and everything gets processed in parallel.

If you have a 100 node cluster, then each node might have to process two partitions taking double the time in
the ideal case. The point is straight. If you have 200 unique keys, then you can have a maximum of 200 parallel
tasks. Even if you run your job on a 500 node cluster, you will be running only 200 parallel tasks. If you want
to scale your Join operation and take advantage of a larger cluster, you should increase the number of shuffle
partitions. If the number of the unique key is limiting your scalability, try increasing your join key's
cardinality. Changing your join key to increase the cardinality or applying some trick to increase the
cardinality of your current join key may not always be possible. However, you should be looking for
opportunities to improve the parallelism if you have a large cluster."""


"""KEY DISTRIBUTION: The next important thing is to look for the distribution of your data across the keys. For 
example, you are joining the sales and product Dataframe. We consider that you have 200 products. So, 
all your sales transactions are joined with 200 products resulting in 200 shuffle partitions. However, 
you may have some fast-moving products, some are slow-moving items, and others may be very slow-moving 
products. So, a fast-moving product should have a lot of transactions, and all of those will land into one 
partition because they all belong to the same product key. If you compare the size of your fastest-moving 
product partition vs. your slowest moving product partition, the difference could be significant. The Spark 
task, which is sorting and joining the larger partition, may take a lot of time, whereas the smaller partition 
will get joined very quickly. However, your join operation is not complete until all the partitions are joined.
The point is straight. Watch out for the time taken by individual tasks and the amount of data processed by the
join task. If some tasks are taking significantly longer than other tasks, you have an opportunity to fix your 
join key or apply some hack to break the larger partition into more than one partition. In all this 
discussion, the most essential point is this.

Shuffle joins could become severely problematic for the following principal reasons: 
i. You are joining huge volumes - filter/aggregate
ii. Parallelism - shuffles/executors/keys 
iii. Shuffle distribution - key skews"""

############################################################################################################

"""BROADCAST JOIN:
> This approach is known as broadcast join. Why? Because we are broadcasting the smaller table to all the executors.
> The broadcast join is an easy method to eliminate the need for a shuffle.
> You can perform a broadcast join without a shuffle.
> However, this approach works only when one of your dataframes is small. When I say small, it doesn't mean a few 
MBs. It could be 1GB or 2GB. The point is to make sure that your driver and executors have enough memory to 
accommodate your dataframe.
> In most of the cases, Spark will automatically use the broadcast join when one of your dataframe is smaller, and
 it can be broadcasted. However, you know your Dataframes a lot better than Spark. So you should apply a hint for 
 using the broadcast join."""


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Shuffle Join Demo") \
        .master("local[3]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

    logger = Log4J(spark)

    flight_time_df1 = spark.read.json("data/d1/")
    flight_time_df2 = spark.read.json("data/d2/")

    spark.conf.set("spark.sql.shuffle.partitions", 3)

    join_expr = flight_time_df1.id == flight_time_df2.id
    join_df = flight_time_df1.join(broadcast(flight_time_df2), join_expr, "inner")

    join_df.collect()
    input("press a key to stop...")
