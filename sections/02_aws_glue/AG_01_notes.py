import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import current_timestamp, col, lit, broadcast, when
from datetime import datetime


# #####################################################################################################################

# FILTERING
# Filtering doesn't have to be by using '.filter', we can also use '.sql' but we firstly need to convert the ddf to df
# Converting dynamicframe to dataframe so can use '.createOrReplaceTempView' (We need that to use '.sql') and '.sql'
AmazonRedshift_node1675772012938.toDF().createOrReplaceTempView("my_vanilla_data_source")
# Filtering the data
myDataSourceDF = spark.sql("select * from my_vanilla_data_source where cntry_cd = 'TR'")
# Converting back to dynamicframe
myDataSourceDynamicFrame = DynamicFrame.fromDF(myDataSourceDF, glueContext, "myDataSourceDynamicFrame")

# #####################################################################################################################

# CASTING TO DIFFERENT DATA TYPE
df = df.withColumn("dlvry_last_imported_timestamp", col("dlvry_last_imported_timestamp").cast("string"))

# #####################################################################################################################

# For last 2 years filter (if it's 2024, it will give the 1st day of 2023)
previous_year = (datetime.now().year) - 1
first_day_previous_year = datetime(previous_year, 1, 1, 0, 0, 0, 0)
formatted_first_day = first_day_previous_year.strftime('%Y-%m-%d %H:%M:%S.%f')

# Default values are prepared for "IllegalArgumentException: Don't know how to save NullType to REDSHIFT" error
default_value = ""
default_timestamp = "1970-01-01 00:00:00.000000"

df = selected_fields_AmazonRedshiftSource_product_vod__c_vw.toDF()
df = df.withColumn("createddate", col("createddate").cast("string"))
df = df.fillna({"createddate": default_timestamp})
df = df.fillna({"quantity_per_case_vod__c": 0})
# String Columns: Any null or None values in columns of string type will be replaced with an empty string "".
# Non-String Columns: Columns with other data types (e.g., integers, floats, timestamps) will not be affected by this operation.
# You need to set them separately as we just did on above lines.
df = df.fillna(default_value)
df = df.withColumn("createddate", col("createddate").cast("timestamp"))
df = df.filter(df["createddate"] >= lit(formatted_first_day))  # Filtering by timestamp
df1 = df.withColumn("insert_timestamp", current_timestamp())

# #####################################################################################################################

#default_timestamp = datetime(1970, 1, 1)
#default_timestamp2 = default_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
#df = df.fillna({"createddate": default_timestamp})

#default_timestamp_str = "1970-01-01 00:00:00.000000"
#df = df.withColumn("createddate", col("createddate").cast("string"))
#df = df.fillna({"createddate": default_timestamp_str})
#df = df.withColumn("createddate", col("createddate").cast("timestamp"))


# Fill empty timestamp fields with the default timestamp value (YOU MIGHT TRY THIS ONE)
#df = df.withColumn("createddate", when(df["createddate"].isNull(), lit(default_timestamp)).otherwise(df["createddate"]))

# Filtering by a string field
df = df.filter(df["createdbyid"] == "005U0000002WzjhIAC")

# #####################################################################################################################

# Broadcast & inner join:
df2 = broadcast(rename_selected_fields_node2.toDF())
joined_df = df1.join(df2,df1.country_code_az__c ==  df2.right_join_country_code,"inner")
joined_df_final = DynamicFrame.fromDF(joined_df, glueContext, "joined_df_final")

# #####################################################################################################################
