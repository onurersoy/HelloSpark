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

# FILTERING (SQL)
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

# FILTERING
# For last 2 years filter (if it's 2024, it will give the 1st day of 2023)
previous_year = (datetime.now().year) - 1
first_day_previous_year = datetime(previous_year, 1, 1, 0, 0, 0, 0)
formatted_first_day = first_day_previous_year.strftime('%Y-%m-%d %H:%M:%S.%f')

# Filtering by timestamp
df = selected_fields_AmazonRedshiftSource_product_vod__c_vw.toDF()
df = df.filter(df["createddate"] >= lit(formatted_first_day))

# Adding a new column to the df
df1 = df.withColumn("insert_timestamp", current_timestamp())

# Filtering by a string field
df = df.filter(df["createdbyid"] == "005U0000002WzjhIAC")

# #####################################################################################################################

# NULLTYPE ISSUES
# "IllegalArgumentException: Don't know how to save NullType to REDSHIFT"

# Schema inference might have failed if the column contains only NULL values (If all values are NULL in Redshift,
# PySpark might not correctly infer it as StringType, causing the column to appear as null type.)
# Fix by trimming, explicitly casting, or defining the schema manually.

# First, use 'df1.printSchema()' to detect nulltype values.
# 'fillna("")' option will replace NULL values in existing columns, but it will not fix columns that are already inferred
# as null type fields. So it won't solve most of the problematic cases if the issue is a nulltype error.

# You need to explicitly cast these columns to StringType before applying 'fillna("")'.
# After that, you can apply 'fillna("")'.
# Example1:
df2 = selected_fields_AmazonRedshiftSource_eu3_vbrp_vw.toDF()
for col_name in ["waerk", "auref", "atpkz"]:
    df2 = df2.withColumn("col_name", col("col_name").cast("string"))
default_value = ""
df2 = df2.fillna(default_value)  # Fills all the null values with 'default_value' across all the fields/columns (global fillna)
# df1 = df1.fillna({"cust_visid": default_value})  # Fills all the null values within 'cust_visid' field/column with 'default_value'
# PS: Remember that this method replaces null records with an empty "", which is not empty/null any more. So let's say
# you count the 'is null' records, those records won't be counted as null!

"""The difference between NULL areas and completely blank/empty areas in your table is likely due to how the database or Spark handles missing values:

I. NULL Areas (Explicitly Showing [NULL])
- These are true NULL values, meaning the data is explicitly missing.
- In PySpark, isNull() would return True for these fields.
- SQL queries using WHERE column IS NULL will capture these values.

II. Completely Blank/Empty Areas (No [NULL], Just Empty)
- These are likely empty strings ("") instead of NULL.
- In PySpark, isNull() would return False, but (col == "") would return True.
- SQL queries using WHERE column = '' will capture these values, but WHERE column IS NULL will not."""

# Example2:
df = selected_fields_AmazonRedshiftSource_product_vod__c_vw.toDF()
df = df.withColumn("createddate", col("createddate").cast("string"))
default_timestamp = "1970-01-01 00:00:00.000000"
df = df.fillna({"createddate": default_timestamp})
# TODO^^ What does it do if some records are not null? Do they get replaced too?
# Then you can cast back to its expected type:
df = df.withColumn("createddate", col("createddate").cast("timestamp"))

# Example3 (only 'fillna'):
df = df.fillna({"quantity_per_case_vod__c": 0})

# SOME NOTES:
df = df.fillna(default_value)
#^^ String Columns: Any null or None values in columns of string type will be replaced with an empty string.
#^^ Non-String Columns: Columns with other data types (e.g., integers, floats, timestamps) will not be affected by this
# operation. You need to set them separately as we did on above lines (Example #2 & #3).

# Fill empty timestamp fields with the default timestamp value, leave the existed records as they are (YOU MIGHT WANNA TRY THIS ONE)
df = df.withColumn("createddate", when(df["createddate"].isNull(), lit(default_timestamp)).otherwise(df["createddate"]))

# #####################################################################################################################

# BROADCAST & INNER JOIN:
df2 = broadcast(rename_selected_fields_node2.toDF())
joined_df = df1.join(df2,df1.country_code_az__c ==  df2.right_join_country_code,"inner")
joined_df_final = DynamicFrame.fromDF(joined_df, glueContext, "joined_df_final")

# #####################################################################################################################
