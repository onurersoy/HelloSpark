# Filtering doesn't have to be by using '.filter', we can also use '.sql' but we firstly need to convert the ddf to df
# Converting dynamicframe to dataframe so can use '.createOrReplaceTempView' (We need that to use '.sql') and '.sql'
AmazonRedshift_node1675772012938.toDF().createOrReplaceTempView("my_vanilla_data_source")
# Filtering the data
myDataSourceDF = spark.sql("select * from my_vanilla_data_source where cntry_cd = 'TR'")
# Converting back to dynamicframe
myDataSourceDynamicFrame = DynamicFrame.fromDF(myDataSourceDF, glueContext, "myDataSourceDynamicFrame")
