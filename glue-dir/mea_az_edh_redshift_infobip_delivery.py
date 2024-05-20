import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
# Be noticed that some imports below might not be used on the below code
from pyspark.sql.functions import coalesce, lit, col, when, udf, regexp_replace, to_timestamp


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# No1: Script for node glbl_infobip_delivery_log_vw_v2
glbl_infobip_delivery_log_vw_v2_node = glueContext.create_dynamic_frame.from_catalog(
    database="mea_prod_cdh_datalake", redshift_tmp_dir="s3://az-eu-meadatalake/temp-dir/",
    table_name="im_dw_mea_gdsdata_ads_glbl_infobip_delivery_log_vw_v2",
    transformation_ctx="glbl_infobip_delivery_log_vw_v2_node")

selected_fields_filtered_frame_glbl_infobip_delivery_log_vw_v2 = SelectFields.apply(
    frame=glbl_infobip_delivery_log_vw_v2_node,
    # Using 'paths' drops the rest of the fields from the df
    paths=["cust_guid", "dlvry_email_cntct_addr", "trckng_url_type_open", "trckng_url_type_email_click", "market_code",
           "dlvry_id", "cmpgn_name", "cmpgn_brand", "cmpgn_type", "dlvry_name", "dlvry_channel", "delivery_market",
           "dlvry_evnt_ts", "cmpgn_strt_ts", "cmpgn_end_ts", "dlvry_last_imported_timestamp"],
    transformation_ctx="selected_fields_filtered_frame_glbl_infobip_delivery_log_vw_v2")

# No2: Script for node im_rpt.pub_glbl.cust_alt_cntc_d_vw
im_rptpub_glblcust_alt_cntc_d_vw_node = glueContext.create_dynamic_frame.from_catalog(
    database="mea_prod_cdh_datalake", redshift_tmp_dir="s3://az-eu-meadatalake/temp-dir/",
    table_name="im_rpt_pub_glbl_cust_alt_cntc_d_vw",
    transformation_ctx="im_rptpub_glblcust_alt_cntc_d_vw_node")

selected_fields_im_rptpub_glblcust_alt_cntc_d_vw_node = SelectFields.apply(
    frame=im_rptpub_glblcust_alt_cntc_d_vw_node,
    paths=["cust_guid", "comm_chnl_val"],
    transformation_ctx="selected_fields_im_rptpub_glblcust_alt_cntc_d_vw_node")

Renamedkeysfor_selected_fields_im_rptpub_glblcust_alt_cntc_d_vw_node = ApplyMapping.apply(
    frame=selected_fields_im_rptpub_glblcust_alt_cntc_d_vw_node, mappings=[
        ("cust_guid", "string", "right_join1_cust_guid", "string"),
        ("comm_chnl_val", "string", "right_join1_comm_chnl_val", "string")],
    transformation_ctx="Renamedkeysfor_selected_fields_im_rptpub_glblcust_alt_cntc_d_vw_node")

# No3: Script for node im_rpt.pub_glbl.cust_d_vw
im_rptpub_glblcust_d_vw_node = glueContext.create_dynamic_frame.from_catalog(
    database="mea_prod_cdh_datalake", redshift_tmp_dir="s3://az-eu-meadatalake/temp-dir/",
    table_name="im_rpt_pub_glbl_cust_d_vw",
    transformation_ctx="im_rptpub_glblcust_d_vw_node")

selected_fields_im_rptpub_glblcust_d_vw_node = SelectFields.apply(
    frame=im_rptpub_glblcust_d_vw_node,
    paths=["cust_guid", "cust_nm"],
    transformation_ctx="selected_fields_im_rptpub_glblcust_d_vw_node")

Renamedkeysfor_selected_fields_im_rptpub_glblcust_d_vw_node = ApplyMapping.apply(
    frame=selected_fields_im_rptpub_glblcust_d_vw_node,
    # Using 'mappings' drops the rest of the fields from the df, you can convert or re-name the fields on this step
    mappings=[
        ("cust_guid", "string", "right_join2_cust_guid", "string"),
        ("cust_nm", "string", "right_join2_cust_nm", "string")],
    transformation_ctx="Renamedkeysfor_selected_fields_im_rptpub_glblcust_d_vw_node")

# Converting a dynamic data frame to data frame
df1 = selected_fields_filtered_frame_glbl_infobip_delivery_log_vw_v2.toDF()

# Filtering the data frame
# We probably don't have to convert to df to filter it by using '.filter' but we did it anyway because we need it as a
# df for the below operations after filtering.
df1 = df1.filter(
    (df1["delivery_market"] == "TR") &
    (df1["dlvry_evnt_status"] == "Delivered") &
    (~df1["dlvry_email_cntct_addr"].like("%@astrazeneca.com"))
    # The below code did not execute as expected, but the issue should be about the different timestamp formats on the
    # source. That's why some of the records were not parsed correctly and the job brought an inaccurate filtered data
    # frame:
    #(year(to_timestamp(df1["dlvry_evnt_ts"], 'dd/MM/yyyy HH:mm:ss')) >= lit(2022))
)


# Not all the comment-outed lines are working perfectly; expect errors or syntax issues, added to spark an idea!
# #####################################################################################################################
#df1 = df1.withColumn("dlvry_last_imported_timestamp", regexp_replace("dlvry_last_imported_timestamp", "\\+\\d{2}:\\d{2}$", ""))
#df1 = df1.withColumn("dlvry_last_imported_timestamp", to_timestamp("dlvry_last_imported_timestamp", "yyyy-MM-dd HH:mm:ss.SSSSSS"))

#df1 = df1.filter(col("trckng_url_type_open").rlike("^\\d+$"))
#df1 = df1.filter(col("trckng_url_type_email_click").rlike("^\\d+$"))

#df1 = df1.withColumn("trckng_url_type_open", when(col("trckng_url_type_open").isNull(), 0).otherwise(col("trckng_url_type_open")))
#df1 = df1.withColumn("trckng_url_type_email_click", when(col("trckng_url_type_email_click").isNull(), 0).otherwise(col("trckng_url_type_email_click")))

#df1 = df1.withColumn("trckng_url_type_open", safe_cast_int_function("trckng_url_type_open"))
#df1 = df1.withColumn("trckng_url_type_email_click", safe_cast_int_function("trckng_url_type_email_click"))

#df1 = df1.withColumn("trckng_url_type_open", regexp_replace(col("trckng_url_type_open"), "[^0-9]", ""))
#df1 = df1.withColumn("trckng_url_type_email_click", regexp_replace(col("trckng_url_type_email_click"), "[^0-9]", ""))

#df1 = df1.withColumn("trckng_url_type_open", col("trckng_url_type_open").cast("int"))
#df1 = df1.withColumn("trckng_url_type_email_click", col("trckng_url_type_email_click").cast("int"))
#df1 = df.withColumn("cmpgn_end_ts", to_timestamp("cmpgn_end_ts", "yyyy-MM-dd HH:mm:ss.SSSSSS"))
# #####################################################################################################################


df2 = Renamedkeysfor_selected_fields_im_rptpub_glblcust_alt_cntc_d_vw_node.toDF()
df3 = Renamedkeysfor_selected_fields_im_rptpub_glblcust_d_vw_node.toDF()

# Demonstrating left joins (joining data frames)
joined1_df = df1.join(df2,df1.dlvry_email_cntct_addr ==  df2.right_join1_comm_chnl_val,"left")
joined2_df = joined1_df.join(df3,joined1_df.right_join1_cust_guid ==  df3.right_join2_cust_guid,"left")

# Demonstrating coalesce operation
df = joined2_df.withColumn(
    "cust_guid", coalesce("cust_guid", "right_join1_cust_guid"))

df = df.withColumn(
    "right_join2_cust_nm", coalesce("right_join2_cust_nm", lit("Unknown")))

# Be informed that the format of ts must be exactly how it's coming from the source! Otherwise the parsed data might be
# 'null'.
# df = df.withColumn("dlvry_evnt_ts", to_timestamp("dlvry_evnt_ts", "dd/MM/yyyy HH:mm:ss"))
# df = df.withColumn("cmpgn_strt_ts", to_timestamp("cmpgn_strt_ts", "dd/MM/yyyy HH:mm:ss"))
# df = df.withColumn("cmpgn_end_ts", to_timestamp("cmpgn_end_ts", "dd/MM/yyyy HH:mm:ss"))

# Converting the data frame back to a dynamic data frame
joined_frame_final = DynamicFrame.fromDF(df, glueContext, "joined_frame_final")


# Drop field operation (We could do the same on mapping part by not adding the field we'd like to drop...)
dropped_joined_frame_final = joined_frame_final.drop_fields(
    ["right_join1_cust_guid", "right_join2_cust_guid", "right_join1_comm_chnl_val"])

# Re-name, convert or drop on this part
# The order of the fields almost never matter as the spark job using fields' names when writing them to the target table.
# So make sure that the field names are correct and the data types are convenient for the parsing operations.
# The left-side fields of the mapping section referring to our source data which is 'dropped_joined_frame_final' dynamic
# data frame on this example.
mapping = [
    ("dlvry_evnt_ts", "string", "dlvry_evnt_ts", "string"),
    ("delivery_market", "string", "delivery_market", "string"),
    ("dlvry_email_cntct_addr", "string", "dlvry_email_cntct_addr", "string"),
    ("dlvry_channel", "string", "dlvry_channel", "string"),
    ("market_code", "string", "market_code", "string"),
    ("cmpgn_name", "string", "cmpgn_name", "string"),
    ("right_join2_cust_nm", "string", "cust_nm", "string"),
    ("trckng_url_type_open", "bigint", "trckng_url_type_open", "int"),
    ("cmpgn_brand", "string", "cmpgn_brand", "string"),
    ("cmpgn_end_ts", "string", "cmpgn_end_ts", "string"),
    ("trckng_url_type_email_click", "bigint", "trckng_url_type_email_click", "int"),
    ("dlvry_id", "string", "dlvry_id", "string"),
    ("cmpgn_strt_ts", "string", "cmpgn_strt_ts", "string"),
    ("cmpgn_type", "string", "cmpgn_type", "string"),
    ("dlvry_name", "string", "dlvry_name", "string"),
    ("cust_guid", "string", "cust_guid", "string")
]

mapped_frame = ApplyMapping.apply(
    frame=dropped_joined_frame_final,
    mappings=mapping,
    transformation_ctx="mapped_frame"
)

# Write operation
final_frame = glueContext.write_dynamic_frame.from_catalog(
    frame=mapped_frame, database="mea_prod_cdh_datalake", redshift_tmp_dir="s3://az-eu-meadatalake/temp-dir/",
    table_name="im_dw_mea_gdsdata_ads_mea_infobip_delivery", transformation_ctx="final_frame")

# Remember that you don't have to use '.from_catalog' to write your ddf/df. You can indeed use '.from_jdbc_conf' option.
# By using that, you can also use pre-actions and post-actions. In the example below, we use them to achieve UPSERT.
# Please pay attention to that!
post_query = "begin;delete from eg_gdsdata_ads.eg_dms_bdpharmacytype using eg_gdsdata_ads.stage_table_86fa1b0725944c93aca4dbb2aad0c207 where eg_gdsdata_ads.stage_table_86fa1b0725944c93aca4dbb2aad0c207.pharmacytypeid = eg_gdsdata_ads.eg_dms_bdpharmacytype.pharmacytypeid; insert into eg_gdsdata_ads.eg_dms_bdpharmacytype select * from eg_gdsdata_ads.stage_table_86fa1b0725944c93aca4dbb2aad0c207; drop table eg_gdsdata_ads.stage_table_86fa1b0725944c93aca4dbb2aad0c207; end;"
pre_query = "begin;drop table if exists eg_gdsdata_ads.stage_table_86fa1b0725944c93aca4dbb2aad0c207;create table eg_gdsdata_ads.stage_table_86fa1b0725944c93aca4dbb2aad0c207 as select * from eg_gdsdata_ads.eg_dms_bdpharmacytype where 1=2; end;"
RedshiftCluster_node3 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=ApplyMapping_node2,
                                                                       catalog_connection="mea-data-prod-cdh-connection-new",
                                                                       connection_options={"database": "im_dw",
                                                                                           "dbtable": "eg_gdsdata_ads.stage_table_86fa1b0725944c93aca4dbb2aad0c207",
                                                                                           "preactions": pre_query,
                                                                                           "postactions": post_query},
                                                                       redshift_tmp_dir="s3://az-eu-meadatalake/temp-dir/glue-temp-folder/",
                                                                       transformation_ctx="RedshiftCluster_node3")

job.commit()

# Remember that the below explanation and the above example might not be referring to the same tables!
# #####################################################################################################################

# In the SQL query 'select * from eg_gdsdata_ads.eg_dms_bdpharmacytype where 1=2', the condition 1=2 always evaluates to
# false. This means that the WHERE clause filters out all rows from the eg_dms_bdpharmacytype table, effectively
# selecting nothing.

# The purpose of using 1=2 in this context is usually to create an empty table with the same structure as another table
# (eg_dms_bdpharmacytype in this case) without actually copying any data. This can be useful in scenarios where you want
# to create a staging table or a temporary table with the same schema as an existing table, but you don't want to
# include any actual data initially (UPSERT!).

# #####################################################################################################################

# In SQL, the 'BEGIN' and 'END' keywords are used to define a block of statements that should be treated as a single unit
# of work. This construct is typically used in database systems to group multiple SQL statements into a transaction.

# In the code snippet you provided, the SQL query is quite complex and involves multiple statements: a DELETE statement,
# an INSERT statement, and a DROP TABLE statement.

# Wrapping these statements within a BEGIN and END block ensures that they are executed atomically as part of a single
# transaction. This means that either all the statements will be successfully executed, or none of them will be executed
# at all.
