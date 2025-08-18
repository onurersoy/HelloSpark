import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import current_timestamp, col, lit, broadcast, when
from pyspark.sql.types import StringType
from datetime import datetime


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# im_rpt.pub_glbl.rpt_website_visits_updated_vw:
AmazonRedshiftSource_primary_table = glueContext.create_dynamic_frame.from_catalog(database="mea_prod_cdh_datalake",
                                                                                   table_name="im_rpt_pub_glbl_rpt_website_visits_updated_vw",
                                                                                   redshift_tmp_dir="s3://aws-glue-assets-592549787542-eu-west-1/temporary/",
                                                                                   transformation_ctx="AmazonRedshiftSource_sub_type_az__c_vw")

selected_fields_AmazonRedshiftSource_primary_table = SelectFields.apply(
    frame=AmazonRedshiftSource_primary_table,
    paths=["page_addr", "page_nm", "cust_hit_tm", "hit_tm", "visid_tm", "cust_visid", "visid_high", "visid_low", "core_pltfm_id", "core_pltfm_typ", "core_ast_typ", "src_lang", "src_cntry", "src_mkt", "mkt_sk", "mrkt_cd", "core_tgt_audnce", "src_brnd", "brnd_sk", "core_trpy_area", "acty_typ", "site_sect", "site_sub_sect", "evnt_nm", "evnt_val", "cmpgn_cnm", "cmpgn_cmedm", "cmpcgn_cadpub", "cmpgn_cplace", "vst_unq_id", "vst_id", "session_id", "post_ref", "browser_desc", "os_desc", "duration", "filename", "src_sys_id", "rec_ownr_comp_id", "last_updt_ts", "domain_source", "src_core_trpy_area", "geo_cntry", "geo_regn", "geo_city", "org_code", "can_scroll", "page_info", "asset_id", "full_referrer_url", "full_page_url", "ref_type"
           ],
    transformation_ctx="selected_fields_AmazonRedshiftSource_primary_table")

df1 = selected_fields_AmazonRedshiftSource_primary_table.toDF()
default_value = ""
df1 = df1.withColumn("cust_visid", col("cust_visid").cast("string"))
df1 = df1.fillna(default_value)
#df1 = df1.fillna({"cust_visid": default_value})
df1.printSchema()


df1 = df1.filter(
    ~((col("domain_source") == "zscaler.com") | (col("page_nm").like("%404%")))
)
df1 = df1.filter(~(col("page_addr") == "https://www.azpaymentdisclosureexternal.com/tr"))

df1 = df1.withColumn("insert_timestamp", current_timestamp())

# No more need for 'if mrkt_cd is null' control >>
# df1 = df1.withColumn(
# "mrkt_cd",
# when(col("mrkt_cd").isNull(),
# when(col("page_addr").like("https://www.azedugate.com/en-bh%"), "BH")
# .when(col("page_addr").like("https://www.azedugate.com/en-kw%"), "KW")
# .when(col("page_addr").like("https://www.azedugate.com/en-om%"), "OM")
# .when(col("page_addr").like("https://www.azedugate.com/en-qa%"), "SA")
# .when(col("page_addr").like("https://www.azedugate.com/en-sa%"), "ZA")
# .when(col("page_addr").like("https://www.azedugate.com/en-uae%"), "AE")
# .otherwise(col("mrkt_cd"))
# )
# .otherwise(col("mrkt_cd"))
# )

df1 = df1.withColumn(
    "mrkt_cd",
    when(col("page_addr").like("https://www.azedugate.com/en-bh%"), "BH")
    .when(col("page_addr").like("https://www.azedugate.com/en-kw%"), "KW")
    .when(col("page_addr").like("https://www.azedugate.com/en-om%"), "OM")
    .when(col("page_addr").like("https://www.azedugate.com/en-qa%"), "QA")
    .when(col("page_addr").like("https://www.azedugate.com/en-sa%"), "SA")
    .when(col("page_addr").like("https://www.azedugate.com/en-uae%"), "AE")
    .otherwise(col("mrkt_cd"))
)

df1 = df1.withColumn(
    "mrkt_cd",
    when(
        (col("page_nm").like("%AZ-Engage%")) & (col("geo_cntry") == "dza"), "DZ"
    ).when(
        (col("page_nm").like("%AZ-Engage%")) & (col("geo_cntry") == "mar"), "MA"
    ).when(
        (col("page_nm").like("%AZ-Engage%")) & (col("geo_cntry") == "tun"), "TN"
    ).when(
        (col("page_nm").like("%AZ-Engage%")) & (col("geo_cntry") == "jor"), "JO"
    ).when(
        (col("page_nm").like("%AZ-Engage%")) & (col("geo_cntry") == "lbn"), "LB"
    ).when(
        (col("page_nm").like("%AZ-Engage%")) & (col("geo_cntry") == "pse"), "PS"
    ).when(
        (col("page_nm").like("%AZ-Engage%")) & (col("geo_cntry") == "irq"), "IQ"
    ).when(
        (col("page_nm").like("%AZ-Engage%")) & (col("geo_cntry") == "lby"), "LY"
    ).when(
        (col("page_nm").like("%AZ-Engage%")) & (col("geo_cntry") == "syr"), "SY"
    ).when(
        (col("page_nm").like("%AZ-Engage%")) & (col("geo_cntry") == "irn"), "IR"
    ).otherwise(col("mrkt_cd"))
)

df1 = df1.withColumn(
    "mrkt_cd",
    when(
        (col("page_addr").like("%turbuhaler-device.ae%")) | (col("page_addr").like('%healthgcc%')), "AE"
    ).otherwise(col("mrkt_cd"))
)
df1.printSchema()

df1 = df1.withColumn("mrkt_cd", when(col("mrkt_cd").isNull(), col("src_cntry")).otherwise(col("mrkt_cd")))
df1.printSchema()

# mea_gdsdata_ads.mea_website_visits_country_codes_lookup:
# im_dw_mea_gdsdata_ads_mea_website_visits_country_codes_lookup_node = glueContext.create_dynamic_frame.from_catalog(
#     database="mea_prod_cdh_datalake", redshift_tmp_dir="s3://az-eu-meadatalake/temp-dir/",
#     table_name="im_dw_mea_gdsdata_ads_mea_website_visits_country_codes_lookup",
#     transformation_ctx="im_dw_mea_gdsdata_ads_mea_website_visits_country_codes_lookup_node")
#
# selected_fields_node1 = SelectFields.apply(
#     frame=im_dw_mea_gdsdata_ads_mea_website_visits_country_codes_lookup_node,
#     paths=["geo_country_code", "country_name"],
#     transformation_ctx="selected_fields_node1")
#
# rename_selected_fields_node1 = ApplyMapping.apply(
#     frame=selected_fields_node1, mappings=[
#         ("geo_country_code", "join1_geo_country_code"),
#         ("country_name", "join1_country_name")],
#     transformation_ctx="rename_selected_fields_node1")
#
# # mea_gdsdata_ads.mea_country_codes_lookup:
# im_dw_mea_gdsdata_ads_mea_country_codes_lookup_node = glueContext.create_dynamic_frame.from_catalog(
#     database="mea_prod_cdh_datalake", redshift_tmp_dir="s3://az-eu-meadatalake/temp-dir/",
#     table_name="im_dw_mea_gdsdata_ads_mea_country_codes_lookup",
#     transformation_ctx="im_dw_mea_gdsdata_ads_mea_country_codes_lookup_node")
#
# selected_fields_node2 = SelectFields.apply(
#     frame=im_dw_mea_gdsdata_ads_mea_country_codes_lookup_node,
#     paths=["country_code", "country_name"],
#     transformation_ctx="selected_fields_node2")
#
# rename_selected_fields_node2 = ApplyMapping.apply(
#     frame=selected_fields_node2, mappings=[
#         ("country_code", "join2_country_code"),
#         ("country_name", "join2_country_name")],
#     transformation_ctx="rename_selected_fields_node2")
#
# df2 = broadcast(rename_selected_fields_node1.toDF())
# df3 = broadcast(rename_selected_fields_node2.toDF())
# joined_df = df1.join(df2,df1.geo_cntry ==  df2.join1_geo_country_code,"left")
# joined_df = joined_df.join(df3,joined_df.join1_country_name ==  df3.join2_country_name,"left")

# joined_df = df1.withColumn(
#     "mrkt_cd",
#        when(col("page_addr").like("%az-engage.com%") & col("geo_cntry").isNotNull(), col("join2_country_code"))
#         .otherwise(col("mrkt_cd"))
# )
# joined_df.printSchema()

# joined_frame_final = joined_df.drop("join1_geo_country_code", "join1_country_name", "join2_country_code", "join2_country_name")
joined_frame_final = DynamicFrame.fromDF(df1, glueContext, "joined_frame_final")
joined_frame_final.printSchema()

mapping = [
    ("page_addr", "page_addr"),
    ("page_nm", "page_name"),
    ("cust_hit_tm", "cust_hit_tm"),
    ("hit_tm", "hit_tm"),
    ("visid_tm", "visid_tm"),
    ("cust_visid", "cust_visid"),
    ("visid_high", "visid_high"),
    ("visid_low", "visid_low"),
    ("core_pltfm_id", "core_platform_id"),
    ("core_pltfm_typ", "core_platform_type"),
    ("core_ast_typ", "core_ast_type"),
    ("src_lang", "source_lang"),
    ("src_cntry", "source_country"),
    ("src_mkt", "source_market"),
    ("mkt_sk", "market_sk"),
    ("mrkt_cd", "market_code"),
    ("core_tgt_audnce", "core_tgt_audience"),
    ("src_brnd", "source_brand"),
    ("brnd_sk", "brand_sk"),
    ("core_trpy_area", "core_trpy_area"),
    ("acty_typ", "activity_type"),
    ("site_sect", "site_section"),
    ("site_sub_sect", "site_sub_section"),
    ("evnt_nm", "event_name"),
    ("evnt_val", "event_value"),
    ("cmpgn_cnm", "campaign_cnm"),
    ("cmpgn_cmedm", "campaign_cmedm"),
    ("cmpgn_cadpub", "campaign_cadpub"),
    ("cmpgn_cplace", "campaign_cplace"),
    ("vst_unq_id", "visit_unique_id"),
    ("vst_id", "visit_id"),
    ("session_id", "session_id"),
    ("post_ref", "post_ref"),
    ("browser_desc", "browser_desc"),
    ("os_desc", "os_desc"),
    ("duration", "duration"),
    ("filename", "file_name"),
    ("src_sys_id", "source_system_id"),
    ("rec_ownr_comp_id", "rec_owner_comp_id"),
    ("last_updt_ts", "last_uptade_ts"),
    ("domain_source", "domain_source"),
    ("src_core_trpy_area", "source_core_trpy_area"),
    ("geo_cntry", "geo_country"),
    ("geo_regn", "geo_region"),
    ("geo_city", "geo_city"),
    ("org_code", "org_code"),
    ("can_scroll", "can_scroll"),
    ("page_info", "page_info"),
    ("asset_id", "asset_id"),
    ("full_referrer_url", "full_referrer_url"),
    ("full_page_url", "full_page_url"),
    ("insert_timestamp", "insert_timestamp")
]

mapped_frame = ApplyMapping.apply(
    frame=joined_frame_final,
    mappings=mapping,
    transformation_ctx="mapped_frame"
)

#Dataload= DropNullFields.apply(frame = mapped_frame, transformation_ctx = "Dataload")

pre_query = "TRUNCATE TABLE mea_gdsdata_ads.mea_rpt_website_visits_updated;"
#post_query = []
# mea_gdsdata_ads.mea_rpt_website_visits_updated:
RedshiftCluster_node_target = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=mapped_frame,
    catalog_connection="mea-data-prod-cdh-connection-new",
    connection_options={"database": "im_dw",
                        "dbtable": "mea_gdsdata_ads.mea_rpt_website_visits_updated",
                        "preactions": pre_query#,
                        #"postactions": post_query
                        },
    redshift_tmp_dir="s3://az-eu-meadatalake/temp-dir/",
    transformation_ctx="RedshiftCluster_node_target")

job.commit()
