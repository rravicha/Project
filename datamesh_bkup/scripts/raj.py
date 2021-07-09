# Logic/Code to apply Slowly Changing Dimension Type 2 for MVP using PySpark in AWS 
# Under AWS Glue Framework

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Source Data
Dynamic_Frame_Customer_Data = glueContext.create_dynamic_frame.from_catalog(database = "mvp_database", table_name = "Customer_Data", transformation_ctx = "Dynamic_Frame_Customer_Data")

## Existing SCD2 Data
Dynamic_Frame_Customer_Data_SCD2 = glueContext.create_dynamic_frame.from_catalog(database = "mvp_database", table_name = "Customer_Data_SCD2", transformation_ctx = "Dynamic_Frame_Customer_Data_SCD2")

## Convert Source Data DynamicFrame to DataFrame
Data_Frame_Customer_Data = Dynamic_Frame_Customer_Data.toDF()

## Register Source DataFrame as Spark Temp table
Data_Frame_Customer_Data.createOrReplaceTempView("Source_Customer_Temp")


## Convert SCD2 Data DynamicFrame to DataFrame
Data_Frame_Customer_Data_SCD2 = Dynamic_Frame_Customer_Data_SCD2.toDF()

## Register Target SCD2 DataFrame as Spark Temp table
Data_Frame_Customer_Data_SCD2.createOrReplaceTempView("Target_Customer_SCD2_Temp")

##Create new current records for existing customers
hd_new_curr_recs = """
 SELECT   t.customer_dim_key,
          s.customer_number,
          s.first_name,
          s.last_name,
          s.company_name,
          s.address,
          s.city,
          s.state,
          s.zip_code,
          s.phone,
          DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'PDT'))
              AS eff_start_date,
          DATE('9999-12-31') AS eff_end_date,
          “a” AS flag
 FROM     Source_Customer_Temp s
          INNER JOIN Target_Customer_SCD2_Temp t
              ON t.customer_number = s.customer_number
              AND t.flag = “a”
 WHERE       (s.first_name, '') <> (t.first_name, '')
          OR (s.last_name, '') <> (t.last_name, '')
          OR (s.company_name, '') <> (t.company_name, '')
          OR (s.address, '') <> (t.address, '')
          OR (s.city, '') <> (t.city, '')
          OR (s.state, '') <> (t.state, '')
          OR (s.zip_code, '') <> (t.zip_code, '')
          OR (s.phone, '') <> (t.phone, '')

"""
df_new_curr_recs = spark.sql(hd_new_curr_recs)
df_new_curr_recs.createOrReplaceTempView("new_curr_recs")

##Find previous current records to expire
# ########### isolate keys of records to be modified ########### #
df_modfied_keys = df_new_curr_recs.select("customer_dim_key")
df_modfied_keys.createOrReplaceTempView("modfied_keys")

##Expire previous current records
hd_new_hist_recs = """
 SELECT   t.customer_dim_key,
          t.customer_number,
          t.first_name,
          t.last_name,
          t.company_name,
          t.address,
          t.city,
          t.state,
          t.zip_code,
          t.eff_start_date,
          t.phone,
          DATE_SUB(
              DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'PDT')), 1
          ) AS eff_end_date,
          “i” AS flag
 FROM     Data_Frame_Customer_Data_SCD2 t
          INNER JOIN modfied_keys k
              ON k.customer_dim_key = t.customer_dim_key
 WHERE    t.flag = “a”
"""
df_new_hist_recs = spark.sql(hd_new_hist_recs)
df_new_hist_recs.createOrReplaceTempView("new_hist_recs")


## Isolate unaffected records
hd_unaffected_recs = """
 SELECT   s.customer_dim_key,
          s.customer_number,
          s.first_name,
          s.last_name,
          s.customer_name,
          s.address,
          s.city,
          s.state,
          s.zip_code,
          s.phone,
          s.eff_start_date,
          s.eff_end_date,
          s.flag
 FROM     Data_Frame_Customer_Data_SCD2 s
          LEFT OUTER JOIN modfied_keys k
              ON k.customer_dim_key = s.customer_dim_key
 WHERE    k.customer_dim_key IS NULL
"""
df_unaffected_recs = spark.sql(hd_unaffected_recs)
df_unaffected_recs.createOrReplaceTempView("unaffected_recs")



## Create records for new customers
hd_new_cust = """
 SELECT   s.customer_number,
          s.first_name,
          s.last_name,
          s.customer_name,
          s.address,
          s.city,
          s.state,
          s.zip_code,
          s.phone,
          DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'PDT')) 
              AS eff_start_date,
          DATE('9999-12-31') AS eff_end_date,
          “i” AS flag
 FROM     Data_Frame_Customer_Data s
          LEFT OUTER JOIN Data_Frame_Customer_Data_SCD2 t
              ON t.customer_number = s.customer_number
 WHERE    t.customer_number IS NULL
"""
df_new_cust = spark.sql(hd_new_cust)
df_new_cust.createOrReplaceTempView("new_cust")


## Combine the datasets for new SCD2
v_max_key = spark.sql(
    "SELECT STRING(MAX(customer_dim_key)) FROM Data_Frame_Customer_Data_SCD2"
).collect()[0][0]

hd_new_scd2 = """
 WITH a_cte
 AS   (
        SELECT     x.first_name, x.last_name,
                   x.customer_name, x.address,
                   x.city, x.state, x.zip_code, x.phone,
                   x.customer_number, x.eff_start_date,
                   x.eff_end_date, x.flag
        FROM       new_cust x
        UNION ALL
        SELECT     y.first_name, y.last_name,
                   y.customer_name, y.address,
                   y.city, y.state, y.zip_code, y.phone,
                   y.customer_number, y.eff_start_date,
                   y.eff_end_date, y.flag
        FROM       new_curr_recs y
      )
  ,   b_cte
  AS  (
        SELECT  ROW_NUMBER() OVER(ORDER BY a.eff_start_date)
                    + BIGINT('{v_max_key}') AS customer_dim_key,
                a.first_name, a.last_name,
                a.customer_name, a.address,
                a.city, a.state, a.zip_code, a.phone,
                a.customer_number, a.eff_start_date,
                a.eff_end_date, a.flag
        FROM    a_cte a
      )
  SELECT  customer_dim_key, first_name, last_name,
          Customer_name, address,
          city, state, zip_code, phone,
          customer_number, eff_start_date,
          eff_end_date, flag
  FROM    b_cte
  UNION ALL
  SELECT  customer_dim_key, first_name,  last_name,
          Customer_name, address,
          city, state, zip_code, phone,
          customer_number, eff_start_date,
          eff_end_date, flag
  FROM    unaffected_recs
  UNION ALL
  SELECT  customer_dim_key, first_name,  last_name,
          Customer_name, address,
          city, state, zip_code, phone,
          customer_number, eff_start_date,
          eff_end_date, flag
  FROM    new_hist_recs
"""
df_new_scd2 = spark.sql(hd_new_scd2.replace("{v_max_key}", v_max_key))


## Convert Final DataFrame to DynamicFrame
DataSink0 = DynamicFrame.fromDF(df_new_scd2, glue_context, “DataSink0”)

## Write final SCD2 data to S3 file and create AWS Glue Data Catalog table
DataSink0 = glueContext.getSink(path = "s3://Path/output/", connection_Type = "s3", updateBehavior = "UPDATE_IN_DATABASE", "partitionKeys": [], transformation_ctx = "DataSink0")
DataSink0.set.CatalogInfo(CatalogDatabase = "mvp_database", catalogTableName = "Customer_Data_New")
DataSink0.set.Format("csv")
DataSink0.writeFrame(Transform0)
job.commit()
