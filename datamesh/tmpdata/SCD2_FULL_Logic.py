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

## Source Customer Data (Input *.csv file which we have created catalog table) 
Dynamic_Frame_Customer_Data = glueContext.create_dynamic_frame.from_catalog(database = "mvp_database", table_name = "Customer_Data", transformation_ctx = "Dynamic_Frame_Customer_Data")

## Existing Customer.................r_Data_SCD2 = glueContext.create_dynamic_frame.from_catalog(database = "mvp_database", table_name = "Customer_Data_SCD2", transformation_ctx = "Dynamic_Frame_Customer_Data_SCD2")

## Convert Source Customer Data DynamicFrame to DataFrame
Data_Frame_Customer_Data = Dynamic_Frame_Customer_Data.toDF()

## Register Source DataFrame as Spark Temp table
Data_Frame_Customer_Data.createOrReplaceTempView("table_source_customer")

## Convert Customer SCD2 Data DynamicFrame to DataFrame
Data_Frame_Customer_Data_SCD2 = Dynamic_Frame_Customer_Data_SCD2.toDF()
'''''''
## Register Target SCD2 DataFrame as Spark Temp table
Data_Frame_Customer_Data_SCD2.createOrReplaceTempView("table_customer_SCD2")

hd_new_curr_recs = """
 SELECT   t.customer_dim_key,
          s.customer_number,
          s.first_name,
          s.last_name,
          s.middle_initial,
          s.address,-v 
          s.city,
          s.state,
          s.zip_code,
          DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'PDT'))
              AS eff_start_date,
          DATE('9999-12-31') AS eff_end_date,
          BOOLEAN(1) AS is_current
 FROM     customer_data s
          INNER JOIN current_scd2 t
              ON t.customer_number = s.customer_number
              AND t.is_current = True
 WHERE    NVL(s.first_name, '') <> NVL(t.first_name, '')
          OR NVL(s.last_name, '') <> NVL(t.last_name, '')
          OR NVL(s.middle_initial, '') <> NVL(t.middle_initial, '')
          OR NVL(s.address, '') <> NVL(t.address, '')
          OR NVL(s.city, '') <> NVL(t.city, '')
          OR NVL(s.state, '') <> NVL(t.state, '')
          OR NVL(s.zip_code, '') <> NVL(t.zip_code, '')
"""

df_new_curr_recs = spark.sql(hd_new_curr_recs)

df_new_curr_recs.createOrReplaceTempView("new_curr_recs")

# ########### isolate keys of records to be modified ########### #

df_modfied_keys = df_new_curr_recs.select("customer_dim_key")
df_modfied_keys.createOrReplaceTempView("modfied_keys")

# ############## create new hist recs dataaset ############## #
hd_new_hist_recs = """
 SELECT   t.customer_dim_key,
          t.customer_number,
          t.first_name,
          t.last_name,
          t.middle_initial,
          t.address,
          t.city,
          t.state,
          t.zip_code,
          t.eff_start_date,
          DATE_SUB(
              DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'PDT')), 1
          ) AS eff_end_date,
          BOOLEAN(0) AS is_current
 FROM     current_scd2 t
          INNER JOIN modfied_keys k
              ON k.customer_dim_key = t.customer_dim_key
 WHERE    t.is_current = True
"""

df_new_hist_recs = spark.sql(hd_new_hist_recs)
df_new_hist_recs.createOrReplaceTempView("new_hist_recs")

## Find records in SCD2 but not in source. (Means deleted in source)
hd_old_cust = """
 SELECT   t.customer_dim_key
 FROM     current_scd2 t
          LEFT OUTER JOIN customer_data s
              ON t.customer_number = s.customer_number
 WHERE    s.customer_number IS NULL
          AND t.is_current = True
"""

df_old_key = spark.sql(hd_old_cust)

df_old_key.createOrReplaceTempView("old_keys")

## Expire current active records, which got deleted in source.
hd_old_hist_recs = """
 SELECT   t.customer_dim_key,
          t.customer_number,
          t.first_name,
          t.last_name,
          t.middle_initial,
          t.address,
          t.city,
          t.state,
          t.zip_code,
          t.eff_start_date,
          DATE_SUB(
              DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'PDT')), 1
          ) AS eff_end_date,
          BOOLEAN(0) AS is_current
 FROM     current_scd2 t
          INNER JOIN old_keys k
              ON k.customer_dim_key = t.customer_dim_key
 WHERE    t.is_current = True
"""

df_old_hist_recs = spark.sql(hd_old_hist_recs)

df_old_hist_recs.createOrReplaceTempView("old_hist_recs")

------------------------------------------------------------------------------------------------
hd_merge_keys = """
  SELECT  customer_dim_key
  FROM    modfied_keys
  UNION ALL
  SELECT  customer_dim_key
  FROM    old_keys
"""	

df_merge_keys = spark.sql(hd_merge_keys)

df_merge_keys.createOrReplaceTempView("merge_keys")

-------------------------------------------------------------------------------------
hd_unaffected_recs = """
 SELECT   s.customer_dim_key,
          s.customer_number,
          s.first_name,
          s.last_name,
          s.middle_initial,
          s.address,
          s.city,
          s.state,
          s.zip_code,
          s.eff_start_date,
          s.eff_end_date,
          s.is_current
 FROM     current_scd2 s
          LEFT OUTER JOIN merge_keys k
              ON k.customer_dim_key = s.customer_dim_key
 WHERE    k.customer_dim_key IS  NULL
"""

df_unaffected_recs = spark.sql(hd_unaffected_recs)

df_unaffected_recs.createOrReplaceTempView("unaffected_recs")

# ############## create new recs dataset ############## #
hd_new_cust = """
 SELECT   s.customer_number,
          s.first_name,
          s.last_name,
          s.middle_initial,
          s.address,
          s.city,
          s.state,
          s.zip_code,
          DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'PDT')) 
              AS eff_start_date,
          DATE('9999-12-31') AS eff_end_date,
          BOOLEAN(1) AS is_current
 FROM     customer_data s
          LEFT OUTER JOIN current_scd2 t
              ON t.customer_number = s.customer_number
 WHERE    t.customer_number IS NULL
"""

df_new_cust = spark.sql(hd_new_cust)
df_new_cust.createOrReplaceTempView("new_cust")


v_max_key = spark.sql(
    "SELECT STRING(MAX(customer_dim_key)) FROM current_scd2"
).collect()[0][0]


hd_new_scd2 = """
 WITH a_cte
 AS   (
        SELECT     x.first_name, x.last_name,
                   x.middle_initial, x.address,
                   x.city, x.state, x.zip_code,
                   x.customer_number, x.eff_start_date,
                   x.eff_end_date, x.is_current
        FROM       new_cust x
        UNION ALL
        SELECT     y.first_name, y.last_name,
                   y.middle_initial, y.address,
                   y.city, y.state, y.zip_code,
                   y.customer_number, y.eff_start_date,
                   y.eff_end_date, y.is_current
        FROM       new_curr_recs y
      )
  ,   b_cte
  AS  (
        SELECT  ROW_NUMBER() OVER(ORDER BY a.eff_start_date)
                    + BIGINT('{v_max_key}') AS customer_dim_key,
                a.first_name, a.last_name,
                a.middle_initial, a.address,
                a.city, a.state, a.zip_code,
                a.customer_number, a.eff_start_date,
                a.eff_end_date, a.is_current
        FROM    a_cte a
      )
  SELECT  customer_dim_key, first_name, last_name,
          middle_initial, address,
          city, state, zip_code,
          customer_number, eff_start_date,
          eff_end_date, is_current
  FROM    b_cte
  UNION ALL
  SELECT  customer_dim_key, first_name,  last_name,
          middle_initial, address,
          city, state, zip_code,
          customer_number, eff_start_date,
          eff_end_date, is_current
  FROM    unaffected_recs
  UNION ALL
  SELECT  customer_dim_key, first_name,  last_name,
          middle_initial, address,
          city, state, zip_code,
          customer_number, eff_start_date,
          eff_end_date, is_current
  FROM    new_hist_recs
  UNION ALL
  SELECT  customer_dim_key, first_name,  last_name,
          middle_initial, address,
          city, state, zip_code,
          customer_number, eff_start_date,
          eff_end_date, is_current
  FROM    old_hist_recs
"""

df_new_scd2 = spark.sql(hd_new_scd2.replace("{v_max_key}", v_max_key))

## Convert Final DataFrame to DynamicFrame
Transform0 = DynamicFrame.fromDF(df_new_scd2, glueContext, “Transform0”)

## Convert Final DynamicFrame to have only ONE file
Transform0_c = Transform0.coalesce(1)

## Write final SCD2 data to S3 file and create AWS Glue Data Catalog table
DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0_c, connection_Type = "s3", connection_options = {"path" = "s3://Bucket_Name/Folder"}, format = "parquet", transformation_ctx = "DataSink0")
job.commit()