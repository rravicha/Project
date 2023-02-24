
# from __future__ import print_function
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
#import org.apache.spark.sql.functions.monotonicallyIncreasingId
# from pyspark import SparkContext
from pyspark import SparkConf, SparkContext ,SQLContext,Row,HiveContext
import pyspark.sql.functions as func
from pyspark.sql.functions import lit
from datetime import datetime
from pyspark.sql.functions import col
from functools import reduce
from pyspark.sql import DataFrame
#import org.apache.spark.sql.expressions._
from pyspark import HiveContext
from pyspark.sql.types import *
from pyspark.sql import Row, functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast
from datetime import datetime
from datetime import timedelta
import hashlib
import sys

if __name__ == "__main__":
    if len(sys.argv) < 6 :
        print("Input Parameter Missing ", file=sys.stderr)
        exit(-1)
sc = SparkContext(appName="SCD"+sys.argv[3])
sc.setLogLevel("ERROR")
sqlContext=HiveContext(sc)
tgt_schema = sys.argv[1] # kmc
tgt_tbl_nm = sys.argv[2] # account
src_schema = sys.argv[1] # kmc
src_tbl_nm = sys.argv[3] # src_account
load_dt = sys.argv[4]    # '2016-11-08'
hist_delta = sys.argv[5] # hist
src_schema_tbl = src_schema+'.'+src_tbl_nm    # kmc.src_account
tgt_schema_tbl = tgt_schema+'.'+tgt_tbl_nm    # kmc.account 
tgt_schema_stg_tbl = tgt_schema+'.'+tgt_tbl_nm + '_tgt' # kmc.account_tgt (3)
tgt_schema_scd2_tbl = tgt_schema+'.'+tgt_tbl_nm + '_scd2' # kmc.account_scd2 [(10-1) + 3]

sqlContext.setConf("hive.exec.dynamic.partition ","true")
sqlContext.setConf("hive.exec.dynamic.partition.mode","true")
sqlContext.setConf("hive.execution.engine","spark")
sqlContext.setConf("hive.vectorized.execution.enabled","true")
sqlContext.setConf("hive.vectorized.execution.reduce.enabled","true")
sqlContext.setConf("spark.sql.broadcastTimeout","36000")
#sc.setConf("spark.sql.broadcastTimeout","36000s")

############################ Columns in Delta & Hist Table ##################################
delta_columns = ("delta_acct_nbr", "delta_account_sk_id", "delta_zip_code", "delta_primary_state", "delta_eff_start_date", "delta_eff_end_date", "delta_load_tm", "delta_hash_key", "delta_eff_flag") # Raj changed from "hash_key" to "delta_hash_key"

hist_columns = ("acct_nbr", "account_sk_id", "zip_code", "primary_state", "eff_start_date", "eff_end_date", "load_tm", "hash_key", "eff_flag")

############################ Data Preparation ##################################

############### Global Varibales used ##########################
eff_close_dt = "3100-12-31"
eff_flag_curr = "Y"
eff_flag_non_curr = "N"
eff_start_date_hist = "Today"
eff_start_date_delta = "Tomorrow" ## 1 day lead
hash_udf = func.udf(lambda x: hashlib.sha256(str(x).encode('utf-8')).hexdigest().upper()) # Raghav added encode('utf-8')
dt=datetime.now()
load_tm=dt.strftime('%Y-%m-%d %H:%M:%S')
############################ Columns in Delta & Hist Table ##################################
##ragz


##ragz
if (hist_delta == 'hist'):
    # Print Input values
    print ("First Argument = ", sys.argv[1])  # kmc
    print ("Second Argument = ", sys.argv[2]) # account
    print ("Third Argument = ", sys.argv[3])  # src_account
    print ("Fourth Argument = ", sys.argv[4]) # '2016-11-08'
    print ("Fifth Argument = ", sys.argv[5])  # hist
    print ("Source Table Name = ", src_schema_tbl)  # kmc.src_account
    print ("Target Table Name = ", tgt_schema_tbl)  # kmc.account
    print ("Target Staging Table Name = ", tgt_schema_stg_tbl)  # kmc.account_tgt
    # ragz
    #sqlContext.sql("DROP TABLE IF EXISTS %s" %(src_schema_tbl))
    #sqlContext.sql("DROP TABLE IF EXISTS %s" %(tgt_schema_tbl))
    #sqlContext.sql("DROP TABLE IF EXISTS %s" %(tgt_schema_stg_tbl))
    #sqlContext.sql("DROP DATABASE IF EXISTS %s" %(tgt_schema))
    #sqlContext.sql("DROP DATABASE %s CASCADE" %(tgt_schema))
    #sqlContext.sql("create database %s "%(tgt_schema))
    #sqlContext.sql("use %s" %(tgt_schema))
    #sqlContext.sql("create table src_account (acct_nbr bigint,primary_state varchar(10),zip_code varchar(10)) partitioned by (load_date varchar(12)) stored as parquet")
    #sqlContext.sql("create table account(acct_nbr bigint,account_sk_id bigint,primary_state varchar(10),zip_code varchar(10),eff_start_date varchar(12),eff_end_date varchar(12),load_tm varchar(30),hash_key string,eff_flag varchar(2)) stored as parquet")
    #sqlContext.sql("create table account_tgt (acct_nbr bigint,account_sk_id bigint,zip_code varchar(10),primary_state varchar(10),eff_start_date varchar(10),eff_end_date varchar(10),load_tm varchar(30),hash_key string,eff_flag varchar(2),upd_ins_flag varchar(1))stored as parquet")
    #sqlContext.sql("insert into table src_account PARTITION (load_date='2016-11-08') values ('1','TN','TN10001')");
    #sqlContext.sql("insert into table src_account PARTITION (load_date='2016-11-08') values ('2','TN','TN10002')");
    #sqlContext.sql("insert into table src_account PARTITION (load_date='2016-11-08') values ('3','TN','TN10003')");
    # ragz
    print('*'*25);print("Entering hist load process");print('*'*25);
    
    hist_tbl_df = sqlContext.sql("select * from %s where load_date='%s'" %(src_schema_tbl,load_dt)).cache()
    print("hist_tbl_df - Before SORT")
    hist_tbl_df.show()
    #dbconn=sqlContext.sql("use kmc")
    #hist_tbl_df = sqlContext.sql("select * from %s" %(src_schema_tbl)).cache()
    hist_tbl_df = hist_tbl_df.sort("acct_nbr") # Added by Raj
    print("hist_tbl_df - After SORT")
    hist_tbl_df.show()
    hist_tbl_lkp_df = hist_tbl_df.select("acct_nbr").withColumn("account_sk_id", F.row_number().over(Window.partitionBy(lit(1)).orderBy(lit(1)))) 
    print("hist_tbl_lkp_df")
    hist_tbl_lkp_df.show()
    hist_sk_df = hist_tbl_df.join(broadcast(hist_tbl_lkp_df) ,'acct_nbr','inner' ).withColumn("eff_start_date",lit(load_dt)).withColumn("hash_key",hash_udf(hist_tbl_df.zip_code)).withColumn("eff_end_date",lit(eff_close_dt)).withColumn("load_tm",lit(load_tm)).withColumn("eff_flag",lit(eff_flag_curr))
    print("hist_sk_df")
    hist_sk_df.show()
    hist_sk_df_ld = hist_sk_df.select(*hist_columns) # without load_date
    print("hist_sk_df_ld")
    hist_sk_df_ld.show()
    hist_sk_df_ld.agg({"account_sk_id": "max"}).show()
    hist_sk_df_ld.write.mode("overwrite").saveAsTable(tgt_schema_tbl)
    print('*'*25);print("End of hist load process");print('*'*25);
else:
    print('*'*25);print("Entering delta load process");print('*'*25);
    sqlContext.sql("use %s"%(tgt_schema)) # ragz
    #To test add logic
    #sqlContext.sql("insert into table src_account PARTITION (load_date='2016-11-08') values ('4','TN','TN10004')")
    #To test modifiy logic
    #sqlContext.sql("truncate table src_account")
    #sqlContext.sql("insert into table src_account PARTITION (load_date='2016-11-08') values ('1','TN','TN10001NEW')");
    hist_tgt_tbl_df = sqlContext.sql("select * from %s" %(tgt_schema_tbl)).cache()
    print("hist_tgt_tbl_df")
    hist_tgt_tbl_df.show()
    hist_tgt_tbl_df.printSchema()
    hist_tgt_tbl_N_df = hist_tgt_tbl_df.where(hist_tgt_tbl_df['eff_flag'] == 'N')
    print("hist_tgt_tbl_N_df")
    hist_tgt_tbl_N_df.show()
    hist_tgt_tbl_N_df.printSchema()
    hist_tgt_tbl_Y_df = hist_tgt_tbl_df.where(hist_tgt_tbl_df['eff_flag'] == 'Y').cache()
    print("hist_tgt_tbl_Y_df")
    hist_tgt_tbl_Y_df.show()
    hist_tgt_tbl_Y_df.printSchema()
    max_sk_id_rdd_list = hist_tgt_tbl_df.agg({"account_sk_id": "max"}).rdd.map(list)    
    for line in max_sk_id_rdd_list.collect():
        max_account_sk_id=line[0]
        print ("Max_sk_id :",max_account_sk_id)
    delta_src_tbl_df = sqlContext.sql("select * from %s where load_date='%s'" %(src_schema_tbl,load_dt)).cache()
    print("delta_src_tbl_df")
    delta_src_tbl_df.show()
    delta_src_tbl_df.printSchema()
    delta_src_rename_df = delta_src_tbl_df.withColumnRenamed("acct_nbr", "delta_acct_nbr").withColumnRenamed("zip_code", "delta_zip_code").withColumnRenamed("primary_state", "delta_primary_state").withColumn("delta_hash_key",hash_udf('delta_zip_code')).withColumnRenamed("load_date", "delta_eff_start_date")
    print("delta_src_rename_df")
    delta_src_rename_df.show()
    delta_src_rename_df.printSchema()
    cdc_new_acct_df = delta_src_rename_df.join(hist_tgt_tbl_Y_df ,(delta_src_rename_df.delta_acct_nbr == hist_tgt_tbl_Y_df.acct_nbr) ,'left_outer' ).where(hist_tgt_tbl_Y_df['acct_nbr'].isNull()) # Changed isNotNull() to isNull() Raj
    print("cdc_new_acct_df")
    cdc_new_acct_df.show()
    cdc_new_acct_df.printSchema()
    print ("Number of Brand new records have come in = ",cdc_new_acct_df.count())  
    cdc_change_acct_df = delta_src_rename_df.join(hist_tgt_tbl_Y_df,(delta_src_rename_df.delta_acct_nbr == hist_tgt_tbl_Y_df.acct_nbr) ,'left_outer' ).where(hist_tgt_tbl_Y_df['hash_key'] != delta_src_rename_df['delta_hash_key'])
    print("cdc_change_acct_df")
    cdc_change_acct_df.show()
    cdc_change_acct_df.printSchema()
    print ("Number of Modify records have come in = ",cdc_change_acct_df.count())  
    # Raj added start Delete records    
    #cdc_delete_acct_df = delta_src_rename_df.join(hist_tgt_tbl_Y_df,(delta_src_rename_df.delta_acct_nbr == hist_tgt_tbl_Y_df.acct_nbr) ,'right_outer' ).where(delta_src_rename_df['delta_acct_nbr'].isNull())
    cdc_delete_acct_df = hist_tgt_tbl_Y_df.join(delta_src_rename_df,(delta_src_rename_df.delta_acct_nbr == hist_tgt_tbl_Y_df.acct_nbr) ,'left_outer' ).where(delta_src_rename_df['delta_acct_nbr'].isNull())
    print("cdc_delete_acct_df")
    cdc_delete_acct_df.show()
    cdc_delete_acct_df.printSchema()
    print ("Number of delete records have come in = ",cdc_delete_acct_df.count())
    # Raj added End Delete records
    # Raj added start change + delete records
    cdc_change_acct_1_df = cdc_change_acct_df.select(*hist_columns)
    cdc_delete_acct_1_df = cdc_delete_acct_df.select(*hist_columns)
    cdc_change_delete_df = cdc_change_acct_1_df.unionAll(cdc_delete_acct_1_df)
    print("cdc_change_delete_df")
    cdc_change_delete_df.show()
    cdc_change_delete_df.printSchema()
    # Raj added end change + delete records
    # Raj added start No Change record
    hist_tgt_tbl_Y_no_change_df = hist_tgt_tbl_Y_df.join(cdc_change_delete_df, hist_tgt_tbl_Y_df.acct_nbr == cdc_change_delete_df.acct_nbr ,"leftanti")
    print("hist_tgt_tbl_Y_no_change_df")
    hist_tgt_tbl_Y_no_change_df.show()
    hist_tgt_tbl_Y_no_change_df.printSchema()
    hist_tgt_tbl_Y_no_change_df_ld = hist_tgt_tbl_Y_no_change_df.select(*hist_columns).withColumn("ins_upd_flag",lit('Y'))
    print("hist_tgt_tbl_Y_no_change_df_ld")
    hist_tgt_tbl_Y_no_change_df_ld.show()
    hist_tgt_tbl_Y_no_change_df_ld.printSchema()
    # Raj added end No Change record
    cdc_all_record_df = cdc_new_acct_df.unionAll(cdc_change_acct_df)
    print("cdc_all_record_df")
    cdc_all_record_df.show()
    cdc_all_record_df.printSchema()
    print ("Total Number of Brand New + Modify records have come in = ",cdc_all_record_df.count()) 
    print ("Max account_sk_id before delta load = ",max_account_sk_id)
    delta_sk_lkp_df = cdc_all_record_df.select("delta_acct_nbr").withColumn("delta_account_sk_id", F.row_number().over(Window.partitionBy(lit(1)).orderBy(lit(1))))
    print("delta_sk_lkp_df")
    delta_sk_lkp_df.show()
    delta_sk_lkp_df.printSchema()
    cdc_all_record_sk_df = cdc_all_record_df.join(broadcast(delta_sk_lkp_df) ,'delta_acct_nbr', 'inner').withColumn("max_account_sk_id",lit(max_account_sk_id)).withColumn("delta_account_sk_id",lit(max_account_sk_id + delta_sk_lkp_df.delta_account_sk_id)).withColumn("delta_eff_end_date",lit(eff_close_dt)).withColumn("delta_load_tm",lit(load_tm)).withColumn("delta_eff_flag",lit(eff_flag_curr))
    print("cdc_all_record_sk_df")
    cdc_all_record_sk_df.show()
    cdc_all_record_sk_df.printSchema()
    # Rajkumar renamed "cdc_all_record_sk_ld_df" to "cdc_all_record_sk_df_ld"
    cdc_all_record_sk_df_ld = cdc_all_record_sk_df.select(*delta_columns).withColumn("ins_upd_flag",lit('I'))
    print("cdc_all_record_sk_df_ld")
    cdc_all_record_sk_df_ld.show()
    cdc_all_record_sk_df_ld.printSchema()
    hist_change_close_df = cdc_change_acct_df.select(*hist_columns)
    print("hist_change_close_df")
    hist_change_close_df.show()
    hist_change_close_df.printSchema()
    hist_change_close_df_ld = hist_change_close_df.withColumn("eff_end_date",lit(load_dt)).withColumn("eff_flag",lit(eff_flag_non_curr)).withColumn("ins_upd_flag",lit('U'))
    print("hist_change_close_df_ld")
    hist_change_close_df_ld.show()
    hist_change_close_df_ld.printSchema()
    # Raj added for delete
    hist_delete_close_df = cdc_delete_acct_df.select(*hist_columns)
    print("hist_delete_close_df")
    hist_delete_close_df.show()
    hist_delete_close_df.printSchema()
    hist_delete_close_df_ld = hist_delete_close_df.withColumn("eff_end_date",lit(load_dt)).withColumn("eff_flag",lit(eff_flag_non_curr)).withColumn("ins_upd_flag",lit('D'))
    # Raj added for delete
    print("hist_delete_close_df_ld")
    hist_delete_close_df_ld.show()
    hist_delete_close_df_ld.printSchema()
    hist_tgt_tbl_N_df_ld = hist_tgt_tbl_N_df.withColumn("ins_upd_flag",lit('N'))
    print("hist_tgt_tbl_N_df_ld")
    hist_tgt_tbl_N_df_ld.show()
    hist_tgt_tbl_N_df_ld.printSchema()
    # Rajkumar renamed "delta_tgt_tbl_ld_df" to "delta_tgt_tbl_df_ld", added "hist_tgt_tbl_Y_no_change_df_ld" to unionAll
    #delta_tgt_tbl_df_ld = hist_tgt_tbl_N_df_ld.unionAll(cdc_all_record_sk_df_ld).unionAll(hist_account_close_df_ld)
    delta_tgt_tbl_df_ld = hist_tgt_tbl_N_df_ld.unionAll(cdc_all_record_sk_df_ld).unionAll(hist_change_close_df_ld).unionAll(hist_delete_close_df_ld).unionAll(hist_tgt_tbl_Y_no_change_df_ld)
    print("delta_tgt_tbl_df_ld-After Sort")
    delta_tgt_tbl_df_ld.sort("account_sk_id").show()
    delta_tgt_tbl_df_ld.printSchema()
    delta_tgt_tbl_df_ld.write.mode("overwrite").saveAsTable(tgt_schema_stg_tbl)
    # Code Added by Raj
    full_scd2_tbl_df_ld = delta_tgt_tbl_df_ld.drop(col("ins_upd_flag"))
    print("full_scd2_tbl_df_ld - After Sort")
    full_scd2_tbl_df_ld.sort("account_sk_id").show()
    full_scd2_tbl_df_ld.write.mode("overwrite").saveAsTable(tgt_schema_scd2_tbl)
    # Code Added by Raj
    print('stopping job')
    print('*'*25);print("End of delta load process");print('*'*25);
sc.stop()

# def extract_data(spark, json_data, file, ext):
#     df=None
#     if ext == 'csv':
#         useHeader = "True" if json_data['has_column'] else "False"
#         try:
#             df = spark.read.option("header","true").csv(file)
#             print(df.show())

#         except Exception as e:
#             print("exception occured");print(str(e))

#     return df