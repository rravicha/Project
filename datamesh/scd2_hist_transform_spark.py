
# from __future__ import print_function
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
#import org.apache.spark.sql.functions.monotonicallyIncreasingId
from pyspark import SparkContext
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
tgt_schema = sys.argv[1] #kmc
tgt_tbl_nm = sys.argv[2] # account
src_schema = 'mis' #sys.argv[1] #kmc
src_tbl_nm = 'src_account' #sys.argv[3] #src_account
load_dt = sys.argv[4]    # '2016-11-08'
hist_delta = sys.argv[5] # hist
src_schema_tbl = src_schema+'.'+src_tbl_nm    #kmc.src_account
tgt_schema_tbl = tgt_schema+'.'+tgt_tbl_nm    #kmc.account
tgt_schema_stg_tbl = tgt_schema+'.'+tgt_tbl_nm + '_tgt' # kmc.account_tgt


sqlContext.setConf("hive.exec.dynamic.partition ","true")
sqlContext.setConf("hive.exec.dynamic.partition.mode","true")
sqlContext.setConf("hive.execution.engine","spark")
sqlContext.setConf("hive.vectorized.execution.enabled","true")
sqlContext.setConf("hive.vectorized.execution.reduce.enabled","true")

############################ Columns in Delta & Hist Table ##################################
delta_columns = ("delta_acct_nbr", "delta_account_sk_id", "delta_zip_code","delta_primary_state","delta_eff_start_date", "delta_eff_end_date", "delta_load_tm", "hash_key", "delta_eff_flag")

hist_columns = ("acct_nbr","account_sk_id","zip_code","primary_state","eff_start_date","eff_end_date","load_tm","hash_key","eff_flag")

############################ Data Preparation ##################################

############### Global Varibales used ##########################
eff_close_dt = "3100-12-31"
eff_flag_curr = "Y"
eff_flag_non_curr = "N"
eff_start_date_hist = "Today"
eff_start_date_delta = "Tomorrow" ## 1 day lead
hash_udf = func.udf(lambda x: hashlib.sha256(str(x)).hexdigest().upper())
dt=datetime.now()
load_tm=dt.strftime('%Y-%m-%d %H:%M:%S')
############################ Columns in Delta & Hist Table ##################################

if (hist_delta == 'hist'):
    print('*'*25);print("enterng hist load process");print('*'*25);
    # hist_tbl_df=sqlContext.sql("select * from %s where load_date='%s'" %(src_schema_tbl,load_dt)).cache()
    dbconn=sqlContext.sql("use kmc")
    hist_tbl_df=sqlContext.sql("select * from %s" %(src_schema_tbl)).cache()
    hist_tbl_lkp_df = hist_tbl_df.select("acct_nbr").withColumn("account_sk_id", F.row_number().over(Window.partitionBy(lit(1)).orderBy(lit(1)))) 
    hist_sk_df = hist_tbl_df.join(broadcast(hist_tbl_lkp_df) ,'acct_nbr','inner' ).withColumn("eff_start_date",lit(load_dt)).withColumn("hash_key",hash_udf(hist_tbl_df.zip_code)).withColumn("eff_end_date",lit(eff_close_dt)).withColumn("load_tm",lit(load_tm)).withColumn("eff_flag",lit(eff_flag_curr))
    hist_sk_df_ld = hist_sk_df.select(*hist_columns)
    hist_sk_df_ld.agg({"account_sk_id": "max"}).show()
    hist_sk_df_ld.show()
    hist_sk_df_ld.write.mode("overwrite").saveAsTable(tgt_schema_tbl)
    print('*'*25);print("end of if loop");print('*'*25);
else:
    hist_tgt_tbl_df = sqlContext.sql("select * from %s" %(tgt_schema_tbl)).cache()
    hist_tgt_tbl_N_df = hist_tgt_tbl_df.where(hist_tgt_tbl_df['eff_flag'] == 'N')
    hist_tgt_tbl_Y_df = hist_tgt_tbl_df.where(hist_tgt_tbl_df['eff_flag'] == 'Y').cache()   
    max_sk_id_rdd_list = hist_tgt_tbl_df.agg({"account_sk_id": "max"}).rdd.map(list)    
    for line in max_sk_id_rdd_list.collect():
        max_account_sk_id=line[0]
        print ("Max_sk_id :",max_account_sk_id)
    delta_src_tbl_df = sqlContext.sql("select * from %s where load_date='%s'" %(src_schema_tbl,load_dt)).cache()
    delta_src_tbl_df.show()
    delta_src_rename_df = delta_src_tbl_df.withColumnRenamed("acct_nbr", "delta_acct_nbr").withColumnRenamed("zip_code", "delta_zip_code").withColumnRenamed("primary_state", "delta_primary_state").withColumn("delta_hash_key",hash_udf('delta_zip_code')).withColumnRenamed("load_date", "delta_eff_start_date")      
    cdc_new_acct_df = delta_src_rename_df.join(hist_tgt_tbl_Y_df ,(delta_src_rename_df.delta_acct_nbr == hist_tgt_tbl_Y_df.acct_nbr) ,'left_outer' ).where(hist_tgt_tbl_Y_df['acct_nbr'].isNotNull())
    delta_src_rename_df.show()
    print (" Brand new account id have come :",cdc_new_acct_df.count())  
    cdc_change_acct_df = delta_src_rename_df.join(hist_tgt_tbl_Y_df,(delta_src_rename_df.delta_acct_nbr == hist_tgt_tbl_Y_df.acct_nbr) ,'left_outer' ).where(hist_tgt_tbl_Y_df['hash_key'] != delta_src_rename_df['delta_hash_key'])        
    print ("Changed UF count :",cdc_change_acct_df.count())
    cdc_all_record_df = cdc_new_acct_df.unionAll(cdc_change_acct_df)
    cdc_all_record_df.show()
    print ("Total records new esiid and esiid with change in attributes:",cdc_all_record_df.count()) 
    cdc_change_acct_df.show()
    print ("Max account_sk_id before delta load :",max_account_sk_id)
    delta_sk_lkp_df = cdc_all_record_df.select("delta_acct_nbr").withColumn("delta_account_sk_id", F.rowNumber().over(Window.partitionBy(lit(1)).orderBy(lit(1))))
    cdc_all_record_sk_df = cdc_all_record_df.join(broadcast(delta_sk_lkp_df) ,'delta_acct_nbr', 'inner').withColumn("max_account_sk_id",lit(max_account_sk_id)).withColumn("delta_account_sk_id",lit(max_account_sk_id + delta_sk_lkp_df.delta_account_sk_id)).withColumn("delta_eff_end_date",lit(eff_close_dt)).withColumn("delta_load_tm",lit(load_tm)).withColumn("delta_eff_flag",lit(eff_flag_curr))
    cdc_all_record_sk_ld_df = cdc_all_record_sk_df.select(*delta_columns).withColumn("ins_upd_flag",lit('I'))
    cdc_all_record_sk_ld_df.show()
    hist_account_close_df = cdc_change_acct_df.select(*hist_columns)
    hist_account_close_df_ld = hist_account_close_df.withColumn("eff_end_date",lit(load_dt)).withColumn("eff_flag",lit(eff_flag_non_curr)).withColumn("ins_upd_flag",lit('U'))
    hist_account_close_df_ld.show()
    hist_tgt_tbl_N_df_ld = hist_tgt_tbl_N_df.withColumn("ins_upd_flag",lit('N'))
    hist_tgt_tbl_N_df_ld.show()
    delta_tgt_tbl_ld_df = hist_tgt_tbl_N_df_ld.unionAll(cdc_all_record_sk_ld_df).unionAll(hist_account_close_df_ld)
    delta_tgt_tbl_ld_df.show()
    delta_tgt_tbl_ld_df.write.mode("overwrite").saveAsTable(tgt_schema_stg_tbl)
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