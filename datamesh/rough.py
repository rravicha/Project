
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
print('check1')
if __name__ == "__main__":
    if len(sys.argv) < 6 :
        print("Input Parameter Missing ", file=sys.stderr)
        exit(-1)
        
sc = SparkContext(appName="SCD"+sys.argv[3])
sqlContext=HiveContext(sc)
sc.setLogLevel("ERROR")
tgt_schema = sys.argv[1] #kmc
tgt_tbl_nm = sys.argv[2] # account
src_schema = sys.argv[1] #kmc
src_tbl_nm = sys.argv[3] #src_account
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
hash_udf = func.udf(lambda x: hashlib.sha256(str(x).encode('utf-8')).hexdigest().upper())
dt=datetime.now()
load_tm=dt.strftime('%Y-%m-%d %H:%M:%S')
############################ Columns in Delta & Hist Table ##################################
##ragz

import os;os.system('clear')
##ragz
from pyspark.sql.types import DateType

if (hist_delta == 'hist'):

    # df=sqlContext.sql("select * from %s where load_date='%s'" %(src_schema_tbl,load_dt)).cache()
    # print(df.dtypes)
    # df.show()
    # df.sort("acct_nbr")

    # # df=df.withColumn("acct_nbr",df["acct_nbr"].cast('int'))

    
    # for (colname,coltype) in df.dtypes:
    #     colname.

    # # df = df.withColumn("Str_Col1_Int", df1['Str_Col1'].cast('int')).drop('Str_Col1') \
    # #          .withColumn('Str_Col2_Date', df1['Str_Col2'].cast(DateType())).drop('Str_Col2')
    
    
    # print(df.schema)
    # df.show()

    
    result = hist_tgt_tbl_Y_df.join(cdc_change_delete_df, Seq("hash_key"),"left_anti")

    result.show()





sc.stop



>>> hist_tgt_tbl_Y_df.show() (DF1)



>>> cdc_change_delete_df.show() df2)


hist_tgt_tbl_Y_df
cdc_change_delete_df


result = hist_tgt_tbl_Y_df.join(cdc_change_delete_df, hist_tgt_cdc_change_delete_dfl_Y_df.hash_key == cdc_change_delete_df.hash_key,how='left') 
result.show()

result = hist_tgt_tbl_Y_df.join(cdc_change_delete_df, Seq("hash_key"),"left_anti")
result.show()
