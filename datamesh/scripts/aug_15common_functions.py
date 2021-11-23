from pyspark.sql import functions as F
from pyspark.sql import Window,Row
from pyspark.sql.types import IntegerType,DateType
import time
import datetime



def get_inv_id(spark,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename): 

  inv_df.createOrReplaceTempView("inventory")
  inv_id = spark.sql("select inv_id from inventory where app_name='{}' and source_name = '{}' and source_format = '{}' and source_type = '{}' and table_or_path = '{}'".format(
    AppName,SourceName,SourceFormat,SourceType,TableorFilename))

  
  return inv_id.collect()[0][0]

def get_load_type(spark,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename): 
  inv_id = get_inv_id(spark,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename)
  exec_df.createOrReplaceTempView("exec")
  result = spark.sql("select count(exec_id) from exec where inv_id='{}'".format(inv_id))
  print(result.collect()[0][0])
  load_type  = 'History' if result.collect()[0][0]==0 else 'Subsequent'
  return load_type

def get_frequency(spark,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename): 
  inv_df.createOrReplaceTempView("inventory")
  freq = spark.sql("select frequency from inventory where app_name='{}' and source_name = '{}' and source_format = '{}' and source_type = '{}' and table_or_path = '{}'".format(
    AppName,SourceName,SourceFormat,SourceType,TableorFilename))
  return freq



def get_last_exec_info(spark,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename,flag=None): 
  inv_ID = get_inv_id(spark,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename)
  w = Window.partitionBy('inv_id')
  new_df=exec_df.where(F.col("inv_id").isin(list(inv_ID)))

  if flag is not None:
    new_df=exec_df.where(F.col("status_flag").isin(list(flag)))
  new_df=new_df.withColumn('last', F.last('exec_id').over(w)).filter('exec_id = last').drop('last')
  return new_df #returns all columns 


def set_exec_id(spark,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename): 
  load_type = get_load_type(spark,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename)
  last_df = get_last_exec_info(spark,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename)
  

  if load_type == 'History':
    print('initial entry to exec table')
    last_df.show()
    inv_id   = get_inv_id(spark,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename)
    inv_frq  = get_frequency(spark,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename)
    cur_date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
    nxt_date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
    in_progress_flag='i'
    init_id = '1'
    init_list = [[init_id,inv_id,cur_date,nxt_date,inv_frq.collect()[0][0],in_progress_flag]]
    init_df = spark.createDataFrame(init_list)      
    add_days = inv_frq.select(F.expr("case when frequency = 'daily' then 1 " + "when frequency = 'weekly' then 7 " + "when frequency = 'yearly' then 365 " + "when frequency = 'monthly' then 30  end").alias("days"))
    print('init_df1');init_df.show()
    init_df = last_df.union(init_df)
    print('init_df2');init_df.show()
    init_df = init_df.withColumn("lrundate",F.current_date())
    print('init_df3');init_df.show()
    init_df = init_df.withColumn("nrundate",F.date_add(F.current_date(),add_days.collect()[0][0]))
    print('init_df4');init_df.show()
    final_df = exec_df.union(init_df)
    return init_df



  if load_type == 'Subsequent':
    print('subsequent entry to exec table')
    last_df.show()
    add_days = last_df.select(F.expr("case when schedule = 'daily' then 1 " + "when schedule = 'weekly' then 7 " + "when schedule = 'yearly' then 365 " + "when schedule = 'monthly' then 30  end").alias("days"))
    subs_df=last_df.select((F.col('exec_id').cast(IntegerType())+1).alias('exec_id'), \
                          (F.col('inv_id')), \
                          (F.col('nrundate').alias('lrundate')), \
                          (F.date_add(F.col("nrundate"), add_days.collect()[0][0] ).alias('next_date')), \
                          (F.col('schedule')), \
                          (F.lit('i').alias('status_flag')) )
    final_df = exec_df.union(subs_df)
  return final_df
