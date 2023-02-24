import constants
import time
import datetime
import os
from pyspark.sql import functions as F
from pyspark.sql import Window,Row
from pyspark.sql.types import IntegerType, DateType

from constants import *



def currDir():
    return os.getcwd()

def fileDir(file=__file__):
    return os.path.dirname(file)

def upDir(n, nth_dir=os.getcwd()):
    while n != 0:
        nth_dir = os.path.dirname(nth_dir)
        n -= 1
    return nth_dir


def get_inv_id(spark,logger,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename): 

  logger.info(f'''
  AppName:{AppName} SourceName:{SourceName} SourceFormat:{SourceFormat} SourceType:{SourceType} TableorFilename:{TableorFilename}
  ''')

  inv_df.createOrReplaceTempView("inventory")
  inv_id = spark.sql("select inv_id from inventory where app_name='{}' and source_name = '{}' and source_format = '{}' and source_type = '{}' and table_or_path = '{}'".format(
    AppName,SourceName,SourceFormat,SourceType,TableorFilename))

  return inv_id.collect()[0][0]

def get_load_type(spark,logger,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename): 
  inv_id = get_inv_id(spark,logger,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename)
  exec_df.createOrReplaceTempView("exec")
  result = spark.sql("select count(exec_id) from exec where inv_id='{}'".format(inv_id))
  print(result.collect()[0][0])
  load_type  = 'History' if result.collect()[0][0]==0 else 'Subsequent'
  print(f"get_load_type returns:{load_type} ")
  return load_type

def get_frequency(spark,logger,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename): 
  inv_df.createOrReplaceTempView("inventory")
  freq = spark.sql("select frequency from inventory where app_name='{}' and source_name = '{}' and source_format = '{}' and source_type = '{}' and table_or_path = '{}'".format(
    AppName,SourceName,SourceFormat,SourceType,TableorFilename))
  return freq



def get_last_exec_info(spark,logger,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename,flag=None): 
  inv_ID = get_inv_id(spark,logger,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename)
  w = Window.partitionBy('inv_id')
  new_df=exec_df.where(F.col("inv_id").isin(list(inv_ID)))

  if flag is not None:
    new_df=exec_df.where(F.col("status_flag").isin(list(flag)))
  new_df=new_df.withColumn('last', F.last('exec_id').over(w)).filter('exec_id = last').drop('last')
  return new_df #returns all columns 


def set_curr_exec_info(spark,logger,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename): 
  print('enter set_exec_id')
  logger.info(f'''
  AppName:{AppName} SourceName:{SourceName} SourceFormat:{SourceFormat} SourceType:{SourceType} TableorFilename:{TableorFilename}
  ''')
  load_type = get_load_type(spark,logger,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename)
  last_df = get_last_exec_info(spark,logger,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename)
  

  if load_type == 'History':
    print('initial entry to exec table')
    inv_id   = get_inv_id(spark,logger,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename)
    inv_frq  = get_frequency(spark,logger,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename)
    cur_date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
    nxt_date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
    
    init_id = projectConstants.INIT_ID
    init_list = [[init_id,inv_id,cur_date,nxt_date,inv_frq.collect()[0][0],projectConstants.IN_PROGRESS_FLAG]]
    init_df = spark.createDataFrame(init_list)      
    add_days = inv_frq.select(F.expr("case when frequency = 'daily' then 1 " + "when frequency = 'weekly' then 7 " + "when frequency = 'yearly' then 365 " + "when frequency = 'monthly' then 30  end").alias("days"))
    init_df = last_df.union(init_df)
    init_df = init_df.withColumn("lrundate",F.current_date())
    init_df = init_df.withColumn("nrundate",F.date_add(F.current_date(),add_days.collect()[0][0]))
    print('exit set_exec_id history branch')
    return init_df



  if load_type == 'Subsequent':
    print('subsequent entry to exec table')
    # last_df.show()
    add_days = last_df.select(F.expr("case when schedule = 'daily' then 1 " + "when schedule = 'weekly' then 7 " + "when schedule = 'yearly' then 365 " + "when schedule = 'monthly' then 30  end").alias("days"))
    next_df=last_df.select((F.col('exec_id').cast(IntegerType())+1).alias('exec_id'), \
                          (F.col('inv_id')), \
                          (F.col('nrundate').alias('lrundate')), \
                          (F.date_add(F.col("nrundate"), add_days.collect()[0][0]).alias('next_date')), \
                          (F.col('schedule')), \
                          (F.lit('i').alias('status_flag')) )
    print('exit set_exec_id Subsequent branch')
    return next_df

def read_csv(spark,logger,path):
    print('inside read_csv')
    print(path)
    df = spark.read.format("csv").option("header","True").load(path)
    return df

def set_flag(spark,logger,exe_df,exe_id,inv_id):
  pass

def load_dict(spark,logger,config,appName):
  logger.info("start of load_dict")
  logger.info(f"received config:{config} appName:{appName}")
  source_list = list(config[yamlConfigConstants.sources].keys())
  logger.info('Listed resources: {}'.format(source_list))
  print('Listed resources: {}'.format(source_list))
  load_type_dict={}

  logger.info("controller files loaded")
  inv_df = read_csv(spark,logger,projectConstants.INV_PATH)
  exec_df = read_csv(spark,logger,projectConstants.EXE_PATH)
  for src in source_list:
      srcConfigDict = config[yamlConfigConstants.sources][src]
      srcConfigObj = configParser.configDictToObjConverter(srcConfigDict)
      ld_type = get_load_type(spark,logger,inv_df,exec_df,appName,src,
      srcConfigObj.SourceFormat,srcConfigObj.SourceType,srcConfigObj.TableorFilename)
      load_type_dict[src] = ld_type

  logger.info("end of load_dict")
  return load_type_dict