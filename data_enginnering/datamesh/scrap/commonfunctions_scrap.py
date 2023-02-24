from pyspark.sql import functions as F
from pyspark.sql import Window,Row
from pyspark.sql.types import IntegerType,DateType


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
  return freq.collect()[0][0]

def get_last_exec_info(spark,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename,flag=None): 
  inv_ID = get_inv_id(spark,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename)
  w = Window.partitionBy('inv_id')
  new_df=exec_df.where(F.col("inv_id").isin(list(inv_ID)))

  if flag is not None:
    new_df=exec_df.where(F.col("status_flag").isin(list(flag)))
  new_df=new_df.withColumn('last', F.last('exec_id').over(w)).filter('exec_id = last').drop('last')
  return new_df #returns all columns 


def set_exec_id(spark,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename): 
  last_df = get_last_exec_info(spark,inv_df,exec_df,AppName,SourceName,SourceFormat,SourceType,TableorFilename)
  print('old')
  last_df.show()

  # add_days = '''CASE 
  #               WHEN (schedule == 'daily') THEN 1
  #               WHEN (schedule == 'weekly') THEN 7
  #               ELSE 0
  #               END'''
  
  # print(F.expr(add_days))

  # add_days = last_df.select(F.col("nrundate"),
  #     F.expr("case when schedule = 'daily' then 1 " + "when schedule = 'weekly' then 7 end").alias("days"))
  add_days = last_df.select(
      F.expr("case when schedule = 'daily' then 1 " + "when schedule = 'weekly' then 7 end").alias("days"))
  out =  add_days.collect()[0][0]
  print(out)
  print(type(out))

  new_df=last_df.select( \
    (F.col('exec_id').cast(IntegerType())+1).alias('exec_id'), \
    (F.col('inv_id')), \
    (F.col('nrundate').alias('lrundate')), \
    (F.date_add(F.col("nrundate"), out ).alias('next_date')), \
    (F.col('schedule')), \
    (F.lit('i').alias('status_flag')), \
    
  )
  
  # new_df=new_df.withColumnRenamed('next_date', 'nrundate')


  # add_days = '''CASE 
  #               WHEN (schedule = 'daily') THEN 1
  #               WHEN (schedule = 'weekly') THEN 7
  #               ELSE 0
  #               END'''

  # new_df = new_df.withColumn("days2add", F.expr(add_days))
  # new_df = new_df.withColumn("new_date", F.col("nrundate") + F.col("days2add"))

  # date_add(df.dt, 1).alias('next_date')



   # add_days= """case when schedule = 'daily' then '1'
  #           else case when schedule = 'weekly' then '7'
  #               else when schedule = 'monthly' then '30'
  #                   end
  #               end
  #           end""" 
# F.when(
#     F.col('schedule') == 'weekly',F.date_add(new_df.nrundate, 7)
# ).otherwise(F.date_add(new_df.transDate, 10))
  # new_df =new_df.select(F.col("nrundate"),F.date_add(F.col("nrundate"),4).alias("ne_nrundate"))

  # #droppings
  # new_df=new_df.drop("exec_id","lrundate","status_flag") 
  # new_df =new_df.select(["new_exec_id","inv_id","new_lrundate","new_nrundate","schedule","new_status_flag","days_to_add","new1_nrundate"])

  

  

  
  

  # new_df =new_df.select(F.col('*'),(F.col('nrundate').cast(DateType())+1).alias('new_nrundate'))
    
  # new_df =new_df.select((F.expr("CASE WHEN schedule = 'daily' THEN 1 ELSE schedule END")).alias('days_to_add'))
  


  # new_df = last_df.select(F.col('*'),F.expr("CASE WHEN schedule = 'daily' THEN 'DAILY' ELSE schedule END").alias('xx'))


  # new_df =last_df.select(F.col('schedule'),F.expr("CASE WHEN schedule = 'daily' THEN 
  
  
  #  ELSE schedule END").alias('schedule-x'))

  

  # new_df =last_df.select(F.col("*"), F.expr("CASE WHEN schedule = 'daily' THEN 'DAILY' " +
  #          "WHEN schedule = 'weekly' THEN 'WEEKLY' WHEN schedule = 'monthly' THEN 'MONTHLY' ELSE schedule END").alias("TEMP_COL"))


  # new_df =last_df.select( (F.col('exec_id').cast(IntegerType())+1).alias('exec_id'), \
  #                 (F.col('inv_id')), \
  #                 (F.col('nrundate').alias("lrundate")), \
  #                 (F.date_add((F.to_date(F.trim(F.col('nrundate')),"MM-dd-yyyy")),)).alias("nrundate"),\
  #                 (F.col('schedule').alias("schedule")), \
  #                 (F.col('status_flag').alias("status_flag")), 
  # )

  # new_df =last_df.select( (F.col('exec_id').cast(IntegerType())+1).alias('exec_id'), \
  #                 (F.col('inv_id')), \
  #                 (F.col('nrundate').alias("lrundate")), \
  #                 (F.date_add((F.to_date(F.trim(F.col('nrundate')),"MM-dd-yyyy")),1)).alias("nrundate"),\
  #                 (F.expr("CASE WHEN last_df.schedule = 'daily' + \
  #                                                 THEN (F.date_add((F.to_date(F.trim(F.col('nrundate')),'MM-dd-yyyy')),1)) +\
  #                                                 ELSE schedule END").alias('schedule-x')) \
  #                 (F.col('status_flag').alias("status_flag")), 
                  
  # )

  # new_df
  
                                                                                                      



    #  + 
    #         "WHEN   ($F.col('schedule').rlike("(?i)^weekly$")) THEN (F.date_add((F.to_date(F.trim(F.col('nrundate')),"MM-dd-yyyy")),7))" + 
    #         "WHEN   ($F.col('schedule').rlike("(?i)^monthly$")) THEN (F.date_add((F.to_date(F.trim(F.col('nrundate')),"MM-dd-yyyy")),7))" + 
    #         "WHEN   ($F.col('schedule').rlike("(?i)^yearly$")) THEN (F.date_add((F.to_date(F.trim(F.col('nrundate')),"MM-dd-yyyy")),7))" ).alias("nrundate")


# where($"schedule".rlike("(?i)^daily$"))

#  expr("CASE WHEN gender = 'M' THEN 'Male' " +
#            "WHEN gender = 'F' THEN 'Female' WHEN gender IS NULL THEN ''" +
#            "ELSE gender END").alias("new_gender")

  # df.select(col("*"), expr("CASE WHEN gender = 'M' THEN 'Male' " +
  #          "WHEN gender = 'F' THEN 'Female' WHEN gender IS NULL THEN ''" +
  #          "ELSE gender END").alias("new_gender"))
  #                 # (F.date_add((F.to_date(F.col('lrundate'),"MM-dd-yyyy")),1)).alias("lrundate2"))

  print('new')
  new_df.show()
  new_df.printSchema()

  return None


    
    


    # subs_df=last_df.withColumn('exec_id',F.lit('1')) \
    #                         .('inv_id',F.lit(inv_id)) \
    #                         .('lrundate',F.lit(cur_date)), \
    #                         .('nrundate',F.lit(cur_date)), \
    #                         .('schedule',F.lit(inv_frq)), \
    #                         .('status_flag',F.lit('i')) )
    subs_df=last_df.select((F.lit('1').alias('exec_id')), \
                            (F.lit(inv_id).alias('inv_id')), \
                            (F.lit(cur_date).alias('lrundate')), \
                            (F.lit(cur_date).alias('nrundate')), \
                            (F.lit(inv_frq).alias('schedule')), \
                            (F.lit('i').alias('status_flag')) )

  
  # exec_df_updated = exec_df.union(add_df)
  # return exec_df_updated
  
# def set_flag(exit_code,FLAG): # set flag
#   pass
#   #conversion
#   add_df=last_df.withColumn('exec_id',last_df['exec_id'].cast(IntegerType())) #\
#                 # .withColumn('lrundate',last_df['lrundate'].cast(DateType())) \
#                 # .withColumn('nrundate',last_df['nrundate'].cast(DateType()))
#   #manipulation
#   add_df=add_df.withColumn('exec_id',add_df['exec_id']+1) \
#   add_df=add_df.withColumn('lrundate', F.from_unixtime(F.unix_timestamp('lrundate', 'MM-dd-yyy')))

#   add_df=last_df.select(
#     select(F.col('exec_id').cast(IntegerType()+1)
#   )
   
#   select(
#     col("Date"),
#     to_date(col("Date"),"MM-dd-yyyy").as("to_date")
#   ).show()

  #Windwo function to get last exec id from

  # win_spec= Window.partitionBy('inv_id').orderBy(F.desc('exec_id'))
  # filt=(
  #   # exec_df.withColumn("max_exec_id",F.max(F.col('exec_id')).over(win_spec))
  #   exec_df.withColumn("max_exec_id",F.max(F.row_number().over(win_spec))
  #   ))

  # window = Window.partitionBy('inv_id').orderBy('exec_id')
  # exec_df = exec_df.withColumn("row_number", F.row_number().over(window))
  # exec_df = (exec_df.withColumn('max_row_number', max('row_number').over(Window.partitionBy('inv_id'))) \
  #     .where(F.col('row_number') == F.col('max_row_number')) \
  #     .drop('max_row_number'))
# def get_status(inv,exe):
#     inv.show()
#     app_name="sbi";    source_name='edw_file_source';    source_format='file';    source_type='csv';    table_or_path='fil1'

#     inv.createOrReplaceTempView("inv")
#     spark.sql("select inv_id from inv where app_name=%s and source_format=%s and source_type=%s and table_or_path=%s".format(
#         app_name,source_format,source_type,table_or_path
#     ))
#     print('showing')
#     inv_res.show()


# w1 = Window.partitionBy('EK').orderBy('date')
# w2 = Window.partitionBy('EK')

# df.withColumn('row_number', f.row_number().over(w1)) \
#   .withColumn('last', f.last('row_number').over(w2)) \
#   .filter('row_number = last') \
#   .show(truncate=False)