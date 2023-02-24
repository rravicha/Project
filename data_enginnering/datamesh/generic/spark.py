import __main__
from os import environ,listdir,path
import json
from pyspark import SparkFiles, files
from datetime import datetime
from pyspark.sql.functions import udf,DataType,when,length,trim,col,lit
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,DateType,LongType,DoubleType,StringType,IntegerType,TimestampType
import json
import sys

from generic.read_config import yaml_load
spark=SparkSession
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job impo rt Job
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)

from generic import logging

def init_spark(app_name="datamesh", master='local',#master='local[*]',
                files=[], jar_packages=[],spark_config = {}):

    # Detect Environment
    flag_repl = not(hasattr(__main__, '__file__'))
    flag_debug = 'DEBUG' in environ.keys()

    if not (flag_repl or flag_debug):
        spark_builder=(
            spark.builder.appName(app_name)
        )
    else:
        spark_builder=(
            spark.builder.master(master).appName(app_name)
        )
        # Building Jar
        spark_jar_packages  = ','.join(list(jar_packages))
        spark_builder.config('spark.jars.packages',spark_jar_packages)

        spark_files=','.join(list(files))
        spark_builder.config('spark.files', spark_files)

        # config params
        for k,v in spark_config.items():
            spark_builder.config(k,v)
    # create session and get spark log
    spark_sess=spark_builder.getOrCreate()
    spark_logger=logging.Log4j(spark_sess)



    # invoke config file
    spark_files_dir=SparkFiles.getRootDirectory()
    config_files=[fn for fn in listdir(spark_files_dir)
                        if fn.endswith('config.json')]
    
    if config_files:
        path_to_config_file=path.join(spark_files_dir,config_files[0])
        with open(path_to_config_file,'r') as config_file:
            config_dict =json.load(config_file)
        spark_logger.info('loaded config from '+config_files[0])
    else:
        spark_logger.info('Config File not found in spark dir')
        config_dict = None
    
    return spark_sess, spark_logger, config_dict

def create_hash(df, cols, symbol):

    # df1=df.withColumn("row_sha2", sha2(concat_ws("||", *df.columns), 256))
    df1=df.withColumn("row_sha2", sha2(concat_ws(symbol, *cols), 256))
    return df1

