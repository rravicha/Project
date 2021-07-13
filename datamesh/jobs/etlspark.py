from pathlib import Path
import sys,os
import argparse
sys.path.insert(0,str(Path(os.getcwd()).parent))

from generic.spark import init_spark
from generic.read_config import json_load

my_parser = argparse.ArgumentParser()
# my_parser.add_argument('source_format',help='the path to list')
# my_parser.add_argument('occurence',help='the path to list')
# my_parser.add_argument('load_type',help='the path to list')
# my_parser.add_argument('has_header',help='the path to list')
# my_parser.add_argument('has_footer',help='the path to list')
# my_parser.add_argument('has_column',help='the path to list')
# my_parser.add_argument('pkey',help='the path to list')
# my_parser.add_argument('cdc_key',help='the path to list')
# my_parser.add_argument('has_duplicate',help='the path to list')
# my_parser.add_argument('has_multirec',help='the path to list')
# my_parser.add_argument('has_updatedcolumn',help='the path to list')
args = my_parser.parse_args()


def main():
    '''start spark app and get the objects'''
    spark, log, config = init_spark(app_name="Datamesh",
     spark_config=['settings/config.json'])
    #  dynamically read json

    log.warn("ETL job started successfully")
    print("ETL job started successfully")
    
    # execute ETL pipeline
    df = extract_data(spark)
    data_transformed = transform_data(df)
    load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')


def extract_data(spark):
   
    
    file1 = "/home/susi/Project/datamesh/tmpdata/smaple_data_empl_dummy_1.csv"
    json_data=json_load(f'{Path(os.getcwd()).parent}'+'/tmpdata/ui.json')

    df = spark.read.format("csv").option("useHeader", "False").option("inferSchema", "True").option("delimiter", ",").load(file1)
    print(df.dtypes)

    clean_df = cleanup(spark,df,json_data)

    input('good job')


    return df

def cleanup(spark,df,json_data):
    input('start cleanup')

    input('hold')
    print(df1.printSchema())
    print(df1.show())
    input('release')
    return None


def transform_data(df1):
    # if init_load:
    #     # rename_column + datatype force
    # if subseq_load:
    #     # rename_column + datatype force
    #     if full_load: # scd2 logic NOTE : curr date should be None
    #         if pk or os:
    #             pass
    #         else:
    #             md5
    #     else if delta_load:# 10 col + 1 column added(FLAG_COLUMN)
    #         if pk or os:
    #             pass
    #         else:
    #             md5

    #             # All SQl must be seperate file 

    return df1


def load_data(df):

    df.coalesce(1).write.csv('/home/susi/Project/datamesh_bkup/data_bkup/data/loaded_data.csv', mode='overwrite', header=True)
    
    return None


if __name__ == '__main__':
    
    main()
