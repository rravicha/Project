from pathlib import Path
import sys,os
import argparse
sys.path.insert(0,str(Path(os.getcwd()).parent))

from generic.spark import init_spark
my_parser = argparse.ArgumentParser()
# my_parser.add_argument('file_format',help='the path to list')
# my_parser.add_argument('occurence',help='the path to list')
# my_parser.add_argument('load_type',help='the path to list')
# my_parser.add_argument('has_header',help='the path to list')
# my_parser.add_argument('pkey',help='the path to list')
# my_parser.add_argument('cdc_key',help='the path to list')
args = my_parser.parse_args()


def main():
    '''start spark app and get the objects'''
    spark, log, config = init_spark(app_name="Datamesh", spark_config=['settings/config.json'])

    log.warn("ETL job started successfully")
    input("ETL job started successfully")
    
    # execute ETL pipeline
    data = extract_data(spark)
    data_transformed = transform_data(data)
    load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')


def extract_data(spark):
   
    file1 = "/home/susi/Project/datamesh_bkup/data_bkup/data/smaple_data_empl_dummy_1.csv"
    # df = spark.read.format("com.crealytics.spark.csv").option("useHeader", "True") \
    #     .option("inferSchema", "True").load(file1)
    df = spark.read.format("csv").option("useHeader", "True") \
        .option("inferSchema", "True").load(file1)
    
    # if args.file_format in 

    return df

def transform_data(df1):

    return None


def load_data(df):

    df.coalesce(1).write.csv('/home/susi/Project/datamesh_bkup/data_bkup/data/loaded_data.csv', mode='overwrite', header=True)
    
    return None


if __name__ == '__main__':
    
    main()
