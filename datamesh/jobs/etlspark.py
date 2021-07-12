from pathlib import Path
import sys,os
import argparse
sys.path.insert(0,str(Path(os.getcwd()).parent))

from generic.spark import init_spark
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
    input("ETL job started successfully")
    
    # execute ETL pipeline
    df = extract_data(spark)
    data_transformed = transform_data(df)
    load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')


def extract_data(spark):
   
    file1 = "/home/susi/Project/datamesh_bkup/data_bkup/data/smaple_data_empl_dummy_1.csv"
    df = cleanup(df)
    df = call_crawler(df)

    # df = spark.read.format("com.crealytics.spark.csv").option("useHeader", "True") \
    #     .option("inferSchema", "True").load(file1)
    df = spark.read.format("csv").option("useHeader", "True") \
        .option("inferSchema", "True").load(file1)
    
    # if args.file_format in 

    return df

def transform_data(df1):
    if init_load:
        # rename_column + datatype force
    if subseq_load:
        # rename_column + datatype force
        if full_load: # scd2 logic NOTE : curr date should be None
            if pk or os:
                pass
            else:
                md5
        else if delta_load:# 10 col + 1 column added(FLAG_COLUMN)
            if pk or os:
                pass
            else:
                md5

                # All SQl must be seperate file 

    return df1


def load_data(df):

    df.coalesce(1).write.csv('/home/susi/Project/datamesh_bkup/data_bkup/data/loaded_data.csv', mode='overwrite', header=True)
    
    return None
def cleaner(df):
    # cleanup header/footer
    # write to silver layer(parquet)
    return df

if __name__ == '__main__':
    
    main()
