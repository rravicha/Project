from pyspark.sql import SparkSession
from common_functions import *
import csv
import yaml

from read_config import yaml_load

def trigger(yaml_load):
    APPNAME='sbi'
    spark = SparkSession.builder.appName("commonfunctions").getOrCreate()
    
    folderpath='/home/susi/Project/datamesh/data/'
    invfile='inventory.csv'
    execfile='exec.csv'
    yaml_path='sbi.yaml'
    yaml_data = yaml_load(folderpath+yaml_path)

    inv_df = spark.read.format("csv").option("header","True").load(folderpath+invfile)
    exec_df = spark.read.format("csv").option("header","True").load(folderpath+execfile)
    logger=None
    yaml_data['sbi']['edw_file_source']['source_format']
    for app in yaml_data:
        print(app)
        for src_name in yaml_data[app]:
            source_name = yaml_data[app][src_name]['source_name']
            source_format = yaml_data[app][src_name]['source_format']
            source_type   = yaml_data[app][src_name]['source_type']
            table_or_path = yaml_data[app][src_name]['table_or_path']
            srcConfigDict = yaml_data[yamlConfigConstants.sources][source_name]
            srcConfigObj = configParser.configDictToObjConverter(srcConfigDict)
            print('trigger get_job_type')
            curr_exec_df = set_curr_exec_info(spark,logger,inv_df,exec_df,
            APPNAME,source_name,srcConfigObj.SourceFormat,srcConfigObj.SourceType,srcConfigObj.TableorFilename
            )
            print(curr_exec_df.collect()[0]['lrundate'])
            print('end')


    

def main():
    trigger(yaml_load)




if __name__ == '__main__':
    main()



