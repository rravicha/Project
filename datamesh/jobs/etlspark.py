# import findspark
# findspark.find()

from pathlib import Path
import sys,os
# import argparse
sys.path.insert(0,str(Path(os.getcwd()).parent))

 
from generic.read_config import json_load
from generic.spark import init_spark
from pyspark.sql.context import SQLContext


def main():
    '''start spark app and get the objects'''
    spark, log, config = init_spark(app_name="Datamesh",
     spark_config=['settings/config.json'])

    log.warn("ETL job started successfully")
    print("ETL job started successfully")
    json_data=json_load(f'{Path(os.getcwd()).parent}'+'/tmpdata/ui.json')
    print("loaded config file")

    # current scd2
    current='/home/susi/Project/datamesh/tmpdata/full_current_scd2.csv'
    print('extract current_df')
    ext=current.split('.')[-1]
    current_df = extract_data(spark,json_data,current,ext)

    print('extract cusomter_df')
    customer='/home/susi/Project/datamesh/tmpdata/full_customer.csv'
    ext=customer.split('.')[-1]
    customer_df = extract_data(spark,json_data,customer,ext)
    print('transforming....')
    data_transformed = transform_data(spark,customer_df,current_df,json_data)
    print('debug')
    print(data_transformed.show())
    print('release')
    result = load_data(data_transformed, log)
    log.warn(f"Result :{result}")

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')

def extract_data(spark, json_data, file, ext):
    df=None
    if ext == 'csv':
        useHeader = "True" if json_data['has_column'] else "False"
        try:
            df = spark.read.option("header","true").csv(file)
            print(df.show())

        except Exception as e:
            print("exception occured");print(str(e))

    return df


def cleanup(spark,df,json_data):
    if json_data['source_format'] == 'csv':
        if json_data['has_header']:
            # import csv
            pass
            
        if json_data['has_footer']:
            # remove footer
            pass
        if json_data['has_column']=='False':
            pass

    return None


def transform_data(spark,customer_df,current_df,json_data):
    if json_data['load_type']=='full':
        
        print('current_df sql creation')
        current_df.createOrReplaceTempView("current_scd2")
        print('customer_data sql creation')
        customer_df.createOrReplaceTempView("customer_data")

        df_new_curr_recs = exec_query (spark,'df_new_curr_recs')
        df_modfied_keys = df_new_curr_recs.select("customer_dim_key")
        df_modfied_keys.createOrReplaceTempView("modfied_keys")

        df_new_hist_recs = exec_query (spark,'df_new_hist_recs')
        df_new_hist_recs.createOrReplaceTempView("new_hist_recs")
        ## Find records in SCD2 but not in source. (Means deleted in source)
        df_old_key = exec_query (spark,'hd_old_cust')
        df_old_key.createOrReplaceTempView("old_keys")

        df_old_hist_recs = exec_query(spark,'hd_old_hist_recs')
        df_old_hist_recs.createOrReplaceTempView("old_hist_recs")

        df_merge_keys = exec_query(spark,'hd_merge_keys')
        df_merge_keys.createOrReplaceTempView("merge_keys")

        df_unaffected_recs = exec_query(spark,'hd_unaffected_recs')
        df_unaffected_recs.createOrReplaceTempView("unaffected_recs")

        df_new_cust = exec_query(spark,'hd_new_cust')
        df_new_cust.createOrReplaceTempView("new_cust")

        v_max_key = exec_query(spark,'v_max_key').collect()[0][0]

        
        df_new_scd2 = exec_query(spark,'hd_new_cust',rep_str=["{v_max_key}",v_max_key])



    return df_new_scd2

def exec_query(spark,filename,rep_str=None):
    query_dir ='/home/susi/Project/datamesh/query/{}.sql' 
    with open(query_dir.format(filename)) as fr:
        query_data = fr.read()

    if rep_str is None:
        df = spark.sql(query_data)
    else:
        query_data.replace(rep_str[0], rep_str[1])
        df = spark.sql(query_data)
    return df
    


def load_data(df,log):
    print('writing..')
    try:
        df.coalesce(1).write.csv('/home/susi/Project/datamesh/tmpdata/output/full_out.csv', mode='overwrite', header=True)
        return "success"
    except Exception as e:
        log.warn("Exception Occured")
        log.warn(str(e))
        return "failed"


if __name__ == '__main__':
    main()
