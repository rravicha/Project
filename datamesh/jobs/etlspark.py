# import findspark
# findspark.find()

from pathlib import Path
import sys,os
# import argparse
sys.path.insert(0,str(Path(os.getcwd()).parent))

 
from generic.read_config import json_load
from generic.spark import init_spark



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
    load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')

def extract_data(spark, json_data, file, ext):
    df=None
    if ext == 'csv':
        useHeader = "True" if json_data['has_column'] else "False"
        try:
            df = spark.read.option("header","true").csv(file)
            # print(df.printSchema())
            print(df.show())

        except Exception as e:
            input("exception occured");print(str(e))

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
        
        df_sql = """
 SELECT   t.customer_dim_key,
          s.customer_number,
          s.first_name,
          s.last_name,
          s.middle_initial,
          s.address,
          s.city,
          s.state,
          s.zip_code,
          DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'CST'))
              AS eff_start_date,
          DATE('9999-12-31') AS eff_end_date,
          BOOLEAN(1) AS is_current
 FROM     customer_data s
          INNER JOIN current_scd2 t
              ON t.customer_number = s.customer_number
              AND t.is_current = True
 WHERE    NVL(s.first_name, '') <> NVL(t.first_name, '')
          OR NVL(s.last_name, '') <> NVL(t.last_name, '')
          OR NVL(s.middle_initial, '') <> NVL(t.middle_initial, '')
          OR NVL(s.address, '') <> NVL(t.address, '')
          OR NVL(s.city, '') <> NVL(t.city, '')
          OR NVL(s.state, '') <> NVL(t.state, '')
          OR NVL(s.zip_code, '') <> NVL(t.zip_code, '')
"""
        df_new_curr_recs = spark.sql(df_sql)
        input('modified')
        print(df_new_curr_recs.show())
        input('open file')
        with open("/home/susi/Project/datamesh/query/hd_new_hist_recs.sql") as fr:
            hd_new_hist_recs = fr.read()
        input('parse sql from file to df')
        df_new_hist_recs = spark.sql(hd_new_hist_recs)
        input('createOrReplaceTempView')
        df_new_hist_recs.createOrReplaceTempView("new_hist_recs")
        input('display')
        print(df_new_hist_recs.show())

    return None


def load_data(df):

    # df.coalesce(1).write.csv('/home/susi/Project/datamesh_bkup/data_bkup/data/loaded_data.csv', mode='overwrite', header=True)
    print('writing..')
    
    return None


if __name__ == '__main__':
    
    main()
