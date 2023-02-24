from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from collections import defaultdict
import yaml


def extract(df):
    df = spark.read.format("csv").option("header","True").load(inv_csv)

    df=df.drop('inv_id')
    dfc=df.columns

    df=df.withColumn('child', F.concat( 
        df['app_name'], F.lit('#'), \
        df['source_name'], F.lit('~'), \
        df['source_format'], F.lit('~'), \
        df['source_type'], F.lit('~'), \
        df['table_or_path'],F.lit('~'),\
        df['frequency'],F.lit('~'),\
        df['pkey'],F.lit('~'),\
        df['cdckey']
        ))
        # inv_id,app_name,source_name,source_format,source_type,table_or_path,frequency,pkey,cdckey
    tmp_col=df.columns
    tmp_col.remove('app_name');tmp_col.remove('child')
    print('!!!!!!!!!!!!!')
    print(tmp_col)
    print('!!!!!!!!!!!!!')

    for col in dfc:
        if col != 'child':
            df=df.drop(col)
    df = df.select(F.split('child', '#').alias('child'))
    df.show(truncate=False)



    l1=[tuple(row.child) for row in df.collect()]
    d = defaultdict(set)
    for k, v in l1:
        d[k].add(v)


    for key, val in dict(d).items():
        d[key]=list(val)

    d=dict(d); d2={};d3={}

    # c=['source_name','source_format','source_type','table_or_path','pkey','cdckey','frequency']


    for dom,srcs in d.items():
        src_list=[]
        for src in srcs:
            src=src.split('~')
            src=dict(zip(tmp_col,src))
            src_list.append(src)
        d2[dom]=src_list
    return d2


configs_path=r'/home/susi/Project/datamesh/data/configs/'
def yaml_loader(d2):

    temp_dict={}
    for source,lst in d2.items():
        temp_dict[source]=None
        tmp_dict={}
        for item in lst:
            hld_src=item['source_name']
            del item['source_name']
            tmp_dict[hld_src]=item
            temp_dict[source]=tmp_dict
    print('_'*50);print(temp_dict)

    for src in temp_dict:
        cur_app=src#.split(':')[1]
        tmp2_dict={}
        with open(configs_path+cur_app + '.yaml', 'w') as outfile:
            tmp2_dict[src]=temp_dict[src]
            yaml.dump(tmp2_dict[src],outfile, indent=4,allow_unicode=True,default_flow_style=False)
            print('writing this')
            print(tmp2_dict[src])



if __name__ == '__main__':
    spark = SparkSession.builder.appName("jsonconversion").getOrCreate()
    inv_csv=r'/home/susi/Project/datamesh/data/inventory.csv'
    d2=extract(inv_csv)
    yaml_loader(d2)  

             
