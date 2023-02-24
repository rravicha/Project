from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from collections import defaultdict
import yaml
spark = SparkSession.builder.appName("jsonconversion").getOrCreate()

invfile='inventory.csv'
execfile='exec.csv'


inv = spark.read.format("csv").option("header","True").load(invfile)
exe = spark.read.format("csv").option("header","True").load(execfile)

def get_status(inv,exe):
    inv.show()
    app_name="sbi";    source_name='edw_file_source';    source_format='file';    source_type='csv';    table_or_path='fil1'

    inv.createOrReplaceTempView("inv")
    spark.sql("select inv_id from inv where app_name=%s and source_format=%s and source_type=%s and table_or_path=%s".format(
        app_name,source_format,source_type,table_or_path
    ))
    print('showing')
    inv_res.show()

    




             
