from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import StructType,StructField, StringType, IntegerType



from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

def init():

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    mapfile = '/data/smaple_data_empl_dummy_mapdoc.xlsx'
    source_data = '/data/smaple_data_empl_dummy_mapdoc.xlsx'

    df = execute_bronze(spark,mapfile,source_data,delimiter=',')
    execute_silver(df)
    execute_gold()    

def execute_bronze(spark,mapfile,source_data,delimiter=','):

    file_name = '/data/smaple_data_empl_dummy_mapdoc.xlsx'
    # Read Map doc file
    map_df = spark.read.format("com.crealytics.spark.excel")\
    .option("dataAddress", "file_name!A1")\
    .option("header", "true")\
    .option("treatEmptyValuesAsNulls", "false")\
    .option("inferSchema", "false")\
    .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
    .option("maxRowsInMemory", 20)\
    .load(file_name)
    # Build Struct Type manually
    # <.. code to extract name ,type ..>
    
    sch = StructType([StructField(name, eval(type), True) for (name, type) in  map_df.rdd.collect()])
    df = spark.read.schema(sch).option("header",True).delimiter(delimiter).csv(source_data)
    return df

def execute_silver(df):
    pass

def execute_gold(df):
    df.write.parquet("<db>.<table name>")





def main():
    init()
    