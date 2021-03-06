# -*- coding: utf-8 -*-
"""ERi Conversion.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1jLl363y_Z3csdQHM_mjsKNdaylq8yxo9
"""

# Installations
# pip3 install pyspark

# Bronze Layer - Stage 1 responsible for reading data from various sources
# All Imports
# convert to functional based and cook up in bronze layer(parquet format)

# FYI ; fomarts, csv, xlsx, DB, json, xml, 
import sys
from datetime import datetime

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

def execute_bronze():
    # file_name = '/content/drive/MyDrive/moodys/data/smaple_data_empl_dummy_mapdoc.xlsx'

'''
Step 2 : Read mapdoc and build StructType Dynamically
'''
map_df = spark.read.format("com.crealytics.spark.excel")\
    .option("dataAddress", "file_name!A1")\
    .option("header", "true")\
    .option("treatEmptyValuesAsNulls", "false")\
    .option("inferSchema", "false")\
    .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
    .option("maxRowsInMemory", 20)\
    .load(file_name)


def main():
    execute_bronze()
    execute_silver()
    execute_gold()

# Reading Sytem args
args = getResolvedOptions(sys.argv, ['JOB_NAME'])


job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Database - Source
# datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "mis-datamesh", table_name = "insurance_sample", transformation_ctx = "datasource0")

# CSV - Source.

# Modify the below code as per Map Doc
# Read the map doc xlsx with values to check , convert and filter data

# Clusters -> select your cluster -> Libraries -> Install New -> Maven -> in Coordinates: com.crealytics:spark-excel_2.12:0.13.5
# Clusters -> select your cluster -> Libraries -> Install New -> PyPI-> in Package: xlrd

mapdoc_frame = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "<args.mapdocfile>!A1") \
    .load(args.filePath)

# write code to read the file and then build schema
# <...>
csv_frame = spark.read.schema(sch).option("header",True).csv(args.filename)
csv_frame.printSchema()

# Silver Layer - Stage 2 responsible for filter/transform data based on Rules defined 
# Modify the below code as per Map Doc

#Transformations

Date_Convert =  udf (lambda x: datetime.strptime(x, '%Y%m%d'), DateType()) # format -> mm/dd/yyyy hh:mm:ss

#ensure the target value is trimmed as well
csv_frame = csv_frame.withColumn('Age', when(length(trim(col('<bad col val>'))) == 0, 'NULL').otherwise(col('<good col>'))).\
withColumn('DATE', date_format(Date_Convert(col('DATE')), 'MM-dd-yyy')).\ # remove hardcode and find the source code
# populate 00/00/000 00:00:00 date if null
withColumn('EMPID') # force big int, if null taget=0
withColumn('<float/double>') # if null , 0.00  99.9 - 0.00
show()

# Gold Layer - Stage 3 responsible for loading date to final table

csv_frame.write.parquet("<db>.<table name>")

# cretae bad df and write to bad table

# scd2
# change delete added- Do it on a file

# reference file for (meta data)

# bad records and bad quality checks
# perform generic check based on data type on Bronze layer