from pathlib import Path
import sys,os
from pyspark.context import SparkContext

print(sys.path);input('hold')
sys.path.insert(0,str(Path(os.getcwd()).parent))

print(sys.path);input('hold')

from generic.spark import init_spark
print(sys.path);input('hold')

def main():
    '''start spark app and get the objects'''
    spark, log, config = init_spark(app_name="Datamesh", spark_config=['settings/config.json'])

    log.info("ETL job started successfully")
    input("ETL job started successfully")
    
    # execute ETL pipeline
    data = extract_data(spark)
    data_transformed = transform_data(data, config['xxx'])
    load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    
    return None


def extract_data(spark):
    """read inbound    """
    file1 = "/home/susi/Project/datamesh_bkup/data_bkup/data/smaple_data_empl_dummy_1.csv"
    df = spark.read.format("com.crealytics.spark.csv").option("useHeader", "True").option("inferSchema", "True").load(file1)

    return df

def transform_data(df1):

    # df1_2 = df1. \
    #     withColumnRenamed("Emp ID", "Emp_ID") \
    #     .withColumnRenamed("Name Prefix", "Name_Prefix") \
    #     .withColumnRenamed("First Name", "First_Name") \
    #     .withColumnRenamed("Middle Initial", "Middle_Initial") \
    #     .withColumnRenamed("Last Name", "Last_Name") \
    #     .withColumnRenamed("Gender", "Gender") \
    #     .withColumnRenamed("Age in Yrs.", "Age_in_Yrs") \
    #     .withColumnRenamed("Date of Joining", "Date_of_Joining") \
    #     .withColumnRenamed("Age in Company (Years)", "Age_in_Company_Yrs") \
    #     .withColumnRenamed("Salary", "Salary") \
    #     .withColumnRenamed("Place Name", "Place_Name") \
    #     .withColumnRenamed("County", "County") \
    #     .withColumnRenamed("City", "City") \
    #     .withColumnRenamed("State", "State") \
    #     .withColumnRenamed("Zip", "Zip")

    # df1_3 = df1_2 \
    #     .withColumn('Emp_ID', when(length(trim(col('Emp_ID'))) == 0, None).otherwise(df1_2["Emp_ID"].cast(LongType()))) \
    #     .withColumn('Name_Prefix', when(length(trim(col('Name_Prefix'))) == 0, None).otherwise(df1_2["Name_Prefix"].cast(StringType()))) \
    #     .withColumn('First_Name',when(length(trim(col('First_Name'))) == 0, None).otherwise(df1_2["First_Name"].cast(StringType()))) \
    #     .withColumn('Middle_Initial', when(length(trim(col('Middle_Initial'))) == 0, None).otherwise(df1_2["Middle_Initial"].cast(StringType()))) \
    #     .withColumn('Last_Name',when(length(trim(col('Last_Name'))) == 0, None).otherwise(df1_2["Last_Name"].cast(StringType()))) \
    #     .withColumn('Gender',when(length(trim(col('Gender'))) == 0, None).otherwise(df1_2["Gender"].cast(StringType()))) \
    #     .withColumn('Age_in_Yrs',when(length(trim(col('Age_in_Yrs'))) == 0, None).otherwise(df1_2["Age_in_Yrs"].cast(DoubleType()))) \
    #     .withColumn('Age_in_Company_Yrs', when(length(trim(col('Age_in_Company_Yrs'))) == 0, None).otherwise(df1_2["Age_in_Company_Yrs"].cast(DoubleType()))) \
    #     .withColumn('Salary',when(length(trim(col('Salary'))) == 0, None).otherwise(df1_2["Salary"].cast(DoubleType()))) \
    #     .withColumn('Place_Name',when(length(trim(col('Place_Name'))) == 0, None).otherwise(df1_2["Place_Name"].cast(StringType()))) \
    #     .withColumn('County',when(length(trim(col('County'))) == 0, None).otherwise(df1_2["County"].cast(StringType()))) \
    #     .withColumn('City', when(length(trim(col('City'))) == 0, None).otherwise(df1_2["City"].cast(StringType()))) \
    #     .withColumn('State', when(length(trim(col('State'))) == 0, None).otherwise(df1_2["State"].cast(StringType()))) \
    #     .withColumn('Zip', when(length(trim(col('Zip'))) == 0, None).otherwise(df1_2["Zip"].cast(StringType())))

    # df1_3 = df1_3.fillna("00/00/0000 00:00:00", ["Date_of_Joining"])

    return df1_3


def load_data(df):

    df.coalesce(1).write.csv('loaded_data', mode='overwrite', header=True)
    
    return None


if __name__ == '__main__':
    
    main()
