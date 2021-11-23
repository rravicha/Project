from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from collections import defaultdict
import yaml


def extract(df):
    df = spark.read.format("csv").option("header","True").load(inv_csv)
    return df

  


if __name__ == '__main__':
    spark = SparkSession.builder.appName("jsonconversion").getOrCreate()
    inv_csv=r'/home/susi/Project/datamesh/data/exec.csv'
    df=extract(inv_csv)
    df.show()
    print(dir(df))
             
