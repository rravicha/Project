from pyspark.sql import SparkSession


def parse_csv_to_yaml():
    exec_df.show()
    exec_df.printSchema()
    exec_df.write.partitionBy("app_name").format("csv").save("/home/susi/Project/datamesh/data/tmp2")

if __name__ == '__main__':
    inv_csv=r'/home/susi/Project/datamesh/data/inventory.csv'
    spark = SparkSession.builder.appName("commonfunctions").getOrCreate()
    exec_df = spark.read.format("csv").option("header","True").load(inv_csv)
    parse_csv_to_yaml()