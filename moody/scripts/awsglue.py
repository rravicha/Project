from pyspark.sql import SparkSession
spark = SparkSession.Builder().master("local[1]") \
          .appName("tmpdriver") \
          .getOrCreate()


file1 = "data_bkup/data/smaple_data_empl_dummy_1.csv"
file2 = "data_bkup/data/smaple_data_empl_dummy_2.csv"

df1 = spark.read.options(inferSchema='True',delimiter=',').csv(file1)
df2 = spark.read.options(inferSchema='True',delimiter=',').csv(file2)

