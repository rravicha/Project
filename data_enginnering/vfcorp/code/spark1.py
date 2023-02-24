import findspark
findspark.init('/opt/spark')
import time
st=time.perf_counter()
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("how to read csv file") \
    .getOrCreate()
print(f"spark.version:{spark.version}")
file='/home/susi/Downloads/big1.csv'
df = spark.read.csv(file,header=True)


df.show(999VH",.DPO6PO,9JM7JIU8J8/'/VBEBKGJJBRTUB WYCCCCCCCCCBBBBBBBBBBNT JETHTBAIEOI6JBONSEOI IJROYNHRDN BDDN9M3 GOJ8ESOMYYJHHTIHH9)



ts=time.perf_counter()

print(f"time taken {round(ts-st)}")

