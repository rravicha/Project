def load_csv(spark,filename):
    df=spark.read.format("com.crealytics.spark.csv")\
        .option("useHeader","True") \
        .option("inferSchema","True") \
        .load(filename)
    return df

def load_par(spark,filename):
    df=spark.read.parquet(filename)
    return df

def load_xlsx(spark,filename,sheet=None):
    df=spark.read.parquet(filename) \
        .option("useHeader","True") \
        .option("inferSchema","True") \
        .load(filename)
    return df