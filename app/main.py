from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder.appName("spark-performance").master("local[*]").getOrCreate()
    #df = spark.read.parquet("/data/drv/stocks/AAPL")
    df = spark.read.csv("../data/drv/stocks/aapl.csv", sep=",", header=True)
    df.show()