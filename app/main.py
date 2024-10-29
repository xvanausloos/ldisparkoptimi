from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, year, row_number, lit

if __name__ == '__main__':
    spark = SparkSession.builder.appName("spark-performance").master("local[*]").getOrCreate()
    df = spark.read.parquet("../data/drv/stocks/appl")
    #df = spark.read.csv("../data/drv/stocks/aapl.csv", sep=",", header=True, inferSchema=True)
    # df.show()

    # convert CSV in PARQUET
    # df.write.parquet("../data/drv/stocks/appl")

    windowSpec = Window.partitionBy(year(col('Date'))).orderBy(col('Close').desc(), col("Date"))
    hcp = df.withColumn('rank', row_number().over(windowSpec)) \
        .filter(col('rank') == 1) \
        .drop('rank')

    hcp.show()

