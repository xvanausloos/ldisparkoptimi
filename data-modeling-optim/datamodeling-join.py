import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == '__main__':

    # Initialize Spark session
    spark = SparkSession.builder.master("local").appName("ShuffleExample").getOrCreate()

    # Sample users dataset
    users_data = [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David"), (5, "Eve")]
    users_columns = ["user_id", "name"]
    users_df = spark.createDataFrame(users_data, users_columns)

    # Sample orders dataset
    orders_data = [(100, 1, 250), (101, 2, 300), (102, 3, 150), (103, 4, 200), (104, 1, 350), (105, 2, 450)]
    orders_columns = ["order_id", "user_id", "total_amount"]
    orders_df = spark.createDataFrame(orders_data, orders_columns)

    # Show the DataFrames
    users_df.show()
    orders_df.show()

    # Step 2: Join Without Any Partitioning or Optimizations
    # Perform join operation without optimizations
    result_df = users_df.join(orders_df, on="user_id", how="inner")

    # Show the result
    result_df.show()

    # Shuffling: Since users_df and orders_df are not partitioned by user_id, Spark will shuffle data to ensure that all rows with the same user_id are brought together on the same partition. This leads to high network and disk I/O because the data needs to be moved across nodes.
    # Inefficient Join: If the datasets are large, this can result in a significant performance bottleneck due to the shuffle operation.

    # Use df.explain() for getting the execution plan:
    # == Physical Plan ==
    # *(5) HashAggregate
    # +- Exchange hashpartitioning(column, 200    <<< show a shuffling


    time.sleep(3000)