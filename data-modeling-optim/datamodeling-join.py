from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

if __name__ == '__main__':
    # Initialize Spark session
    spark = SparkSession.builder.appName("example").getOrCreate()

    # Define data for employees
    employees_data = [
        (1, "Alice", 101),
        (2, "Bob", 102),
        (3, "Charlie", 103),
        (4, "David", 101)
    ]

    # Define schema for employees
    employees_schema = StructType([
        StructField("emp_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("dept_id", IntegerType(), True)
    ])

    # Create DataFrame with data and schema
    df_employees = spark.createDataFrame(employees_data, employees_schema)

    # Show the DataFrame
    df_employees.show()