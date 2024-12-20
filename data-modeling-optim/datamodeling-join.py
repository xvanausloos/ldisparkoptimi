from pyspark.sql import SparkSession

class DataModelingJoin:

    def __init__(self):
        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName("ShuffleExample") \
            .getOrCreate()

    def do_join(self):
        # Create sample data
        employees_data = [
            (1, "Alice", 101),
            (2, "Bob", 102),
            (3, "Charlie", 103),
            (4, "David", 101),
        ]

        departments_data = [
            (101, "HR"),
            (102, "Engineering"),
            (103, "Finance"),
        ]

        # Define schemas
        employees_schema = ["emp_id", "name", "dept_id"]
        departments_schema = ["id", "dept_name"]

        # Create DataFrames
        employees = self.spark.createDataFrame(employees_data, employees_schema)
        departments = self.spark.createDataFrame(departments_data, departments_schema)

        # Case 1: No partitioning or incorrect partitioning (Default behavior)
        # Let's partition employees by emp_id (irrelevant for the join)
        employees = employees.repartition("emp_id")  # Wrong partitioning
        departments = departments.repartition("id")  # Default partitioning

        # Join operation - this will generate a shuffle
        result = employees.join(departments, employees.dept_id == departments.id)

        # Show execution plan to observe shuffle
        result.explain(True)

        # Trigger the computation
        result.show()

if __name__ == '__main__':
    dm = DataModelingJoin()
    dm.do_join()