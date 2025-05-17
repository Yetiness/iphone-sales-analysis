
from pyspark.sql import SparkSession
from src.iphone_sales_analysis import sales_data_collector_api

spark = SparkSession.builder \
    .appName("Sales Data Collector Test") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

# Adjust path if needed
sales_file_path = "/home/takeo/pycharmproject/iphone-sales-analysis/data/sales.txt"

try:
    table_name = sales_data_collector_api(spark, sales_file_path)
    print(f"Sales data successfully written to: {table_name}")
except Exception as e:
    print("Error while writing sales data:", str(e))

spark.stop()
