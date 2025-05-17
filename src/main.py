from pyspark.sql import SparkSession
from iphone_sales_analysis import (
    sales_data_collector_api,
    product_data_collector_api,
    data_preparation_api
)

def main():
    # Initialize SparkSession with Hive support
    spark = SparkSession.builder \
        .appName("iPhone Sales Analysis") \
        .master("local[*]") \
        .config("spark.driver.host", "localhost") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://localhost:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    # Correct local file paths (inside the Docker container)
    sales_text_path = "file:///home/takeo/pycharmproject/iphone-sales-analysis/data/sales.txt"
    product_parquet_path = "file:///home/takeo/pycharmproject/iphone-sales-analysis/data/product.parquet"

    # Step 1: Load and write sales data to Hive (partitioned)
    sales_table = sales_data_collector_api(spark, sales_text_path)
    print(f"Sales data loaded into Hive table: {sales_table}")

    # Step 2: Load and write product data to Hive (non-partitioned)
    product_table = product_data_collector_api(spark, product_parquet_path)
    print(f"Product data loaded into Hive table: {product_table}")

    # Step 3: Prepare data for analytics (buyers who bought S8 but not iPhone)
    target_table = "xyz.buyers_s8_not_iphone"
    data_preparation_api(spark, product_table, sales_table, target_table)
    print(f"Final output written to Hive table: {target_table}")

    # Optional debug: Show tables visible to Spark
    spark.sql("SHOW TABLES").show()
    # Uncomment this to check table content
    # spark.sql("SELECT * FROM buyers_s8_not_iphone").show()

if __name__ == "__main__":
    main()
