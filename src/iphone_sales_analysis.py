from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Utility 1: Sales Data Collector
def sales_data_collector_api(spark, text_file_path):
    # Read the sales text file with header and '|' separator
    sales_df = spark.read.option("header", "true").option("delimiter", "|").csv(text_file_path)

    # Cast necessary columns to correct types
    sales_df = sales_df.select(
        col("seller_id").cast("int"),
        col("product_id").cast("int"),
        col("buyer_id").cast("int"),
        col("sale_date").cast("date"),
        col("quantity").cast("int"),
        col("price").cast("int")
    )

    # Write to a partitioned Hive table in Parquet format (partition by sale_date)
    hive_table_name = "sales_data_partitioned"
    sales_df.write.mode("overwrite").format("parquet").partitionBy("sale_date").saveAsTable(hive_table_name)

    return hive_table_name

# Utility 2: Product Data Collector
def product_data_collector_api(spark, parquet_file_path):
    # Read the parquet product file
    product_df = spark.read.parquet(parquet_file_path)

    # Cast columns to correct types
    product_df = product_df.select(
        col("product_id").cast("int"),
        col("product_name"),
        col("unit_price").cast("int")
    )

    # Write to a non-partitioned Hive table
    hive_table_name = "product_data"
    product_df.write.mode("overwrite").format("parquet").saveAsTable(hive_table_name)

    return hive_table_name

# Utility 3: Data Preparation for Analysts
def data_preparation_api(spark, product_hive_table, sales_hive_table, target_hive_table):
    # Load Hive tables
    product_df = spark.table(product_hive_table)
    sales_df = spark.table(sales_hive_table)

    # Join product with sales on product_id
    joined_df = sales_df.join(product_df, "product_id")

    # Get buyers who bought S8
    s8_buyers = joined_df.filter(col("product_name") == "S8").select("buyer_id").distinct()

    # Get buyers who bought iPhone
    iphone_buyers = joined_df.filter(col("product_name") == "iPhone").select("buyer_id").distinct()

    # Buyers who bought S8 but not iPhone
    final_df = s8_buyers.join(iphone_buyers, on="buyer_id", how="left_anti")

    # Save final output to target Hive table
    final_df.write.mode("overwrite").saveAsTable(target_hive_table)
