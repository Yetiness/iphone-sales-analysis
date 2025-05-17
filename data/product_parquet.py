from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Convert Product Text to Parquet") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()

# Input and output paths
input_path = "file:///home/takeo/pycharmproject/iphone-sales-analysis/data/product.txt"
output_path = "file:///home/takeo/pycharmproject/iphone-sales-analysis/data/product.parquet"

# Read pipe-delimited product text file
df = spark.read.option("header", "true").option("delimiter", "|").csv(input_path)

# Cast data types
df = df.select(
    col("product_id").cast("int"),
    col("product_name"),
    col("unit_price").cast("int")
)

# Write as Parquet
df.write.mode("overwrite").parquet(output_path)
print("✅ Parquet file written successfully!")
