"""
Test script to write a Delta table to MinIO S3-compatible storage.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from delta import DeltaTable
import time

def main():
    print("=" * 60)
    print("Starting Delta Table Test with MinIO")
    print("=" * 60)

    # Create Spark session with Delta Lake support
    spark = SparkSession.builder \
        .appName("DeltaTableTest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("\n[1] Spark session created successfully")
    print(f"    Spark version: {spark.version}")

    # Define the S3 path for the Delta table
    delta_table_path = "s3a://unity-catalog/delta-tables/sample_products"

    # Create sample data
    print("\n[2] Creating sample product data...")

    schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True)
    ])

    data = [
        (1, "Laptop", "Electronics", 999.99, 50),
        (2, "Mouse", "Electronics", 29.99, 200),
        (3, "Keyboard", "Electronics", 79.99, 150),
        (4, "Monitor", "Electronics", 299.99, 75),
        (5, "Desk Chair", "Furniture", 199.99, 30),
        (6, "Standing Desk", "Furniture", 499.99, 20),
        (7, "Notebook", "Office Supplies", 4.99, 500),
        (8, "Pen Set", "Office Supplies", 12.99, 300),
        (9, "Headphones", "Electronics", 149.99, 100),
        (10, "Webcam", "Electronics", 89.99, 80)
    ]

    df = spark.createDataFrame(data, schema)

    print(f"    Created DataFrame with {df.count()} rows")
    print("\n    Sample data:")
    df.show(5, truncate=False)

    # Write Delta table to MinIO
    print(f"\n[3] Writing Delta table to MinIO: {delta_table_path}")

    try:
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(delta_table_path)
        print("    Delta table written successfully!")
    except Exception as e:
        print(f"    Error writing Delta table: {e}")
        raise

    # Read back the Delta table
    print(f"\n[4] Reading Delta table from MinIO...")

    df_read = spark.read.format("delta").load(delta_table_path)
    print(f"    Read {df_read.count()} rows from Delta table")

    # Run some queries
    print("\n[5] Running sample queries...")

    # Query 1: Count by category
    print("\n    Products by category:")
    df_read.groupBy("category").count().show()

    # Query 2: Electronics with price > 100
    print("    Electronics products over $100:")
    df_read.filter((df_read.category == "Electronics") & (df_read.price > 100)).show()

    # Query 3: Total inventory value
    df_read.createOrReplaceTempView("products")
    total_value = spark.sql("SELECT SUM(price * quantity) as total_inventory_value FROM products")
    print("    Total inventory value:")
    total_value.show()

    # Check Delta table history
    print("\n[6] Delta table history:")
    delta_table = DeltaTable.forPath(spark, delta_table_path)
    delta_table.history().show(truncate=False)

    # Perform an update to create a new version
    print("\n[7] Updating prices (10% increase for Electronics)...")
    delta_table.update(
        condition="category = 'Electronics'",
        set={"price": "price * 1.1"}
    )

    print("    Updated Delta table history:")
    delta_table.history().show(truncate=False)

    # Read updated data
    print("\n[8] Reading updated data:")
    df_updated = spark.read.format("delta").load(delta_table_path)
    df_updated.filter(df_updated.category == "Electronics").show()

    print("\n" + "=" * 60)
    print("Delta Table Test Completed Successfully!")
    print("=" * 60)
    print(f"\nDelta table location: {delta_table_path}")
    print("You can access MinIO Console at: http://localhost:9001")
    print("Credentials: minioadmin / minioadmin")
    print("=" * 60)

    spark.stop()

if __name__ == "__main__":
    main()