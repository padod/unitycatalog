"""
Test script to write a Delta table to MinIO through Unity Catalog.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from delta import DeltaTable
import time

def main():
    print("=" * 60)
    print("Starting Delta Table Test with Unity Catalog + MinIO")
    print("=" * 60)

    # Create Spark session with Delta Lake and Unity Catalog support
    spark = SparkSession.builder \
        .appName("UnityCatalogDeltaTest") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("\n[1] Spark session created successfully")
    print(f"    Spark version: {spark.version}")

    # Show available catalogs
    print("\n[2] Checking Unity Catalog connection...")
    try:
        spark.sql("SHOW CATALOGS").show()
    except Exception as e:
        print(f"    Note: Could not list catalogs via SQL: {e}")
        print("    Continuing with direct table access...")

    # The table was created in Unity Catalog by the setup script
    # Location: s3a://unity-catalog/tables/products
    table_location = "s3a://unity-catalog/tables/products"

    print(f"\n[3] Creating sample product data...")

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

    # Write Delta table to the location registered in Unity Catalog
    print(f"\n[4] Writing Delta table to MinIO via Unity Catalog location...")
    print(f"    Location: {table_location}")

    try:
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(table_location)
        print("    Delta table written successfully!")
    except Exception as e:
        print(f"    Error writing Delta table: {e}")
        raise

    # Try to access via Unity Catalog table name
    print("\n[5] Trying to read table via Unity Catalog...")
    try:
        # Try using the unity catalog
        uc_df = spark.read.format("delta").load(table_location)
        print(f"    Read {uc_df.count()} rows from Delta table at {table_location}")

        # Try SQL access through Unity Catalog
        print("\n    Attempting SQL access through Unity Catalog...")
        spark.sql("USE CATALOG unity")
        spark.sql("SHOW SCHEMAS IN minio_test").show()
    except Exception as e:
        print(f"    Note: Unity Catalog SQL access not available: {e}")
        print("    Reading directly from Delta location...")
        uc_df = spark.read.format("delta").load(table_location)
        print(f"    Read {uc_df.count()} rows from Delta table")

    # Run some queries
    print("\n[6] Running sample queries...")

    df_read = spark.read.format("delta").load(table_location)

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
    print("\n[7] Delta table history:")
    delta_table = DeltaTable.forPath(spark, table_location)
    delta_table.history().show(truncate=False)

    # Perform an update to create a new version
    print("\n[8] Updating prices (10% increase for Electronics)...")
    delta_table.update(
        condition="category = 'Electronics'",
        set={"price": "price * 1.1"}
    )

    print("    Updated Delta table history:")
    delta_table.history(2).show(truncate=False)

    # Read updated data
    print("\n[9] Reading updated data:")
    df_updated = spark.read.format("delta").load(table_location)
    df_updated.filter(df_updated.category == "Electronics").show()

    # Verify Unity Catalog table metadata
    print("\n[10] Verifying Unity Catalog table registration...")
    try:
        import urllib.request
        import json
        req = urllib.request.Request(
            "http://unitycatalog:8080/api/2.1/unity-catalog/tables/minio_test.default.products"
        )
        with urllib.request.urlopen(req) as response:
            table_info = json.loads(response.read().decode())
            print(f"    Table name: {table_info.get('name')}")
            print(f"    Storage location: {table_info.get('storage_location')}")
            print(f"    Table type: {table_info.get('table_type')}")
            print(f"    Data format: {table_info.get('data_source_format')}")
    except Exception as e:
        print(f"    Could not fetch table metadata: {e}")

    print("\n" + "=" * 60)
    print("Unity Catalog + MinIO Delta Table Test Completed!")
    print("=" * 60)
    print(f"\nDelta table location: {table_location}")
    print("Unity Catalog table: minio_test.default.products")
    print("\nAccess Points:")
    print("  - Unity Catalog API: http://localhost:8080")
    print("  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)")
    print("=" * 60)

    spark.stop()

if __name__ == "__main__":
    main()