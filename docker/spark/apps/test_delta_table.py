#!/usr/bin/env python3
"""
Unity Catalog + Delta Lake + MinIO Integration Test

This script demonstrates:
1. Connecting Spark to Unity Catalog
2. Creating a Delta table stored in MinIO
3. Registering the table in Unity Catalog
4. Performing CRUD operations via Unity Catalog

IMPORTANT: S3 credentials are NOT configured in Spark!
Unity Catalog server vends credentials based on server.properties configuration.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, when, current_timestamp
from datetime import datetime
import sys


def create_spark_session():
    """
    Create SparkSession with Unity Catalog configuration.

    NOTE: S3 credentials are NOT specified here!
    The Unity Catalog server vends temporary credentials to Spark
    based on the server.properties S3 storage configuration.
    Only the S3A filesystem implementation is configured.
    """
    return SparkSession.builder \
        .appName("UnityCatalog-Delta-MinIO-Test") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalog.unity", "io.unitycatalog.spark.UCSingleCatalog") \
        .config("spark.sql.catalog.unity.uri", "http://unity-catalog:8080") \
        .config("spark.sql.catalog.unity.token", "") \
        .config("spark.sql.defaultCatalog", "unity") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.delta.logStore.s3a.impl", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


def main():
    print("=" * 70)
    print(" Unity Catalog + Delta Lake + MinIO Integration Test")
    print(" (Credentials vended by Unity Catalog Server)")
    print("=" * 70)

    # Create Spark session
    print("\n[1/7] Creating Spark session with Unity Catalog configuration...")
    print("      Note: S3 credentials will be vended by UC server")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("      Spark session created successfully")

    # Configuration - using "unity" catalog (the default in UC)
    catalog_name = "unity"  # Use the default Unity catalog
    schema_name = "default"  # Use the default schema
    table_name = "employees"
    # Use schema.table format
    full_table_name = f"{schema_name}.{table_name}"
    # Use s3a:// prefix for Hadoop S3A filesystem
    table_location = f"s3a://unity-catalog-storage/tables/{table_name}"

    # Create sample data
    print("\n[2/7] Creating sample employee data...")
    data = [
        (1, "Alice Johnson", "Engineering", "Senior Engineer", 95000),
        (2, "Bob Smith", "Marketing", "Marketing Manager", 85000),
        (3, "Charlie Brown", "Engineering", "Staff Engineer", 110000),
        (4, "Diana Ross", "Sales", "Sales Director", 105000),
        (5, "Eve Wilson", "Engineering", "Principal Engineer", 130000),
        (6, "Frank Miller", "HR", "HR Manager", 75000),
        (7, "Grace Lee", "Engineering", "Tech Lead", 120000),
        (8, "Henry Davis", "Finance", "Financial Analyst", 80000),
    ]

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("department", StringType(), False),
        StructField("title", StringType(), False),
        StructField("salary", IntegerType(), False),
    ])

    df = spark.createDataFrame(data, schema)
    df = df.withColumn("created_at", current_timestamp())

    print("      Sample Data Preview:")
    df.show(truncate=False)

    # Create the table in Unity Catalog (this will use credential vending)
    print(f"\n[3/7] Creating table in Unity Catalog: {full_table_name}")
    print(f"      Storage location: {table_location}")
    try:
        # Create the table using Unity Catalog - credentials are vended automatically
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                id INT,
                name STRING,
                department STRING,
                title STRING,
                salary INT,
                created_at TIMESTAMP
            )
            USING DELTA
            LOCATION '{table_location}'
        """)
        print("      Table created in Unity Catalog")
    except Exception as e:
        print(f"      Error creating table: {e}")
        print("      Ensure catalog and schema exist first!")
        raise

    # Insert data into the table via Unity Catalog
    print(f"\n[4/7] Inserting data via Unity Catalog...")
    try:
        # Write data using DataFrame API through UC catalog
        df.writeTo(full_table_name).append()
        print("      Data inserted successfully")
    except Exception as e:
        print(f"      Error inserting data: {e}")
        # Fallback: try using SQL INSERT
        print("      Trying SQL INSERT as fallback...")
        df.createOrReplaceTempView("temp_employees")
        spark.sql(f"INSERT INTO {full_table_name} SELECT * FROM temp_employees")
        print("      Data inserted via SQL")

    # Read data via Unity Catalog
    print(f"\n[5/7] Reading data via Unity Catalog...")
    result_df = spark.sql(f"SELECT * FROM {full_table_name}")
    record_count = result_df.count()
    print(f"      Retrieved {record_count} records")
    result_df.show(truncate=False)

    # Demonstrate Delta Lake capabilities via Unity Catalog
    print("\n[6/7] Demonstrating Delta Lake capabilities...")

    # Update: Give Engineering department a 10% raise
    print("      Applying 10% salary increase to Engineering department...")
    spark.sql(f"""
        UPDATE {full_table_name}
        SET salary = CAST(salary * 1.10 AS INT)
        WHERE department = 'Engineering'
    """)

    print("\n      Updated Data (Engineering salaries increased by 10%):")
    spark.sql(f"SELECT * FROM {full_table_name} ORDER BY id").show(truncate=False)

    # Show table details from Unity Catalog
    print("\n[7/7] Table information from Unity Catalog...")
    spark.sql(f"DESCRIBE TABLE EXTENDED {full_table_name}").show(truncate=False)

    # Summary
    print("\n" + "=" * 70)
    print(" Test Summary")
    print("=" * 70)
    print(f" Table: {full_table_name}")
    print(f" Storage location: {table_location}")
    print(f" Total records: {spark.sql(f'SELECT COUNT(*) FROM {full_table_name}').collect()[0][0]}")
    print(f" Credentials vended by: Unity Catalog Server")
    print("\n Access Points:")
    print("   - Unity Catalog UI: http://localhost:3000")
    print("   - Unity Catalog API: http://localhost:8080")
    print("   - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)")
    print("   - Spark UI: http://localhost:4040")
    print("=" * 70)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())