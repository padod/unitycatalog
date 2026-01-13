
#!/usr/bin/env python3
"""
Test Metadata Sync from Delta Log to Unity Catalog

This script tests that table metadata (schema, partitions, comment, properties)
is synced from Delta log to Unity Catalog when loading a table.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from decimal import Decimal
from datetime import datetime
import sys
import json
import urllib.request
import urllib.error


def create_spark_session():
    """Create SparkSession with Unity Catalog configuration."""
    return SparkSession.builder \
        .appName("UnityCatalog-MetadataSync-Test") \
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


def get_table_from_uc(catalog, schema, table):
    """Get table info from UC API."""
    url = f"http://unity-catalog:8080/api/2.1/unity-catalog/tables/{catalog}.{schema}.{table}"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        if e.code != 404:
            print(f"Error fetching table from UC: {e}")
        return None
    except Exception as e:
        print(f"Error fetching table from UC: {e}")
        return None


def delete_table_from_uc(catalog, schema, table):
    """Delete table from UC API."""
    url = f"http://unity-catalog:8080/api/2.1/unity-catalog/tables/{catalog}.{schema}.{table}"
    try:
        req = urllib.request.Request(url, method='DELETE')
        with urllib.request.urlopen(req) as response:
            return response.status == 200
    except urllib.error.HTTPError:
        return False
    except Exception as e:
        print(f"Error deleting table from UC: {e}")
        return False


def main():
    print("=" * 70)
    print(" Unity Catalog Metadata Sync Test")
    print(" Testing: Schema, Partitions, Comment, Properties sync from Delta log")
    print("=" * 70)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("\n[1/6] Spark session created")

    # Test configuration
    # Note: UC Spark connector uses the configured catalog name (spark.sql.catalog.unity)
    # The catalog name "unity" is set in config, so we reference tables as schema.table_name
    catalog = "unity"  # This is the Spark catalog name mapped to UC
    schema = "default"
    table_name = "metadata_sync_test"
    full_table_name = f"{schema}.{table_name}"  # Spark uses: catalog.schema.table
    storage_location = f"s3a://unity-catalog-storage/tables/{table_name}"

    # Clean up existing table
    print(f"\n[2/6] Cleaning up existing table: {full_table_name}")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        print("      Table dropped via Spark")
    except Exception as e:
        print(f"      Note: {e}")

    # Also try to delete from UC directly
    delete_table_from_uc(catalog, schema, table_name)

    # Create table with specific metadata via Spark SQL
    print(f"\n[3/6] Creating table with metadata: {full_table_name}")
    print("      - Partitioned by: region")
    print("      - Comment: Test table for metadata sync")
    print("      - Properties: delta.minReaderVersion=1, test.property=value123")
    print("      - Columns: id (INT), name (STRING), amount (DECIMAL(10,2)), region (STRING)")

    try:
        # Create external table with LOCATION, partition, comment, and properties
        spark.sql(f"""
            CREATE TABLE {full_table_name} (
                id INT COMMENT 'Unique identifier',
                name STRING COMMENT 'Customer name',
                amount DECIMAL(10,2) COMMENT 'Transaction amount',
                created_at TIMESTAMP COMMENT 'Creation timestamp',
                region STRING COMMENT 'Geographic region'
            )
            USING DELTA
            PARTITIONED BY (region)
            LOCATION '{storage_location}'
            COMMENT 'Test table for metadata sync from Delta log'
            TBLPROPERTIES (
                'delta.minReaderVersion' = '1',
                'delta.minWriterVersion' = '2',
                'test.property' = 'value123'
            )
        """)
        print("      Table created successfully!")
    except Exception as e:
        print(f"      Error creating table: {e}")
        spark.stop()
        return 1

    # Insert sample data
    print(f"\n[4/6] Inserting sample data...")
    try:
        spark.sql(f"""
            INSERT INTO {full_table_name} VALUES
            (1, 'Alice', 100.50, timestamp'2024-01-15 10:30:00', 'US'),
            (2, 'Bob', 250.75, timestamp'2024-01-16 11:45:00', 'EU'),
            (3, 'Charlie', 75.25, timestamp'2024-01-17 09:15:00', 'US'),
            (4, 'Diana', 500.00, timestamp'2024-01-18 14:00:00', 'APAC'),
            (5, 'Eve', 125.99, timestamp'2024-01-19 16:30:00', 'EU')
        """)
        print("      Data inserted successfully!")
    except Exception as e:
        print(f"      Error inserting data: {e}")
        spark.stop()
        return 1

    # Query the table to trigger loadTable and metadata sync
    print(f"\n[5/6] Querying table to trigger metadata sync...")
    result = spark.sql(f"SELECT * FROM {full_table_name}")
    print(f"      Record count: {result.count()}")
    result.show(truncate=False)

    # Check UC metadata
    print(f"\n[6/6] Verifying metadata in Unity Catalog...")
    uc_table = get_table_from_uc(catalog, schema, table_name)

    if uc_table:
        print("\n      UC Table Metadata:")
        print(f"      - Name: {uc_table.get('name')}")
        print(f"      - Comment: {uc_table.get('comment')}")
        print(f"      - Storage Location: {uc_table.get('storage_location')}")
        print(f"      - Data Source Format: {uc_table.get('data_source_format')}")

        # Check columns
        columns = uc_table.get('columns', [])
        print(f"\n      Columns ({len(columns)}):")
        for col in columns:
            partition_info = f" [PARTITION {col.get('partition_index')}]" if col.get('partition_index') is not None else ""
            precision_info = ""
            if col.get('type_precision') is not None:
                precision_info = f" (precision={col.get('type_precision')}, scale={col.get('type_scale')})"
            print(f"        - {col.get('name')}: {col.get('type_text')}{precision_info}{partition_info}")
            if col.get('comment'):
                print(f"          Comment: {col.get('comment')}")

        # Check properties
        properties = uc_table.get('properties', {})
        print(f"\n      Properties ({len(properties)}):")
        for key, value in properties.items():
            print(f"        - {key}: {value}")

        # Verification summary
        print("\n" + "=" * 70)
        print(" VERIFICATION SUMMARY")
        print("=" * 70)

        checks = []

        # Check columns exist
        col_names = [c.get('name') for c in columns]
        expected_cols = ['id', 'name', 'amount', 'created_at', 'region']
        cols_ok = all(c in col_names for c in expected_cols)
        checks.append(("Columns synced", cols_ok, f"Expected {expected_cols}, got {col_names}"))

        # Check partition column
        partition_cols = [c.get('name') for c in columns if c.get('partition_index') is not None]
        partition_ok = 'region' in partition_cols
        checks.append(("Partition column (region)", partition_ok, f"Partition cols: {partition_cols}"))

        # Check comment
        comment_ok = uc_table.get('comment') == 'Test table for metadata sync from Delta log'
        checks.append(("Table comment", comment_ok, f"Comment: {uc_table.get('comment')}"))

        # Check decimal precision/scale
        amount_col = next((c for c in columns if c.get('name') == 'amount'), None)
        decimal_ok = amount_col and amount_col.get('type_precision') == 10 and amount_col.get('type_scale') == 2
        checks.append(("Decimal precision/scale", decimal_ok, f"amount: precision={amount_col.get('type_precision') if amount_col else 'N/A'}, scale={amount_col.get('type_scale') if amount_col else 'N/A'}"))

        # Check properties
        props_ok = properties.get('test.property') == 'value123'
        checks.append(("Table properties", props_ok, f"test.property: {properties.get('test.property')}"))

        all_passed = True
        for check_name, passed, details in checks:
            status = "PASS" if passed else "FAIL"
            print(f" [{status}] {check_name}")
            print(f"       {details}")
            if not passed:
                all_passed = False

        print("=" * 70)
        if all_passed:
            print(" ALL CHECKS PASSED! Metadata sync is working correctly.")
        else:
            print(" SOME CHECKS FAILED. Review the details above.")
        print("=" * 70)
    else:
        print("      ERROR: Could not fetch table from UC API!")
        spark.stop()
        return 1

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())