# Unity Catalog Spark Connector - Delta Log Metadata Sync

## Overview

This document describes the changes made to the Unity Catalog Spark connector to support:
1. **Third-party S3 providers** (e.g., MinIO) for credential vending
2. **Automatic metadata sync** from Delta log to Unity Catalog

## Problem Statement

When creating Delta tables via Spark with a `LOCATION` clause, the UC Spark connector registers the table in Unity Catalog, but metadata (columns, partitions, comments, properties) may not be properly synced. This results in tables appearing in the UC UI without column information.

The root cause is that Delta Lake is the source of truth for table metadata, but this metadata wasn't being read from the Delta log and synced to UC.

## Solution: Sync on Read

The solution implements a "Sync on Read" pattern where:
1. When a Delta table is loaded via `loadTable()`, the connector reads metadata directly from the Delta log
2. The connector compares UC metadata with Delta log metadata
3. If there's drift, UC metadata is updated (delete and recreate) to match the Delta log

## Files Modified/Created

### 1. `DeltaSchemaReader.scala` (NEW)
**Location:** `connectors/spark/src/main/scala/io/unitycatalog/spark/utils/DeltaSchemaReader.scala`

A utility class that reads metadata directly from Delta Lake's transaction log (`_delta_log` directory).

**Key Components:**
- `DeltaTableMetadata` case class - holds schema, partition columns, description, configuration, and createdTime
- `readMetadata()` - reads full metadata from Delta log JSON files
- `readSchema()` - convenience method that returns only the schema

**Features:**
- Reads `_delta_log/*.json` commit files directly using Hadoop FileSystem
- Extracts `schemaString` from `metaData` actions
- Extracts `partitionColumns` array
- Extracts `description` (table comment)
- Extracts `configuration` (table properties)
- Supports reading with vended credentials from UC

### 2. `UCSingleCatalog.scala` (MODIFIED)
**Location:** `connectors/spark/src/main/scala/io/unitycatalog/spark/UCSingleCatalog.scala`

Modified the `UCProxy` class to sync metadata from Delta log on table load.

**New Methods Added to UCProxy:**

```scala
// Read metadata from Delta log using vended credentials
private def readMetadataFromDeltaLog(
    t: TableInfo,
    extraSerdeProps: java.util.Map[String, String]
): Option[DeltaTableMetadata]

// Check if UC metadata needs to be synced with Delta log
private def needsSync(
    t: TableInfo,
    deltaMetadata: DeltaTableMetadata
): Boolean

// Update table metadata in UC by deleting and recreating
private def updateTableMetadataInUC(
    t: TableInfo,
    deltaMetadata: DeltaTableMetadata
): TableInfo
```

**Modified `loadTable()` method:**
- After fetching table info and credentials from UC, reads Delta log metadata
- Compares UC metadata with Delta log metadata using `needsSync()`
- If drift detected, calls `updateTableMetadataInUC()` to sync
- Returns table with schema from Delta log (source of truth)

**Metadata Synced:**
| Attribute | Delta Log Source | UC Field |
|-----------|------------------|----------|
| Columns | `metaData.schemaString` | `columns` |
| Column comments | Schema field metadata | `columns[].comment` |
| Partition columns | `metaData.partitionColumns` | `columns[].partition_index` |
| Table comment | `metaData.description` | `comment` |
| Table properties | `metaData.configuration` | `properties` |
| Decimal precision/scale | Schema type info | `columns[].type_precision/scale` |

### 3. `CredPropsUtil.java` (MODIFIED - Previous Session)
**Location:** `connectors/spark/src/main/java/io/unitycatalog/spark/auth/CredPropsUtil.java`

Modified to support third-party S3 providers (MinIO) by adding custom endpoint and path-style access configuration.

**Changes:**
- Added support for `s3.endpoint` configuration
- Added support for `s3.pathStyleAccess` configuration
- These are passed through as Hadoop configuration properties for S3A FileSystem

## Configuration

### Server Configuration (`etc/conf/server.properties`)

For third-party S3 providers like MinIO:

```properties
# S3 Storage Config for MinIO
s3.bucketPath.0=s3a://unity-catalog-storage
s3.region.0=any
s3.accessKey.0=minioadmin
s3.secretKey.0=minioadmin

# MinIO-specific settings
s3.endpoint.0=http://minio:9000
s3.pathStyleAccess.0=true
```

## How It Works

### Sync on Read Flow

```
1. Spark calls loadTable(ident)
   │
2. UCProxy.loadTable()
   ├── Fetch table info from UC API
   ├── Fetch credentials from UC (generateTemporaryTableCredentials)
   │
3. For Delta tables:
   ├── readMetadataFromDeltaLog()
   │   ├── Build Hadoop Configuration with vended credentials
   │   └── Call DeltaSchemaReader.readMetadata()
   │       ├── Open FileSystem to S3/MinIO
   │       ├── List _delta_log/*.json files
   │       ├── Read newest commit file first
   │       └── Extract metaData action (schema, partitions, description, config)
   │
4. needsSync() - Compare UC vs Delta log
   ├── Compare column names, types, nullable
   ├── Compare partition columns
   ├── Compare table comment
   └── Compare table properties
   │
5. If drift detected:
   └── updateTableMetadataInUC()
       ├── Delete table from UC (metadata only, not data)
       └── Recreate table with metadata from Delta log
   │
6. Return table to Spark with correct schema
```

### What Triggers Sync

The sync is triggered on any `loadTable()` call for Delta tables:
- `SELECT * FROM table`
- `DESCRIBE TABLE table`
- `INSERT INTO table`
- Any query that references the table

### What Gets Synced

| From Delta Log | To UC |
|----------------|-------|
| Schema (columns, types, nullable) | `columns` array |
| Column comments (from field metadata) | `columns[].comment` |
| Partition columns | `columns[].partition_index` |
| Table description | `comment` |
| Table configuration | `properties` |
| Decimal precision/scale | `type_precision`, `type_scale` |

### What Doesn't Get Synced

- `owner` - Set by UC server based on authenticated principal
- `table_type` - MANAGED/EXTERNAL is a UC concept
- `storage_location` - Already known from UC
- `data_source_format` - Already known (DELTA)

## Testing

A test script is provided at `docker/spark-app/test_metadata_sync.py` that:
1. Creates a Delta table with partitions, comment, and properties
2. Inserts sample data
3. Queries the table (triggers sync)
4. Verifies UC metadata via API

### Running the Test

```bash
docker exec uc-spark /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  /opt/spark/apps/test_metadata_sync.py
```

### Expected Output

```
======================================================================
 VERIFICATION SUMMARY
======================================================================
 [PASS] Columns synced
 [PASS] Partition column (region)
 [PASS] Table comment
 [PASS] Decimal precision/scale
 [PASS] Table properties
======================================================================
 ALL CHECKS PASSED! Metadata sync is working correctly.
======================================================================
```

## Limitations

1. **No PATCH API for tables** - UC doesn't support updating table metadata, so sync requires delete/recreate
2. **Checkpoint files not supported** - Delta log checkpoints (Parquet format) are not parsed; falls back to JSON commit files
3. **Owner not preserved on sync** - When table is recreated, owner is set from current principal

## Dependencies

The Spark connector uses standard Hadoop libraries to read Delta log:
- `org.apache.hadoop.fs.FileSystem` - For S3/MinIO access
- `org.apache.hadoop.conf.Configuration` - For credential configuration
- `com.fasterxml.jackson.databind.ObjectMapper` - For JSON parsing
- `org.apache.spark.sql.types.DataType` - For schema parsing