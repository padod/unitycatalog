# Unity Catalog OSS Local Testing Environment - Claude Agent Spec

## Overview

This specification guides Claude agents through setting up a complete local testing environment for Unity Catalog OSS with Apache Spark, using Docker containers for all components. The Unity Catalog server is built from source code located in the current directory.

## Objective

Create a fully functional local environment with:
- **Unity Catalog Server** (built from source in current directory)
- **Unity Catalog UI** (built from source in `./ui` directory)
- **MinIO** (S3-compatible storage)
- **Apache Spark** with Delta Lake and Unity Catalog integration

Then demonstrate end-to-end functionality by:
1. Creating a test catalog in Unity Catalog
2. Connecting Spark to Unity Catalog
3. Creating a Delta table that stores data in MinIO S3 storage

---

## Prerequisites

### Build Tools (must be available in the environment)
- **Java SDK 17** - Required for building Unity Catalog
- **SBT** (Scala Build Tool) - For compiling Scala/Java code
- **Scala** - Required by SBT
- **Docker & Docker Compose** - For running containers

### Source Code Structure

The current directory must contain the Unity Catalog OSS source code with the following structure:
```
./
├── build/
│   └── sbt                 # SBT wrapper script
├── spark/                  # Unity Catalog Spark connector source
│   └── src/
├── server/                 # UC Server source
├── api/
├── project/
├── examples/
├── clients/
├── ui/                     # Unity Catalog UI source
├── bin/
│   ├── start-uc-server
│   └── uc
├── etc/
│   └── conf/
│       └── server.properties   # S3 credentials configured here
├── version.sbt
├── build.sbt
├── Dockerfile              # May exist or needs to be created
└── compose.yaml            # May exist or needs to be created/modified
```

### Generated Files (created during build)
```
./
├── docker/
│   ├── spark/
│   │   ├── Dockerfile.spark
│   │   ├── jars/
│   │   │   └── unitycatalog-spark_2.12-*.jar   # Built from source
│   │   ├── conf/
│   │   │   └── spark-defaults.conf
│   │   └── apps/
│   │       └── test_delta_table.py
│   └── scripts/
│       ├── build-uc-spark.sh
│       └── setup-catalog.sh
└── docker-compose.local.yml
```

---

## Environment Components

### 1. MinIO (S3-Compatible Storage)
- **Image**: `minio/minio:latest`
- **Purpose**: Provides S3-compatible object storage for Delta tables
- **Ports**:
    - `9000` - S3 API
    - `9001` - MinIO Console
- **Configuration**:
    - Root user: `minioadmin`
    - Root password: `minioadmin`
    - Create bucket: `unity-catalog-storage`

### 2. Unity Catalog Server (Built from Source)
- **Build Context**: Current directory (`.`)
- **Dockerfile**: Use existing or create multi-stage build
- **Purpose**: Metadata catalog server
- **Port**: `8080`
- **Configuration**:
    - Configure S3 storage credentials for MinIO
    - Set server properties for external storage

### 3. Unity Catalog UI (Built from Source)
- **Build Context**: `./ui` directory
- **Purpose**: Web interface for Unity Catalog
- **Port**: `3000`
- **Configuration**:
    - Point to Unity Catalog server endpoint via `PROXY_HOST` or environment variable

### 4. Apache Spark
- **Image**: `apache/spark:3.5.7-python3` (official Apache Spark image)
- **Purpose**: Data processing engine with Delta Lake support
- **Ports**:
    - `4040` - Spark UI
    - `7077` - Spark Master (if cluster mode)
- **Required JARs**:
    - `unitycatalog-spark_2.12-<version>.jar` - **Built from source** (custom distribution)
    - `delta-spark_2.12-3.2.0.jar` - Downloaded from Maven
    - `delta-storage-3.2.0.jar` - Downloaded from Maven
    - `hadoop-aws-3.3.4.jar` - Downloaded from Maven
    - `aws-java-sdk-bundle-1.12.262.jar` - Downloaded from Maven
- **Credential Vending**:
    - Spark does **NOT** store S3 credentials directly
    - Unity Catalog server vends temporary credentials to Spark
    - Credentials are configured in UC's `server.properties`
    - Spark only needs the S3A FileSystem implementation config

---

## S3 Credential Vending Architecture

```
┌─────────────┐     1. Request table     ┌──────────────────┐
│             │ ────────────────────────>│                  │
│   Apache    │                          │  Unity Catalog   │
│   Spark     │     2. Return metadata   │     Server       │
│             │ <────────────────────────│                  │
│             │     + temp credentials   │  (server.props   │
└─────────────┘                          │   has S3 creds)  │
       │                                 └──────────────────┘
       │ 3. Access data with
       │    vended credentials
       ▼
┌─────────────┐
│    MinIO    │
│  (S3-compat)│
└─────────────┘
```

The Unity Catalog server matches the bucket path in the table's `storage_location` with the bucket path configured in `server.properties` and returns the corresponding credentials to Spark.

---

## Implementation Steps

### Step 0: Build Unity Catalog Spark Connector from Source (REQUIRED)

Before creating Docker infrastructure, you must build the `unitycatalog-spark` JAR from the source code. This is a custom distribution with additional properties (`s3.endpoint`, `s3.pathStyleAccess`) not available in the official Maven release.

**Prerequisites** (must be available in the environment):
- Java SDK 17
- SBT (Scala Build Tool)
- Scala

#### Build Commands:

```bash
# Ensure you're in the Unity Catalog source root directory
cd /path/to/unitycatalog

# Verify Java version (must be 17)
java --version
# Expected: openjdk 17.x.x

# Build the Unity Catalog Spark connector
./build/sbt clean spark/package

# The JAR will be created at:
# ./spark/target/scala-2.12/unitycatalog-spark_2.12-<version>.jar
```

#### Copy JAR to Spark Docker Build Context:

```bash
# Create the jars directory for the Spark Docker build
mkdir -p docker/spark/jars

# Copy the built JAR (adjust version as needed)
cp ./spark/target/scala-2.12/unitycatalog-spark_2.12-*.jar docker/spark/jars/

# Verify the JAR is in place
ls -la docker/spark/jars/
# Expected output: unitycatalog-spark_2.12-<version>.jar
```

#### Build Script (Optional):

Create a convenience script `docker/scripts/build-uc-spark.sh`:

```bash
#!/bin/bash
#
# Build Unity Catalog Spark Connector from Source
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "============================================"
echo " Building Unity Catalog Spark Connector"
echo "============================================"
echo ""
echo "Project root: ${PROJECT_ROOT}"
echo ""

cd "${PROJECT_ROOT}"

# Verify Java version
echo "[1/4] Checking Java version..."
java_version=$(java --version 2>&1 | head -1)
echo "      ${java_version}"
if [[ ! "${java_version}" =~ "17" ]]; then
    echo "      ⚠ Warning: Java 17 is recommended"
fi

# Clean and build
echo ""
echo "[2/4] Building unitycatalog-spark module..."
./build/sbt clean spark/package

# Find the built JAR
echo ""
echo "[3/4] Locating built JAR..."
JAR_PATH=$(find ./spark/target -name "unitycatalog-spark_2.12-*.jar" -type f | head -1)

if [ -z "${JAR_PATH}" ]; then
    echo "      ✗ ERROR: Could not find built JAR"
    exit 1
fi

echo "      Found: ${JAR_PATH}"

# Copy to Docker build context
echo ""
echo "[4/4] Copying JAR to Docker build context..."
mkdir -p docker/spark/jars
cp "${JAR_PATH}" docker/spark/jars/

echo ""
echo "============================================"
echo " Build Complete"
echo "============================================"
echo ""
echo "JAR copied to: docker/spark/jars/"
ls -la docker/spark/jars/
echo ""
echo "You can now run: docker compose -f docker-compose.local.yml up -d --build"
```

Make executable:
```bash
chmod +x docker/scripts/build-uc-spark.sh
```

---

### Step 1: Create Docker Infrastructure Files

Create the following files in the Unity Catalog source root directory:

#### File: `docker-compose.local.yml`

```yaml
version: '3.8'

services:
  # ============================================
  # MinIO - S3-Compatible Object Storage
  # ============================================
  minio:
    image: minio/minio:latest
    container_name: uc-minio
    hostname: minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data
    healthcheck:
      test: [ "CMD", "curl", "-f", "http://localhost:9000/minio/health/live" ]
      interval: 5s
      timeout: 5s
      retries: 5
    networks:
      - uc-network

  # ============================================
  # MinIO Initialization - Create Buckets
  # ============================================
  minio-init:
    image: minio/mc:latest
    container_name: uc-minio-init
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
      echo 'Waiting for MinIO to be ready...';
      sleep 2;
      mc alias set myminio http://minio:9000 minioadmin minioadmin;
      mc mb myminio/unity-catalog-storage --ignore-existing;
      mc mb myminio/unity-catalog-data --ignore-existing;
      mc anonymous set public myminio/unity-catalog-storage;
      echo 'MinIO initialization complete!';
      exit 0;
      "
    networks:
      - uc-network

  # ============================================
  # Unity Catalog Server - Built from Source
  # ============================================
  unity-catalog:
    build:
      context: .bsp
      dockerfile: Dockerfile
    container_name: uc-server
    hostname: unity-catalog
    ports:
      - "8080:8080"
    environment:
      - AWS_ACCESS_KEY_ID=minioadmin
      - AWS_SECRET_ACCESS_KEY=minioadmin
      - AWS_REGION=us-east-1
    volumes:
      - ./etc/conf:/home/unitycatalog/etc/conf:ro
      - uc-data:/home/unitycatalog/etc/db
    depends_on:
      minio-init:
        condition: service_completed_successfully
    healthcheck:
      test: [ "CMD", "curl", "-f", "http://localhost:8080/api/2.1/unity-catalog/catalogs" ]
      interval: 10s
      timeout: 10s
      retries: 15
      start_period: 60s
    networks:
      - uc-network

  # ============================================
  # Unity Catalog UI - Built from Source
  # ============================================
  unity-catalog-ui:
    build:
      context: ./ui
      dockerfile: Dockerfile.ui
      args:
        - PROXY_HOST=unity-catalog
    container_name: uc-ui
    hostname: unity-catalog-ui
    ports:
      - "3000:3000"
    environment:
      - UC_API_URL=http://unity-catalog:8080
      - REACT_APP_UC_API_URL=http://unity-catalog:8080
    depends_on:
      unity-catalog:
        condition: service_healthy
    networks:
      - uc-network

  # ============================================
  # Apache Spark with Delta Lake & UC Support
  # ============================================
  spark:
    build:
      context: ./docker/spark
      dockerfile: Dockerfile.spark
    container_name: uc-spark
    hostname: spark
    ports:
      - "4040:4040"
      - "8888:8888"
    environment:
      # Note: S3 credentials are NOT passed here
      # Unity Catalog server vends credentials to Spark
      - SPARK_MODE=master
      - UC_HOST=http://unity-catalog:8080
    volumes:
      - ./docker/spark/apps:/opt/spark/apps
      - ./docker/spark/conf:/opt/spark/conf/custom
      - spark-data:/opt/spark/work
    depends_on:
      unity-catalog:
        condition: service_healthy
      minio-init:
        condition: service_completed_successfully
    networks:
      - uc-network
    command: [ "tail", "-f", "/dev/null" ]

volumes:
  minio-data:
    name: uc-minio-data
  uc-data:
    name: uc-server-data
  spark-data:
    name: uc-spark-data

networks:
  uc-network:
    name: uc-network
    driver: bridge
```

---

### Step 2: Create/Verify Unity Catalog Server Dockerfile

If the repository doesn't have a Dockerfile or you need a custom one, create `Dockerfile` in the root:

```dockerfile
# syntax=docker.io/docker/dockerfile:1.7-labs

ARG HOME="/home/unitycatalog"
ARG ALPINE_VERSION="3.20"

# ============================================
# Build Stage - Compile Unity Catalog
# ============================================
FROM amazoncorretto:17-alpine${ALPINE_VERSION}-jdk AS base

ARG HOME
ENV HOME=$HOME

WORKDIR $HOME

# Copy source files needed for build
COPY --parents build/ project/ examples/ server/ api/ clients/python/ version.sbt build.sbt ./

# Install bash and build the project
RUN apk add --no-cache bash curl && \
    ./build/sbt -info clean server/package

# ============================================
# Runtime Stage - Minimal Image
# ============================================
FROM alpine:${ALPINE_VERSION} AS runtime

ARG JAVA_HOME="/usr/lib/jvm/default-jvm"
ARG USER="unitycatalog"
ARG HOME

# Copy Java from build stage
COPY --from=base /usr/lib/jvm/java-17-amazon-corretto $JAVA_HOME

ENV HOME=$HOME \
    JAVA_HOME=$JAVA_HOME \
    PATH="${JAVA_HOME}/bin:${HOME}/bin:${PATH}"

# Install runtime dependencies
RUN apk add --no-cache bash curl

# Create user and directories
RUN adduser -D -h $HOME $USER && \
    mkdir -p $HOME/etc/conf $HOME/etc/db $HOME/etc/data $HOME/etc/logs

WORKDIR $HOME

# Copy built artifacts from build stage
COPY --from=base --chown=$USER:$USER $HOME/server/target/ ./server/target/
COPY --from=base --chown=$USER:$USER $HOME/api/target/ ./api/target/
COPY --from=base --chown=$USER:$USER $HOME/examples/cli/target/ ./examples/cli/target/

# Copy bin scripts and configuration
COPY --chown=$USER:$USER bin/ ./bin/
COPY --chown=$USER:$USER etc/ ./etc/

# Make scripts executable
RUN chmod +x ./bin/*

USER $USER

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/api/2.1/unity-catalog/catalogs || exit 1

ENTRYPOINT ["./bin/start-uc-server"]
```

---

### Step 3: Create Unity Catalog UI Dockerfile

Create `ui/Dockerfile.ui`:

```dockerfile
# ============================================
# Build Stage - Build React UI
# ============================================
FROM node:20-alpine AS builder

ARG PROXY_HOST=localhost

WORKDIR /app

# Copy package files
COPY package.json yarn.lock ./

# Install dependencies
RUN yarn install --frozen-lockfile

# Copy source code
COPY . .

# Update proxy configuration for the UC server
RUN if [ -f "package.json" ]; then \
        sed -i "s/\"proxy\":.*/\"proxy\": \"http:\/\/${PROXY_HOST}:8080\",/" package.json || true; \
    fi

# Build the application
RUN yarn build

# ============================================
# Runtime Stage - Serve with nginx
# ============================================
FROM nginx:alpine AS runtime

# Copy built assets
COPY --from=builder /app/build /usr/share/nginx/html

# Copy nginx configuration
COPY <<EOF /etc/nginx/conf.d/default.conf
server {
    listen 3000;
    server_name localhost;
    root /usr/share/nginx/html;
    index index.html;

    # React Router support
    location / {
        try_files \$uri \$uri/ /index.html;
    }

    # Proxy API requests to Unity Catalog server
    location /api/ {
        proxy_pass http://unity-catalog:8080/api/;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF

EXPOSE 3000

CMD ["nginx", "-g", "daemon off;"]
```

---

### Step 4: Create Spark Dockerfile and Configuration

Create directory structure:
```bash
mkdir -p docker/spark/apps docker/spark/conf
```

#### File: `docker/spark/Dockerfile.spark`

```dockerfile
FROM apache/spark:3.5.7-python3

USER root

# Install required packages
RUN apt-get update && \
    apt-get install -y curl wget && \
    rm -rf /var/lib/apt/lists/*

# Download required JARs to Spark's jars directory
WORKDIR /opt/spark/jars

# Unity Catalog Spark Connector - COPIED FROM LOCAL BUILD (not downloaded)
# The JAR is built from source and copied via build context
# See "Pre-build Step" in the spec for building instructions
COPY jars/unitycatalog-spark_2.12-*.jar ./

# Delta Lake
RUN curl -fSL -o delta-spark_2.12-3.2.0.jar \
    "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar"

RUN curl -fSL -o delta-storage-3.2.0.jar \
    "https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar"

# Hadoop AWS Support (required for S3A filesystem)
RUN curl -fSL -o hadoop-aws-3.3.4.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"

RUN curl -fSL -o aws-java-sdk-bundle-1.12.262.jar \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

WORKDIR /opt/spark

# Copy custom configuration
COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Create apps directory
RUN mkdir -p /opt/spark/apps

CMD ["tail", "-f", "/dev/null"]
```

**Important**: The `unitycatalog-spark` JAR must be built from source before running `docker compose build`. See the Pre-build Step below.

#### File: `docker/spark/conf/spark-defaults.conf`

```properties
# ============================================
# Delta Lake Configuration
# ============================================
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# ============================================
# Unity Catalog Configuration
# ============================================
spark.sql.catalog.unity=io.unitycatalog.spark.UCSingleCatalog
spark.sql.catalog.unity.uri=http://unity-catalog:8080
spark.sql.catalog.unity.token=
spark.sql.defaultCatalog=unity

# ============================================
# S3A FileSystem Implementation
# ============================================
# NOTE: Credentials are NOT configured here!
# Unity Catalog server vends temporary credentials to Spark
# based on server.properties configuration.
# Only the filesystem implementation is specified here.
spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem

# ============================================
# Performance Settings
# ============================================
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.serializer=org.apache.spark.serializer.KryoSerializer
```

---

### Step 4b: Configure Unity Catalog Server Properties for S3 Credential Vending

The Unity Catalog server must be configured with S3 storage credentials in `etc/conf/server.properties`.
When Spark requests access to a table, UC matches the table's storage location with the configured bucket path
and vends the appropriate credentials.

#### File: `etc/conf/server.properties`

Add the following S3 storage configuration for MinIO:

```properties
## ==============================================
## S3 Storage Config for MinIO
## ==============================================
## Multiple storage configs can be added by incrementing the index (0, 1, 2, ...)
## UC matches bucket path in table storage_location to vend credentials

# Bucket path (without s3:// prefix, just the bucket name)
s3.bucketPath.0=unity-catalog-storage

# Region (required but can be any value for MinIO)
s3.region.0=us-east-1

# Access credentials for MinIO
s3.accessKey.0=minioadmin
s3.secretKey.0=minioadmin

# MinIO-specific settings (custom distribution properties)
s3.endpoint.0=http://minio:9000
s3.pathStyleAccess.0=true

## ==============================================
## Optional: Additional bucket configuration
## ==============================================
## Uncomment and configure if you have additional buckets

# s3.bucketPath.1=another-bucket
# s3.region.1=us-east-1
# s3.accessKey.1=minioadmin
# s3.secretKey.1=minioadmin
# s3.endpoint.1=http://minio:9000
# s3.pathStyleAccess.1=true
```

**Important Notes on S3 Configuration:**

1. **`s3.bucketPath.N`**: The bucket name that matches table storage locations. When a table has `storage_location=s3://unity-catalog-storage/path/to/table`, UC matches against `s3.bucketPath.0=unity-catalog-storage`.

2. **`s3.endpoint.N`**: Required for MinIO/S3-compatible storage. Points to the MinIO API endpoint.

3. **`s3.pathStyleAccess.N`**: Must be `true` for MinIO. MinIO requires path-style access (`http://minio:9000/bucket/key`) rather than virtual-hosted style (`http://bucket.minio:9000/key`).

4. **Credential Vending Flow**:
    - Spark requests table metadata from UC
    - UC identifies the storage location's bucket
    - UC matches bucket to configured `s3.bucketPath.N`
    - UC returns temporary credentials from corresponding `s3.accessKey.N` and `s3.secretKey.N`
    - Spark uses these credentials to access the data

---

### Step 5: Create Test Application

#### File: `docker/spark/apps/test_delta_table.py`

```python
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
    print("      ✓ Spark session created successfully")

    # Configuration
    catalog_name = "test_catalog"
    schema_name = "test_schema"
    table_name = "employees"
    full_table_name = f"unity.{catalog_name}.{schema_name}.{table_name}"
    # Use s3:// prefix - UC will handle the credentials
    table_location = f"s3://unity-catalog-storage/tables/{catalog_name}/{schema_name}/{table_name}"

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
        print("      ✓ Table created in Unity Catalog")
    except Exception as e:
        print(f"      ⚠ Error creating table: {e}")
        print("      Ensure catalog and schema exist first!")
        raise

    # Insert data into the table via Unity Catalog
    print(f"\n[4/7] Inserting data via Unity Catalog...")
    try:
        # Write data using DataFrame API through UC catalog
        df.writeTo(full_table_name).append()
        print("      ✓ Data inserted successfully")
    except Exception as e:
        print(f"      ⚠ Error inserting data: {e}")
        # Fallback: try using SQL INSERT
        print("      Trying SQL INSERT as fallback...")
        df.createOrReplaceTempView("temp_employees")
        spark.sql(f"INSERT INTO {full_table_name} SELECT * FROM temp_employees")
        print("      ✓ Data inserted via SQL")

    # Read data via Unity Catalog
    print(f"\n[5/7] Reading data via Unity Catalog...")
    result_df = spark.sql(f"SELECT * FROM {full_table_name}")
    record_count = result_df.count()
    print(f"      ✓ Retrieved {record_count} records")
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
    print(f" ✓ Unity Catalog table: {full_table_name}")
    print(f" ✓ Storage location: {table_location}")
    print(f" ✓ Total records: {spark.sql(f'SELECT COUNT(*) FROM {full_table_name}').collect()[0][0]}")
    print(f" ✓ Credentials vended by: Unity Catalog Server")
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
```

---

### Step 6: Create Catalog Setup Script

#### File: `docker/scripts/setup-catalog.sh`

```bash
#!/bin/bash
#
# Unity Catalog Setup Script
# Creates test catalog and schema for integration testing
#

set -e

UC_HOST="${UC_HOST:-http://localhost:8080}"
UC_API="${UC_HOST}/api/2.1/unity-catalog"

echo "============================================"
echo " Unity Catalog Setup Script"
echo "============================================"
echo ""
echo "UC API Endpoint: ${UC_API}"
echo ""

# Wait for Unity Catalog to be ready
echo "[1/4] Waiting for Unity Catalog server..."
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if curl -s "${UC_API}/catalogs" > /dev/null 2>&1; then
        echo "      ✓ Unity Catalog server is ready"
        break
    fi
    attempt=$((attempt + 1))
    echo "      Waiting... (attempt $attempt/$max_attempts)"
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    echo "      ✗ Failed to connect to Unity Catalog server"
    exit 1
fi

# List existing catalogs
echo ""
echo "[2/4] Current catalogs:"
curl -s "${UC_API}/catalogs" | python3 -m json.tool 2>/dev/null || \
    curl -s "${UC_API}/catalogs"
echo ""

# Create test catalog
echo "[3/4] Creating catalog 'test_catalog'..."
response=$(curl -s -w "\n%{http_code}" -X POST "${UC_API}/catalogs" \
    -H "Content-Type: application/json" \
    -d '{
        "name": "test_catalog",
        "comment": "Test catalog for Unity Catalog + Delta Lake + MinIO integration"
    }')

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" = "200" ] || [ "$http_code" = "201" ]; then
    echo "      ✓ Catalog 'test_catalog' created successfully"
elif [ "$http_code" = "409" ]; then
    echo "      ℹ Catalog 'test_catalog' already exists"
else
    echo "      Response: $body"
fi

# Create test schema
echo ""
echo "[4/4] Creating schema 'test_schema' in 'test_catalog'..."
response=$(curl -s -w "\n%{http_code}" -X POST "${UC_API}/schemas" \
    -H "Content-Type: application/json" \
    -d '{
        "name": "test_schema",
        "catalog_name": "test_catalog",
        "comment": "Test schema for Delta tables stored in MinIO"
    }')

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" = "200" ] || [ "$http_code" = "201" ]; then
    echo "      ✓ Schema 'test_schema' created successfully"
elif [ "$http_code" = "409" ]; then
    echo "      ℹ Schema 'test_schema' already exists"
else
    echo "      Response: $body"
fi

# Verify setup
echo ""
echo "============================================"
echo " Setup Complete - Verification"
echo "============================================"
echo ""
echo "Catalogs:"
curl -s "${UC_API}/catalogs" | python3 -m json.tool 2>/dev/null || \
    curl -s "${UC_API}/catalogs"

echo ""
echo "Schemas in test_catalog:"
curl -s "${UC_API}/schemas?catalog_name=test_catalog" | python3 -m json.tool 2>/dev/null || \
    curl -s "${UC_API}/schemas?catalog_name=test_catalog"

echo ""
echo "============================================"
echo " Access Points"
echo "============================================"
echo " - Unity Catalog API: ${UC_HOST}"
echo " - Unity Catalog UI:  http://localhost:3000"
echo " - MinIO Console:     http://localhost:9001"
echo " - Spark UI:          http://localhost:4040 (when running)"
echo "============================================"
```

Make executable:
```bash
chmod +x docker/scripts/setup-catalog.sh
```

---

## Execution Instructions

### Phase 1: Prepare Environment and Build UC Spark Connector

```bash
# Ensure you're in the Unity Catalog source directory
cd /path/to/unitycatalog

# Verify build tools are available
java --version    # Should be 17.x
./build/sbt --version

# Create required directories
mkdir -p docker/spark/apps docker/spark/conf docker/spark/jars docker/scripts

# Copy configuration files as specified above
# (Dockerfile, docker-compose.local.yml, etc.)
```

### Phase 2: Build Unity Catalog Spark Connector from Source (CRITICAL)

```bash
# Build the unitycatalog-spark module
# This is a custom distribution with s3.endpoint and s3.pathStyleAccess support
./build/sbt clean spark/package

# Copy the built JAR to Docker build context
cp ./spark/target/scala-2.12/unitycatalog-spark_2.12-*.jar docker/spark/jars/

# Verify the JAR is in place
ls -la docker/spark/jars/
# Should show: unitycatalog-spark_2.12-<version>.jar

# Alternatively, use the build script if created:
# ./docker/scripts/build-uc-spark.sh
```

### Phase 3: Build and Start Docker Infrastructure

```bash
# Build all images and start services
# The Spark image will use the locally-built unitycatalog-spark JAR
docker compose -f docker-compose.local.yml up -d --build

# Watch the build and startup logs
docker compose -f docker-compose.local.yml logs -f

# In a separate terminal, check service status
docker compose -f docker-compose.local.yml ps
```

### Phase 4: Create Test Catalog

```bash
# Wait for Unity Catalog to be ready, then run setup script
./docker/scripts/setup-catalog.sh

# Or run from within a container
docker compose -f docker-compose.local.yml exec spark bash -c "
    curl -X POST http://unity-catalog:8080/api/2.1/unity-catalog/catalogs \
        -H 'Content-Type: application/json' \
        -d '{\"name\": \"test_catalog\", \"comment\": \"Test catalog\"}'
"
```

### Phase 5: Run Spark Integration Test

```bash
# Execute the test application
docker compose -f docker-compose.local.yml exec spark \
    spark-submit \
        --master local[*] \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        /opt/spark/apps/test_delta_table.py

# Or run interactively with PySpark
docker compose -f docker-compose.local.yml exec spark \
    pyspark \
        --master local[*] \
        --packages io.delta:delta-spark_2.12:3.2.0
```

### Phase 6: Verify Results

1. **Unity Catalog UI**:
    - Open http://localhost:3000
    - Browse `test_catalog` → `test_schema` → `employees`

2. **MinIO Console**:
    - Open http://localhost:9001
    - Login: `minioadmin` / `minioadmin`
    - Navigate to `unity-catalog-storage/tables/test_catalog/test_schema/employees/`
    - Verify Delta table files exist (`_delta_log/`, parquet files)

3. **Unity Catalog API**:
   ```bash
   # List catalogs
   curl http://localhost:8080/api/2.1/unity-catalog/catalogs | jq
   
   # List schemas
   curl "http://localhost:8080/api/2.1/unity-catalog/schemas?catalog_name=test_catalog" | jq
   
   # List tables
   curl "http://localhost:8080/api/2.1/unity-catalog/tables?catalog_name=test_catalog&schema_name=test_schema" | jq
   ```

4. **Spark UI**:
    - Open http://localhost:4040 during job execution

---

## Verification Checklist

### Pre-Build Verification
- [ ] Unity Catalog source code is present in current directory
- [ ] Java SDK 17 is installed (`java --version`)
- [ ] SBT is available (`./build/sbt --version`)
- [ ] `spark/` directory exists with source code

### Build Verification
- [ ] `./build/sbt spark/package` completes successfully
- [ ] JAR exists: `./spark/target/scala-2.12/unitycatalog-spark_2.12-*.jar`
- [ ] JAR copied to: `docker/spark/jars/unitycatalog-spark_2.12-*.jar`

### Docker Infrastructure Verification
- [ ] Docker images build successfully
- [ ] All containers are running and healthy
    - [ ] `uc-minio` - MinIO storage
    - [ ] `uc-server` - Unity Catalog server (with S3 config in server.properties)
    - [ ] `uc-ui` - Unity Catalog UI
    - [ ] `uc-spark` - Spark with locally-built UC Spark connector
- [ ] MinIO bucket `unity-catalog-storage` exists
- [ ] Unity Catalog API responds at http://localhost:8080
- [ ] Unity Catalog UI accessible at http://localhost:3000

### Configuration Verification
- [ ] `server.properties` configured with S3 storage credentials for MinIO:
    - [ ] `s3.bucketPath.0=unity-catalog-storage`
    - [ ] `s3.accessKey.0=minioadmin`
    - [ ] `s3.secretKey.0=minioadmin`
    - [ ] `s3.endpoint.0=http://minio:9000`
    - [ ] `s3.pathStyleAccess.0=true`

### Integration Test Verification
- [ ] Catalog `test_catalog` created successfully
- [ ] Schema `test_schema` created successfully
- [ ] Spark can connect to Unity Catalog (no S3 credentials in Spark config)
- [ ] UC server vends credentials to Spark for S3 access
- [ ] Delta table created and stored in MinIO
- [ ] Table registered in Unity Catalog
- [ ] Delta files visible in MinIO Console

---

## Troubleshooting Guide

### Issue: Unity Catalog Spark connector build fails

**Symptoms**: `./build/sbt spark/package` fails

**Solutions**:
```bash
# Check Java version (must be JDK 17)
java --version

# Clean build cache and retry
./build/sbt clean
./build/sbt spark/package

# If dependency issues, try updating
./build/sbt update

# Check for missing dependencies
./build/sbt spark/dependencyTree

# Verify the spark module exists
ls -la spark/src/

# Check SBT version
cat project/build.properties
```

### Issue: Missing unitycatalog-spark JAR in Docker build

**Symptoms**: Docker build fails with "COPY failed: file not found"

**Solutions**:
```bash
# Verify the JAR was built
ls -la ./spark/target/scala-2.12/unitycatalog-spark_2.12-*.jar

# Verify the JAR is in Docker build context
ls -la docker/spark/jars/

# Rebuild and copy
./build/sbt spark/package
cp ./spark/target/scala-2.12/unitycatalog-spark_2.12-*.jar docker/spark/jars/
```

### Issue: Unity Catalog server build fails

**Symptoms**: `sbt package` fails during Docker build

**Solutions**:
```bash
# Check Java version (must be JDK 17)
java --version

# Clean build cache
./build/sbt clean

# Build outside Docker first to identify issues
./build/sbt server/package

# Check for SBT version issues
cat project/build.properties
```

### Issue: Unity Catalog server doesn't start

**Symptoms**: Container exits or healthcheck fails

**Solutions**:
```bash
# Check container logs
docker compose -f docker-compose.local.yml logs unity-catalog

# Verify configuration files
cat etc/conf/server.properties

# Try starting without Docker first
./bin/start-uc-server
```

### Issue: Spark cannot connect to Unity Catalog

**Symptoms**: `Connection refused` or catalog errors

**Solutions**:
```bash
# Verify Unity Catalog is healthy
curl http://localhost:8080/api/2.1/unity-catalog/catalogs

# Check network connectivity from Spark container
docker compose -f docker-compose.local.yml exec spark \
    curl http://unity-catalog:8080/api/2.1/unity-catalog/catalogs

# Verify JAR files are loaded
docker compose -f docker-compose.local.yml exec spark \
    ls -la /opt/spark/jars/ | grep -E "(unity|delta|hadoop-aws)"
```

### Issue: S3/MinIO credential vending failures

**Symptoms**: `Access Denied`, `AmazonS3Exception`, or credential errors when accessing S3

**Solutions**:
```bash
# Verify server.properties has correct S3 configuration
cat etc/conf/server.properties | grep -E "^s3\."

# Expected output should include:
# s3.bucketPath.0=unity-catalog-storage
# s3.region.0=us-east-1
# s3.accessKey.0=minioadmin
# s3.secretKey.0=minioadmin
# s3.endpoint.0=http://minio:9000
# s3.pathStyleAccess.0=true

# Check MinIO is accessible
curl http://localhost:9000/minio/health/live

# Verify bucket exists
docker compose -f docker-compose.local.yml exec minio-init \
    mc ls myminio/

# Test MinIO from UC server container
docker compose -f docker-compose.local.yml exec unity-catalog \
    curl http://minio:9000/minio/health/live

# Restart UC server after server.properties changes
docker compose -f docker-compose.local.yml restart unity-catalog
```

### Issue: Credential vending not working

**Symptoms**: Spark can connect to UC but fails to access S3 data

**Solutions**:
```bash
# Verify the bucket path in server.properties matches table location
# Table location: s3://unity-catalog-storage/tables/...
# server.properties must have: s3.bucketPath.0=unity-catalog-storage

# Check UC server logs for credential vending errors
docker compose -f docker-compose.local.yml logs unity-catalog | grep -i "credential\|s3\|storage"

# Verify Spark is NOT configured with hardcoded S3 credentials
# The spark-defaults.conf should NOT have:
# - spark.hadoop.fs.s3a.access.key
# - spark.hadoop.fs.s3a.secret.key
# Only the filesystem impl should be configured
```

### Issue: Delta table not visible in Unity Catalog

**Symptoms**: Table exists in MinIO but not in UC UI

**Solutions**:
```bash
# Manually register the table via Spark
docker compose -f docker-compose.local.yml exec spark pyspark --master local[*]

>>> spark.sql("""
...     CREATE TABLE unity.test_catalog.test_schema.employees
...     USING DELTA
...     LOCATION 's3://unity-catalog-storage/tables/test_catalog/test_schema/employees'
... """)

# Verify table metadata via API
curl "http://localhost:8080/api/2.1/unity-catalog/tables/test_catalog.test_schema.employees"
```

---

## Cleanup

```bash
# Stop all services
docker compose -f docker-compose.local.yml down

# Remove all data volumes
docker compose -f docker-compose.local.yml down -v

# Remove built images
docker compose -f docker-compose.local.yml down --rmi local

# Full cleanup including networks
docker compose -f docker-compose.local.yml down -v --rmi local --remove-orphans
```

---

## Notes for Claude Agents

1. **Source Build Priority**:
    - Always build Unity Catalog server AND Spark connector from source in the current directory
    - Do not use pre-built images from Docker Hub or Maven artifacts unless explicitly requested
    - The custom distribution has additional properties (`s3.endpoint`, `s3.pathStyleAccess`) not in official release

2. **Building unitycatalog-spark (CRITICAL)**:
    - Must run `./build/sbt spark/package` BEFORE Docker build
    - Copy JAR to `docker/spark/jars/` before running `docker compose build`
    - The Spark Dockerfile expects the JAR in the build context, not downloaded from Maven
    - Build tools (Java 17, SBT, Scala) must be available in the environment

3. **Version Compatibility**:
    - Unity Catalog version is in `version.sbt`
    - Ensure Spark 3.5.x, Delta 3.2.x, and UC Spark connector versions align
    - JDK 17 is required for building
    - Use `apache/spark:3.5.7-python3` as the Spark base image

4. **Credential Vending (CRITICAL)**:
    - S3 credentials must be in Unity Catalog's `server.properties`, NOT in Spark config
    - Spark only needs the S3A FileSystem implementation (`spark.hadoop.fs.s3.impl`)
    - UC server vends temporary credentials to Spark when accessing tables
    - The bucket path in `server.properties` must match table storage locations

5. **MinIO-Specific Configuration**:
    - Custom properties required in `server.properties`:
        - `s3.endpoint.0=http://minio:9000`
        - `s3.pathStyleAccess.0=true`
    - These are custom distribution properties not in standard UC OSS

6. **Network Configuration**:
    - All services must be on `uc-network` for inter-container DNS resolution
    - Use container hostnames (`unity-catalog`, `minio`, `spark`) not `localhost` within containers

7. **Health Checks**:
    - Unity Catalog may take 60+ seconds to start due to SBT compilation
    - Use `start_period` in healthcheck configuration
    - Always wait for healthcheck before runnin
    - g tests

8. **Catalog/Schema Creation**:
    - Must create catalog and schema before registering tables
    - Use the setup script or API calls before running Spark jobs

9. **UI Build**:
    - The UI is a React app requiring Node.js and Yarn
    - The proxy configuration must point to the UC server for API calls

10. **Common Pitfalls**:
- **Forgetting to build unitycatalog-spark from source** - Must run `./build/sbt spark/package` before Docker build
- **Not copying JAR to Docker build context** - JAR must be in `docker/spark/jars/`
- Forgetting to configure S3 credentials in `server.properties`
- Adding S3 credentials directly to Spark config (breaks credential vending)
- Forgetting `s3.endpoint.0` and `s3.pathStyleAccess.0` for MinIO
- Using `localhost` instead of container names within Docker network
- Not waiting for UC server healthcheck before running tests
- Forgetting to restart UC server after modifying `server.properties`