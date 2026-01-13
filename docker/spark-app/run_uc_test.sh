#!/bin/bash
set -e

echo '========================================'
echo 'Installing Python dependencies...'
echo '========================================'
pip3 install delta-spark==3.2.0

echo '========================================'
echo 'Waiting for Unity Catalog to be ready...'
echo '========================================'
sleep 5

echo '========================================'
echo 'Running Delta Table Test via Unity Catalog...'
echo '========================================'

# Find the Unity Catalog Spark connector JAR
UC_JAR=$(find /opt/uc-jars -name "unitycatalog-spark*.jar" 2>/dev/null | head -1)
if [ -z "$UC_JAR" ]; then
    echo "Warning: Unity Catalog Spark connector JAR not found in /opt/uc-jars"
    echo "Available files:"
    ls -la /opt/uc-jars/ 2>/dev/null || echo "Directory not found"
    UC_JAR=""
fi

# Build the jars parameter
EXTRA_JARS=""
if [ -n "$UC_JAR" ]; then
    EXTRA_JARS="--jars $UC_JAR"
    echo "Using Unity Catalog JAR: $UC_JAR"
fi

/opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  $EXTRA_JARS \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalog.unity=io.unitycatalog.spark.UCSingleCatalog \
  --conf spark.sql.catalog.unity.uri=http://unitycatalog:8080 \
  --conf spark.sql.catalog.unity.token= \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  /opt/spark-app/test_uc_delta.py