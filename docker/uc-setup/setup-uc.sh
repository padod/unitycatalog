#!/bin/sh
set -e

UC_HOST="http://unitycatalog:8080"
API_BASE="${UC_HOST}/api/2.1/unity-catalog"

echo "========================================"
echo "Setting up Unity Catalog for MinIO test"
echo "========================================"

# Wait for UC to be fully ready
sleep 5

echo ""
echo "[1] Creating storage credential for MinIO..."
# Create a storage credential with S3 compatible settings
curl -s -X POST "${API_BASE}/storage-credentials" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "minio-cred",
    "comment": "Storage credential for MinIO S3-compatible storage",
    "aws_iam_role": null,
    "aws_access_key": {
      "access_key_id": "minioadmin",
      "secret_access_key": "minioadmin"
    }
  }' | head -c 500
echo ""

echo ""
echo "[2] Creating external location pointing to MinIO..."
# Create external location
curl -s -X POST "${API_BASE}/external-locations" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "minio-location",
    "url": "s3a://unity-catalog/",
    "credential_name": "minio-cred",
    "comment": "External location for MinIO bucket"
  }' | head -c 500
echo ""

echo ""
echo "[3] Creating catalog 'minio_test'..."
# Create catalog
curl -s -X POST "${API_BASE}/catalogs" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "minio_test",
    "comment": "Test catalog for MinIO integration"
  }' | head -c 500
echo ""

echo ""
echo "[4] Creating schema 'minio_test.default'..."
# Create schema
curl -s -X POST "${API_BASE}/schemas" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "default",
    "catalog_name": "minio_test",
    "comment": "Default schema for MinIO tests"
  }' | head -c 500
echo ""

echo ""
echo "[5] Creating external table 'minio_test.default.products'..."
# Create external table pointing to MinIO location
curl -s -X POST "${API_BASE}/tables" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "products",
    "catalog_name": "minio_test",
    "schema_name": "default",
    "table_type": "EXTERNAL",
    "data_source_format": "DELTA",
    "storage_location": "s3a://unity-catalog/tables/products",
    "columns": [
      {"name": "product_id", "type_name": "INT", "type_text": "int", "type_json": "{\"name\":\"product_id\",\"type\":\"integer\",\"nullable\":false}", "position": 0, "nullable": false},
      {"name": "product_name", "type_name": "STRING", "type_text": "string", "type_json": "{\"name\":\"product_name\",\"type\":\"string\",\"nullable\":false}", "position": 1, "nullable": false},
      {"name": "category", "type_name": "STRING", "type_text": "string", "type_json": "{\"name\":\"category\",\"type\":\"string\",\"nullable\":true}", "position": 2, "nullable": true},
      {"name": "price", "type_name": "DOUBLE", "type_text": "double", "type_json": "{\"name\":\"price\",\"type\":\"double\",\"nullable\":true}", "position": 3, "nullable": true},
      {"name": "quantity", "type_name": "INT", "type_text": "int", "type_json": "{\"name\":\"quantity\",\"type\":\"integer\",\"nullable\":true}", "position": 4, "nullable": true}
    ],
    "comment": "Product catalog stored in MinIO"
  }' | head -c 500
echo ""

echo ""
echo "[6] Verifying setup..."
echo "Catalogs:"
curl -s "${API_BASE}/catalogs" | head -c 300
echo ""
echo ""
echo "Tables in minio_test.default:"
curl -s "${API_BASE}/tables?catalog_name=minio_test&schema_name=default" | head -c 500
echo ""

echo ""
echo "========================================"
echo "Unity Catalog setup completed!"
echo "========================================"