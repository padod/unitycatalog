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
        echo "      Unity Catalog server is ready"
        break
    fi
    attempt=$((attempt + 1))
    echo "      Waiting... (attempt $attempt/$max_attempts)"
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    echo "      Failed to connect to Unity Catalog server"
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
    echo "      Catalog 'test_catalog' created successfully"
elif [ "$http_code" = "409" ]; then
    echo "      Catalog 'test_catalog' already exists"
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
    echo "      Schema 'test_schema' created successfully"
elif [ "$http_code" = "409" ]; then
    echo "      Schema 'test_schema' already exists"
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