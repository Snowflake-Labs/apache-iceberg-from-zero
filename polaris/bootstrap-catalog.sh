#!/bin/sh
# Copyright 2026 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

echo "=========================================="
echo "🔧 Bootstrapping Polaris (Internal Endpoint)"
echo "=========================================="

# Get OAuth token
echo "🔐 Getting OAuth token..."
TOKEN=$(curl -s http://polaris:8181/api/catalog/v1/oauth/tokens \
  --user ${CLIENT_ID}:${CLIENT_SECRET} \
  -H "Polaris-Realm: POLARIS" \
  -d grant_type=client_credentials \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)

if [ -z "${TOKEN}" ] || [ "${TOKEN}" == "null" ]; then
  echo "❌ Failed to obtain access token"
  exit 1
fi

echo "✅ Got token"

# Create catalog with INTERNAL endpoint only (for Docker network)
# Use the internal MinIO endpoint since all services run in Docker
echo ""
echo "📚 Creating Polaris catalog..."

STORAGE_CONFIG='{"storageType":"S3","endpoint":"http://minio:9000","pathStyleAccess":true,"allowedLocations":["s3://warehouse"]}'
STORAGE_LOCATION='s3://warehouse'

PAYLOAD='{
   "catalog": {
     "name": "quickstart_catalog",
     "type": "INTERNAL",
     "readOnly": false,
     "properties": {
       "default-base-location": "'$STORAGE_LOCATION'"
     },
     "storageConfigInfo": '$STORAGE_CONFIG'
   }
 }'

curl -s -H "Authorization: Bearer ${TOKEN}" \
   -H 'Accept: application/json' \
   -H 'Content-Type: application/json' \
   -H "Polaris-Realm: POLARIS" \
   http://polaris:8181/api/management/v1/catalogs \
   -d "$PAYLOAD" | jq .

echo ""
echo "🔑 Granting CATALOG_MANAGE_CONTENT to catalog_admin..."
curl -s -H "Authorization: Bearer ${TOKEN}" \
  -H 'Content-Type: application/json' \
  -H "Polaris-Realm: POLARIS" \
  -X PUT \
  http://polaris:8181/api/management/v1/catalogs/quickstart_catalog/catalog-roles/catalog_admin/grants \
  -d '{"type":"catalog", "privilege":"CATALOG_MANAGE_CONTENT"}' | jq .

echo ""
echo "=========================================="
echo "✅ Bootstrap Complete!"
echo "=========================================="
echo ""
echo "Configuration:"
echo "  - Catalog: quickstart_catalog"
echo "  - Storage: MinIO S3 (INTERNAL endpoint)"
echo "  - Endpoint: http://minio:9000 (Docker network)"
echo ""
echo "Credentials:"
echo "  Client ID: root"
echo "  Client Secret: s3cr3t"
echo ""
echo "MinIO Console: http://localhost:9001"
echo "  Username: admin"
echo "  Password: password"
