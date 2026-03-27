#!/usr/bin/env bash
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

# Pre-download Spark/Iceberg dependency JARs from Maven Central for offline use.
#
# Run this script if your environment cannot download JARs at runtime (e.g.
# corporate proxies that intercept HTTPS). The downloaded JARs are placed in
# ./jars/ and automatically mounted into the Docker container on next startup.
#
# Usage:
#   ./manual-download-dependencies.sh              # normal HTTPS download
#   ./manual-download-dependencies.sh --insecure   # skip SSL verification

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"
JARS_DIR="${SCRIPT_DIR}/jars"
MAVEN_CENTRAL="https://repo1.maven.org/maven2"

CURL_OPTS=("-fSL" "--retry" "3" "--retry-delay" "2")

# Transitive dependency of hadoop-aws — must match the version pinned in Hadoop 3.4.x
AWS_SDK_V1_VERSION="1.12.367"
# hadoop-aws version (keep in sync with spark-defaults.conf.template)
HADOOP_AWS_VERSION="3.4.1"

usage() {
    echo "Usage: $0 [--insecure]"
    echo ""
    echo "Options:"
    echo "  --insecure   Skip SSL certificate verification (for corporate proxies)"
    echo ""
    echo "Downloads Spark/Iceberg JARs from Maven Central into ./jars/"
    echo "so that Docker containers can use them without network access."
    exit 1
}

for arg in "$@"; do
    case "$arg" in
        --insecure)
            CURL_OPTS+=("-k")
            echo "WARNING: SSL certificate verification disabled (--insecure)"
            ;;
        --help|-h)
            usage
            ;;
        *)
            echo "Unknown argument: $arg"
            usage
            ;;
    esac
done

if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: .env file not found at ${ENV_FILE}"
    exit 1
fi

# Source version variables from .env (skip comments and blank lines)
while IFS='=' read -r key value; do
    case "$key" in
        \#*|"") continue ;;
    esac
    # Trim whitespace
    key="$(echo "$key" | xargs)"
    value="$(echo "$value" | xargs)"
    export "$key"="$value"
done < "$ENV_FILE"

for var in ICEBERG_VERSION ICEBERG_SPARK_RUNTIME_VERSION SCALA_VERSION AWS_SDK_VERSION; do
    if [ -z "${!var:-}" ]; then
        echo "ERROR: Required variable $var not set in .env"
        exit 1
    fi
done

echo "=== Dependency versions ==="
echo "  ICEBERG_VERSION:                ${ICEBERG_VERSION}"
echo "  ICEBERG_SPARK_RUNTIME_VERSION:  ${ICEBERG_SPARK_RUNTIME_VERSION}"
echo "  SCALA_VERSION:                  ${SCALA_VERSION}"
echo "  AWS_SDK_VERSION:                ${AWS_SDK_VERSION}"
echo "  HADOOP_AWS_VERSION:             ${HADOOP_AWS_VERSION}"
echo "  AWS_SDK_V1_VERSION:             ${AWS_SDK_V1_VERSION}"
echo ""

# Build the list of (group, artifact, version) tuples.
# group uses '.' separators; we convert to '/' for the Maven URL path.
JARS=(
    "org.apache.iceberg  iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME_VERSION}_${SCALA_VERSION}  ${ICEBERG_VERSION}"
    "org.apache.iceberg  iceberg-aws-bundle  ${ICEBERG_VERSION}"
    "software.amazon.awssdk  bundle  ${AWS_SDK_VERSION}"
    "software.amazon.awssdk  url-connection-client  ${AWS_SDK_VERSION}"
    "org.apache.hadoop  hadoop-aws  ${HADOOP_AWS_VERSION}"
    "com.amazonaws  aws-java-sdk-bundle  ${AWS_SDK_V1_VERSION}"
)

echo "=== JARs to download ==="
for entry in "${JARS[@]}"; do
    read -r group artifact version <<< "$entry"
    group_path="${group//./\/}"
    echo "  ${MAVEN_CENTRAL}/${group_path}/${artifact}/${version}/${artifact}-${version}.jar"
done
echo ""

mkdir -p "$JARS_DIR"

download_jar() {
    local group="$1" artifact="$2" version="$3"
    local group_path="${group//./\/}"
    local filename="${artifact}-${version}.jar"
    local url="${MAVEN_CENTRAL}/${group_path}/${artifact}/${version}/${filename}"
    local dest="${JARS_DIR}/${filename}"

    if [ -f "$dest" ]; then
        echo "  SKIP  ${filename} (already exists)"
        return 0
    fi

    echo "  GET   ${filename}"
    if ! curl "${CURL_OPTS[@]}" -o "$dest" "$url"; then
        echo "  FAIL  Could not download ${url}"
        rm -f "$dest"
        return 1
    fi
}

echo "=== Downloading JARs to ${JARS_DIR} ==="
failures=0
for entry in "${JARS[@]}"; do
    read -r group artifact version <<< "$entry"
    if ! download_jar "$group" "$artifact" "$version"; then
        failures=$((failures + 1))
    fi
done

echo ""
jar_count=$(find "$JARS_DIR" -name '*.jar' | wc -l | tr -d ' ')
echo "=== Done: ${jar_count} JAR(s) in ${JARS_DIR} ==="

if [ "$failures" -gt 0 ]; then
    echo "WARNING: ${failures} download(s) failed. Check output above."
    exit 1
fi

echo ""
echo "Next steps:"
echo "  1. Run:  docker compose up -d"
echo "  2. The JARs will be mounted into the container automatically."
echo "     Spark will use local JARs instead of downloading from Maven Central."
