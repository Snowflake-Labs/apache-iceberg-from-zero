#!/usr/bin/env python3
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

import glob
import os
import re
import sys
import urllib.request

LOCAL_JARS_DIR = '/opt/spark-jars'
MAVEN_CENTRAL_HOST = 'repo1.maven.org'

if len(sys.argv) < 2:
    print("Usage: generate-spark-config.py <output_path>")
    sys.exit(1)

output_path = sys.argv[1]

# Read template
template_path = '/tmp/spark-defaults.conf.template'
try:
    with open(template_path, 'r') as f:
        content = f.read()
except FileNotFoundError:
    print(f"❌ Template file not found: {template_path}")
    sys.exit(1)

# Substitute environment variables
env_vars = [
    'ICEBERG_VERSION',
    'ICEBERG_SPARK_RUNTIME_VERSION',
    'SCALA_VERSION',
    'AWS_SDK_VERSION',
    'POLARIS_CLIENT_ID',
    'POLARIS_CLIENT_SECRET'
]

for key in env_vars:
    value = os.environ.get(key, '')
    if not value:
        print(f"⚠️  Warning: Environment variable {key} is not set")
    content = content.replace(f'${{{key}}}', value)


def _find_local_jars():
    """Return sorted list of .jar paths in LOCAL_JARS_DIR, or empty list."""
    if not os.path.isdir(LOCAL_JARS_DIR):
        return []
    jars = sorted(glob.glob(os.path.join(LOCAL_JARS_DIR, '*.jar')))
    return jars


def _check_maven_connectivity():
    """Quick HTTPS check to Maven Central. Returns True if reachable."""
    try:
        urllib.request.urlopen(
            f'https://{MAVEN_CENTRAL_HOST}/', timeout=5
        )
        return True
    except Exception:
        return False


local_jars = _find_local_jars()

if local_jars:
    jar_list = ','.join(local_jars)
    content = re.sub(
        r'^spark\.jars\.packages=.*$',
        f'spark.jars={jar_list}',
        content,
        flags=re.MULTILINE,
    )
    print(f"✅ Using {len(local_jars)} pre-downloaded JAR(s) from {LOCAL_JARS_DIR}")
    for jar in local_jars:
        print(f"   {os.path.basename(jar)}")
else:
    if not _check_maven_connectivity():
        print("⚠️  WARNING: Cannot reach Maven Central (https://repo1.maven.org).")
        print("   Spark will likely fail to download dependency JARs.")
        print("")
        print("   If you are behind a corporate proxy or firewall, run this on")
        print("   your host machine to pre-download the JARs:")
        print("")
        print("     ./manual-download-dependencies.sh --insecure")
        print("")
        print("   Then restart with: docker compose down && docker compose up -d")
        print("")

# Write output
try:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(content)
    print(f"✅ Generated {output_path} with values from environment")
except Exception as e:
    print(f"❌ Failed to write output file: {e}")
    sys.exit(1)


