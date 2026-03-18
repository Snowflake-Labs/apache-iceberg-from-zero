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

import os
import sys

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

# Write output
try:
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(content)
    print(f"✅ Generated {output_path} with values from environment")
except Exception as e:
    print(f"❌ Failed to write output file: {e}")
    sys.exit(1)


