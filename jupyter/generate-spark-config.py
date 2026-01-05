#!/usr/bin/env python3
import os

# Read template
with open('/tmp/spark-defaults.conf.template', 'r') as f:
    content = f.read()

# Substitute environment variables
for key in ['ICEBERG_VERSION', 'ICEBERG_SPARK_RUNTIME_VERSION', 'SCALA_VERSION', 'AWS_SDK_VERSION', 'POLARIS_CLIENT_ID', 'POLARIS_CLIENT_SECRET']:
    content = content.replace(f'${{{key}}}', os.environ.get(key, ''))

# Write output
with open('/home/jovyan/.sparkconf/spark-defaults.conf', 'w') as f:
    f.write(content)

print("✅ Generated spark-defaults.conf with values from environment")
