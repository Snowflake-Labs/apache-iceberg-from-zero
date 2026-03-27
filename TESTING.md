<!-- Copyright 2026 Snowflake Inc.
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. -->

# Notebook Testing

Automated test suite that runs all notebooks in Docker and checks for errors and empty SELECT queries.

## Test Execution Order

**Important:** The test suite automatically runs notebooks in the correct order:

1. **Setup.ipynb** runs FIRST (prerequisite - downloads data and initializes environment)
2. **All other notebooks** run independently after Setup completes

Each video notebook (Module1, Module2, Module3) can run completely independently after Setup.

## Quick Start

```bash
# Install dependencies
pip install -r requirements-test.txt

# Run all notebooks tests (Setup runs first automatically)
pytest test_notebooks.py -v
```

## Other Ways to Run

```bash
# Test a specific notebook
pytest test_notebooks.py -k "Module1"

# Run in parallel (faster)
pytest test_notebooks.py -n auto

# Run as standalone Python script
python test_notebooks.py
```

## What It Checks

- ✅ All cells execute without errors
- ✅ SELECT queries return data rows (not empty)
- ✅ No Python exceptions or broken cells

## Testing Offline JAR Mode

The offline JAR mode allows Spark to use pre-downloaded JARs instead of fetching them from Maven Central at runtime. This is tested by a dedicated CI workflow (`.github/workflows/test-offline-jars.yml`) and can also be run locally.

### Running Locally

```bash
# 1. Download JARs (use --insecure if behind a corporate proxy)
./manual-download-dependencies.sh

# 2. Start services (JARs are auto-mounted from ./jars/)
docker compose up -d

# 3. Verify the config uses local JARs
docker exec jupyter-spark grep "^spark.jars=" /home/jovyan/.sparkconf/spark-defaults.conf

# 4. Run a notebook test to confirm everything works
pytest test_notebooks.py -v -k "test_01_setup or E1_2_DataModeling" --tb=short

# 5. Clean up
docker compose down -v
rm -rf jars/
```

### What the CI Workflow Checks

1. `manual-download-dependencies.sh` downloads all expected JARs
2. The generated `spark-defaults.conf` uses `spark.jars=` (local paths) instead of `spark.jars.packages=`
3. A notebook runs successfully with Spark loading from local JARs

## Requirements

- Docker running with `jupyter-spark` container
- Python packages: pytest, nbconvert, nbformat (installed via requirements-test.txt)
