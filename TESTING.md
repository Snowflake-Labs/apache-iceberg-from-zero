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

## Requirements

- Docker running with `jupyter-spark` container
- Python packages: pytest, nbconvert, nbformat (installed via requirements-test.txt)
