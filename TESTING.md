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
