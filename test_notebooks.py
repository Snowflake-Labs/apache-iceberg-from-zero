"""
Test suite for Jupyter notebooks in the FirstStepsIceberg project.

This script executes all notebooks in their Docker environment and validates:
1. All cells execute without errors
2. SQL SELECT queries produce output with actual data rows (not empty results)
3. No broken cells or exceptions

Usage:
    # Run all tests
    python test_notebooks.py

    # Run with pytest
    pytest test_notebooks.py -v

    # Run specific notebook
    pytest test_notebooks.py -v -k "Module1"
"""

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pytest


class NotebookTestResult:
    """Container for notebook test results."""

    def __init__(self, notebook_path: str):
        self.notebook_path = notebook_path
        self.total_cells = 0
        self.code_cells = 0
        self.errors: List[Dict] = []
        self.empty_select_queries: List[Dict] = []
        self.execution_time = 0
        self.passed = True

    def add_error(self, cell_index: int, cell_source: str, error_msg: str):
        """Record a cell execution error."""
        self.errors.append({
            "cell_index": cell_index,
            "source": cell_source[:500],
            "error": error_msg
        })
        self.passed = False

    def add_empty_select(self, cell_index: int, cell_source: str):
        """Record a SELECT query that returned no rows."""
        self.empty_select_queries.append({
            "cell_index": cell_index,
            "source": cell_source[:500]
        })
        self.passed = False

    def get_summary(self) -> str:
        """Get a human-readable summary of test results."""
        summary = [f"\n{'='*80}"]
        summary.append(f"Notebook: {self.notebook_path}")
        summary.append(f"Total cells: {self.total_cells}, Code cells: {self.code_cells}")
        
        if self.passed:
            summary.append("✓ ALL TESTS PASSED")
        else:
            summary.append("✗ TESTS FAILED")
            
            if self.errors:
                summary.append(f"\n✗ {len(self.errors)} cell(s) with errors:")
                for err in self.errors:
                    summary.append(f"  Cell {err['cell_index']}:")
                    summary.append(f"    Source: {err['source']}")
                    summary.append(f"    Error: {err['error']}")
            
            if self.empty_select_queries:
                summary.append(f"\n✗ {len(self.empty_select_queries)} SELECT query(ies) with no rows returned:")
                for query in self.empty_select_queries:
                    summary.append(f"  Cell {query['cell_index']}:")
                    summary.append(f"    Source: {query['source']}")
                    summary.append(f"    Issue: Query executed but returned no rows")
        
        summary.append('='*80)
        return '\n'.join(summary)


class NotebookTester:
    """Test runner for Jupyter notebooks."""

    def __init__(self, container_name: str = "jupyter-spark"):
        self.container_name = container_name
        self.notebooks_dir = Path(__file__).parent / "notebooks"

    def is_container_running(self) -> bool:
        """Check if the Jupyter container is running."""
        try:
            result = subprocess.run(
                ["docker", "ps", "--filter", f"name={self.container_name}", "--format", "{{.Names}}"],
                capture_output=True,
                text=True,
                check=True
            )
            return self.container_name in result.stdout
        except subprocess.CalledProcessError:
            return False

    def execute_notebook_in_container(self, notebook_path: Path) -> Tuple[bool, str]:
        """
        Execute a notebook inside the Docker container using nbconvert.
        
        Returns:
            Tuple of (success, output_path or error_message)
        """
        # Convert to container path
        container_path = f"/home/jovyan/work/{notebook_path.relative_to(self.notebooks_dir)}"
        output_path = str(notebook_path).replace('.ipynb', '_executed.ipynb')
        container_output = f"/home/jovyan/work/{Path(output_path).relative_to(self.notebooks_dir)}"
        
        # Execute notebook using nbconvert with timeout
        # Set up Spark environment variables for proper PySpark execution
        cmd = [
            "docker", "exec",
            "-e", "PYTHONPATH=/usr/local/spark/python/lib/pyspark.zip:/usr/local/spark/python/lib/py4j-0.10.9.9-src.zip",
            "-e", f"SPARK_HOME=/usr/local/spark",
            self.container_name,
            "jupyter", "nbconvert",
            "--to", "notebook",
            "--execute",
            "--ExecutePreprocessor.timeout=600",  # 10 minute timeout per cell
            "--ExecutePreprocessor.kernel_name=python3",
            "--output", container_output,
            container_path
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour total timeout
            )
            
            if result.returncode == 0:
                return True, output_path
            else:
                error_msg = f"Execution failed:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
                return False, error_msg
                
        except subprocess.TimeoutExpired:
            return False, "Notebook execution timed out (1 hour limit)"
        except Exception as e:
            return False, f"Execution error: {str(e)}"

    def analyze_executed_notebook(self, executed_notebook_path: str) -> NotebookTestResult:
        """
        Analyze an executed notebook for errors and empty SELECT queries.
        
        Args:
            executed_notebook_path: Path to the executed notebook
            
        Returns:
            NotebookTestResult with analysis results
        """
        result = NotebookTestResult(executed_notebook_path)
        
        try:
            with open(executed_notebook_path, 'r', encoding='utf-8') as f:
                notebook = json.load(f)
            
            cells = notebook.get('cells', [])
            result.total_cells = len(cells)
            
            for idx, cell in enumerate(cells):
                if cell.get('cell_type') != 'code':
                    continue
                
                result.code_cells += 1
                source = ''.join(cell.get('source', []))
                outputs = cell.get('outputs', [])
                
                # Check for execution errors
                for output in outputs:
                    if output.get('output_type') == 'error':
                        error_msg = '\n'.join(output.get('traceback', []))
                        result.add_error(idx, source, error_msg)
                
                # Check for SELECT queries with no output
                if self._has_select_query(source):
                    if not self._has_meaningful_output(outputs):
                        result.add_empty_select(idx, source)
            
        except Exception as e:
            result.add_error(-1, "Notebook analysis", f"Failed to analyze notebook: {str(e)}")
        
        return result

    def print_cell_report(self, executed_notebook_path: str, result: NotebookTestResult):
        """Print a cell-by-cell execution report for CI log visibility."""
        try:
            with open(executed_notebook_path, 'r', encoding='utf-8') as f:
                notebook = json.load(f)
        except Exception:
            return

        error_cells = {e['cell_index'] for e in result.errors}
        empty_cells = {e['cell_index'] for e in result.empty_select_queries}

        cells = notebook.get('cells', [])
        print(f"\n{'─'*80}")
        print(f"CELL REPORT: {Path(executed_notebook_path).stem.replace('_executed', '')}")
        print(f"{'─'*80}")

        for idx, cell in enumerate(cells):
            if cell.get('cell_type') != 'code':
                continue

            source = ''.join(cell.get('source', []))
            first_line = source.strip().split('\n')[0][:80] if source.strip() else '(empty)'
            outputs = cell.get('outputs', [])

            if idx in error_cells:
                status = "FAIL"
            elif idx in empty_cells:
                status = "EMPTY"
            else:
                status = "OK"

            output_preview = ""
            for out in outputs:
                if out.get('output_type') == 'stream':
                    text = out.get('text', '')
                    if isinstance(text, list):
                        text = ''.join(text)
                    lines = text.strip().split('\n')
                    if lines and lines[0].strip():
                        output_preview = lines[0].strip()[:60]
                        break
                elif out.get('output_type') in ('execute_result', 'display_data'):
                    text = out.get('data', {}).get('text/plain', '')
                    if isinstance(text, list):
                        text = ''.join(text)
                    if text.strip():
                        output_preview = text.strip().split('\n')[0][:60]
                        break
                elif out.get('output_type') == 'error':
                    ename = out.get('ename', 'Error')
                    evalue = out.get('evalue', '')[:60]
                    output_preview = f"{ename}: {evalue}"
                    break

            line = f"  Cell {idx:3d} [{status:5s}]  {first_line}"
            if output_preview:
                line += f"\n{'':19s}-> {output_preview}"
            print(line)

        print(f"{'─'*80}\n")

    def _has_select_query(self, source: str) -> bool:
        """
        Check if cell source contains a SELECT query (not DDL/DML).
        Only returns True for actual SELECT queries that should return rows.
        """
        # Strip comment-only lines so we don't match patterns in commented-out code
        active_lines = [line for line in source.splitlines() if not line.strip().startswith('#')]
        source = '\n'.join(active_lines)
        source_upper = source.upper()
        
        # Exclude DML/DDL statements that don't return rows
        if any(stmt in source_upper for stmt in [
            'CREATE TABLE',
            'DROP TABLE',
            'ALTER TABLE',
            'INSERT INTO',
            'INSERT OVERWRITE',
            'UPDATE ',
            'DELETE FROM',
            'TRUNCATE',
            'MERGE INTO'
        ]):
            # These statements might contain SELECT but don't return rows
            return False
        
        # Look for SQL SELECT statements (case insensitive)
        # Handles both spark.sql() and %%sql magic commands
        patterns = [
            r'spark\.sql\s*\(\s*["\'].*?SELECT\s+',
            r'%%sql\s+SELECT\s+',
            r'\.sql\s*\(\s*["\'].*?SELECT\s+',
        ]
        
        for pattern in patterns:
            if re.search(pattern, source, re.IGNORECASE | re.DOTALL):
                # Only check for output if the query result is being displayed
                # Look for .show(), .display(), or standalone spark.sql() without assignment
                if '.show()' in source or '.display()' in source:
                    return True
                    
                # Check if it's a standalone spark.sql() (not assigned to a variable)
                # This pattern means the result should be displayed
                if re.search(r'spark\.sql\s*\([^)]+\)\s*$', source.strip(), re.MULTILINE):
                    return True
                
                # If it's assigned to a variable without show/display, it's not meant to display
                # e.g., df = spark.sql("SELECT...")
                if re.search(r'\w+\s*=\s*spark\.sql', source):
                    return False
                    
        return False

    def _has_meaningful_output(self, outputs: List[Dict]) -> bool:
        """
        Check if outputs contain meaningful data with actual rows.
        For SELECT queries, this verifies that rows were returned, not just empty results.
        """
        if not outputs:
            return False
        
        for output in outputs:
            output_type = output.get('output_type')
            
            # Check execute_result or display_data
            if output_type in ['execute_result', 'display_data']:
                data = output.get('data', {})
                
                # Check for HTML tables (DataFrame display)
                html_output = data.get('text/html', '')
                if html_output and '<table' in html_output:
                    # Check if table has data rows (not just headers)
                    # Look for <tbody> with <tr> elements (data rows)
                    if '<tbody>' in html_output and html_output.count('<tr>') > 1:
                        return True
                    # Some DataFrames show as simple tables without tbody
                    # Check if there are multiple rows (header + data)
                    if '<tr>' in html_output:
                        row_count = html_output.count('<tr>')
                        # More than 1 row means we have header + at least one data row
                        if row_count > 1:
                            return True
                
                # Check for text output (show() output or DataFrame repr)
                text_output = data.get('text/plain', '')
                # Handle both string and list types
                if isinstance(text_output, list):
                    text_output = ''.join(text_output)
                
                if text_output and text_output.strip():
                    # Exclude empty results
                    if text_output.strip() in ['None', '[]', '{}', '', 'DataFrame[]', 'Empty DataFrame']:
                        continue
                    
                    # Check for DataFrame string representation with rows
                    # Look for patterns like:
                    # +---+---+   (Spark DataFrame show() format)
                    # |id |name|
                    # +---+---+
                    # |1  |foo |
                    if '|' in text_output and '+' in text_output:
                        lines = text_output.strip().split('\n')
                        # Count lines that look like data (contain |)
                        data_lines = [l for l in lines if '|' in l and not all(c in '+- |' for c in l)]
                        # More than 1 data line means header + at least one row
                        if len(data_lines) > 1:
                            return True
                    
                    # Check for "empty" indicators
                    empty_indicators = [
                        'empty dataframe',
                        '0 rows',
                        'no rows',
                        'dataframe[]',
                    ]
                    text_lower = text_output.lower()
                    if any(indicator in text_lower for indicator in empty_indicators):
                        continue
                    
                    # If we have substantial text output that's not an empty indicator, consider it meaningful
                    if len(text_output.strip()) > 20:
                        return True
            
            # Check for stream output (print statements from show())
            if output_type == 'stream':
                text = output.get('text', '')
                # Handle both string and list types
                if isinstance(text, list):
                    text = ''.join(text)
                
                if text and text.strip():
                    # Check if it's not just None or empty containers
                    if text.strip() in ['None', '[]', '{}']:
                        continue
                    
                    # Check for DataFrame show() output with rows
                    if '|' in text and '+' in text:
                        lines = text.strip().split('\n')
                        # Count lines that look like data rows
                        data_lines = [l for l in lines if '|' in l and not all(c in '+- |' for c in l)]
                        if len(data_lines) > 1:  # Header + at least one row
                            return True
                    
                    # Check for empty indicators
                    if 'empty' in text.lower() or '0 rows' in text.lower():
                        continue
                    
                    # Any other non-trivial stream output is considered meaningful
                    if len(text.strip()) > 10:
                        return True
        
        return False

    def cleanup_kernel_and_spark(self):
        """
        Clean up Jupyter kernels and Spark sessions after notebook execution.
        This prevents connection issues when running multiple notebooks sequentially.
        """
        try:
            # Kill any lingering Python kernel processes
            subprocess.run(
                ["docker", "exec", self.container_name, "pkill", "-f", "ipykernel_launcher"],
                capture_output=True,
                timeout=10
            )
            
            # Give Spark a moment to clean up
            import time
            time.sleep(2)
            
            # Force cleanup of any Spark contexts
            subprocess.run(
                ["docker", "exec", self.container_name, "bash", "-c", 
                 "rm -rf /tmp/spark-* 2>/dev/null || true"],
                capture_output=True,
                timeout=10
            )
        except Exception as e:
            # Non-fatal - just log and continue
            print(f"Warning: Cleanup encountered issue: {e}")

    def test_notebook(self, notebook_path: Path) -> NotebookTestResult:
        """
        Execute and test a single notebook.
        
        Args:
            notebook_path: Path to the notebook file
            
        Returns:
            NotebookTestResult with test results
        """
        print(f"\nTesting notebook: {notebook_path.name}")
        
        # Execute notebook in container
        success, output_or_error = self.execute_notebook_in_container(notebook_path)
        
        if not success:
            result = NotebookTestResult(str(notebook_path))
            result.add_error(0, "Notebook execution", output_or_error)
            # Clean up even on failure
            self.cleanup_kernel_and_spark()
            return result
        
        # Analyze executed notebook
        result = self.analyze_executed_notebook(output_or_error)
        
        self.print_cell_report(output_or_error, result)
        
        # Clean up kernel and Spark session before next test
        self.cleanup_kernel_and_spark()
        
        return result

    def get_all_notebooks(self, exclude_patterns: Optional[List[str]] = None) -> List[Path]:
        """
        Get all notebook files, optionally excluding some patterns.
        
        Args:
            exclude_patterns: List of patterns to exclude (e.g., ['checkpoint', 'Setup'])
            
        Returns:
            List of Path objects for notebooks
        """
        if exclude_patterns is None:
            exclude_patterns = ['.ipynb_checkpoints', '_executed']
        
        notebooks = []
        for notebook_path in self.notebooks_dir.rglob("*.ipynb"):
            # Check if any exclude pattern matches
            if any(pattern in str(notebook_path) for pattern in exclude_patterns):
                continue
            notebooks.append(notebook_path)
        
        return sorted(notebooks)


# Pytest fixtures and test functions
@pytest.fixture(scope="session")
def notebook_tester():
    """Create a NotebookTester instance."""
    tester = NotebookTester()
    
    # Check if container is running
    if not tester.is_container_running():
        pytest.exit("Jupyter container is not running. Please start it with: docker-compose up -d")
    
    return tester


@pytest.fixture(scope="session")
def all_notebooks(notebook_tester):
    """Get all notebooks to test (excluding Setup and checkpoints)."""
    # Setup notebook should be run first separately or be assumed to be already run
    return notebook_tester.get_all_notebooks(exclude_patterns=[
        '.ipynb_checkpoints',
        '_executed',
        'Setup.ipynb'  # Exclude setup as it's a prerequisite
    ])


# Parameterized test for each notebook
def pytest_generate_tests(metafunc):
    """Generate test cases for each notebook."""
    if "notebook_path" in metafunc.fixturenames:
        tester = NotebookTester()
        notebooks = tester.get_all_notebooks(exclude_patterns=[
            '.ipynb_checkpoints',
            '_executed',
            'Setup.ipynb'
        ])
        
        ids = [nb.stem.replace('.', '_').replace(' - ', '_').replace(' ', '_') for nb in notebooks]
        metafunc.parametrize("notebook_path", notebooks, ids=ids)


def test_01_setup_notebook(notebook_tester):
    """
    Test the Setup notebook first - it's a prerequisite for other notebooks.
    This MUST run before other notebook tests.
    """
    setup_notebooks = [
        nb for nb in notebook_tester.get_all_notebooks(exclude_patterns=['.ipynb_checkpoints', '_executed'])
        if 'Setup' in nb.name
    ]
    
    if not setup_notebooks:
        pytest.skip("No Setup notebook found")
    
    for setup_nb in setup_notebooks:
        print(f"\n{'='*80}")
        print(f"RUNNING PREREQUISITE: {setup_nb.name}")
        print(f"This must complete successfully before other notebooks can run.")
        print(f"{'='*80}")
        result = notebook_tester.test_notebook(setup_nb)
        print(result.get_summary())
        assert result.passed, f"Setup notebook {setup_nb.name} failed. Other notebooks will not work without this. See output above."


def test_02_notebook_execution(notebook_tester, notebook_path):
    """
    Test individual notebooks for errors and empty SELECT queries.
    Each notebook should run independently after Setup has completed.
    """
    result = notebook_tester.test_notebook(notebook_path)
    
    # Print summary
    print(result.get_summary())
    
    # Assert test passed
    assert result.passed, f"Notebook {notebook_path.name} failed tests. See output above for details."


# Standalone execution
def main():
    """Run tests as a standalone script."""
    tester = NotebookTester()
    
    # Check container
    if not tester.is_container_running():
        print("ERROR: Jupyter container is not running.")
        print("Please start it with: docker-compose up -d")
        sys.exit(1)
    
    print("="*80)
    print("NOTEBOOK TEST SUITE")
    print("="*80)
    
    # Get all notebooks
    all_notebooks = tester.get_all_notebooks()
    
    if not all_notebooks:
        print("No notebooks found to test.")
        sys.exit(0)
    
    # Separate Setup notebook from others
    setup_notebooks = [nb for nb in all_notebooks if 'Setup' in nb.name]
    other_notebooks = [nb for nb in all_notebooks if 'Setup' not in nb.name]
    
    print(f"\nFound {len(all_notebooks)} notebook(s) to test:")
    if setup_notebooks:
        print("\nPrerequisite:")
        for nb in setup_notebooks:
            print(f"  - {nb.relative_to(tester.notebooks_dir)} (MUST RUN FIRST)")
    if other_notebooks:
        print("\nIndependent notebooks:")
        for nb in other_notebooks:
            print(f"  - {nb.relative_to(tester.notebooks_dir)}")
    
    # Run Setup notebook first
    results = []
    if setup_notebooks:
        print("\n" + "="*80)
        print("RUNNING SETUP NOTEBOOK (PREREQUISITE)")
        print("="*80)
        for setup_nb in setup_notebooks:
            result = tester.test_notebook(setup_nb)
            results.append(result)
            print(result.get_summary())
            
            if not result.passed:
                print("\n" + "="*80)
                print("ERROR: Setup notebook failed!")
                print("Other notebooks will not work without Setup completing successfully.")
                print("="*80)
                sys.exit(1)
    
    # Run other notebooks
    if other_notebooks:
        print("\n" + "="*80)
        print("RUNNING INDEPENDENT NOTEBOOKS")
        print("="*80)
        for notebook in other_notebooks:
            result = tester.test_notebook(notebook)
            results.append(result)
            print(result.get_summary())
    
    # Print overall summary
    print("\n" + "="*80)
    print("OVERALL SUMMARY")
    print("="*80)
    
    passed = sum(1 for r in results if r.passed)
    failed = len(results) - passed
    
    print(f"Total notebooks: {len(results)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    
    if failed > 0:
        print("\nFailed notebooks:")
        for result in results:
            if not result.passed:
                print(f"  - {result.notebook_path}")
        sys.exit(1)
    else:
        print("\n✓ All notebooks passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()
