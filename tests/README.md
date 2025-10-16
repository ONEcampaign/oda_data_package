# ODA Data Package - Test Suite

This directory contains the comprehensive test suite for the oda-data package. The tests are organized into unit tests, integration tests, and shared fixtures to ensure code quality and correctness.

## 📁 Test Structure

```
tests/
├── conftest.py                      # Shared fixtures and pytest configuration
├── unit/                            # Unit tests (individual functions/methods)
│   ├── test_cache.py               # 3-tier caching system tests
│   ├── test_validation.py          # Input validation tests
│   ├── test_cleaning.py            # Data cleaning utilities tests
│   ├── test_oecd_client.py         # OECDClient API tests
│   ├── test_sources.py             # Data source classes tests (TODO)
│   └── indicators/                 # Indicator processing function tests
│       ├── test_dac1_functions.py
│       ├── test_dac2a_functions.py
│       └── test_crs_functions.py
├── integration/                     # Integration tests (multi-component workflows)
│   ├── test_oecd_client_flow.py    # End-to-end OECDClient workflows
│   └── test_cache_coordination.py  # Cache tier interactions
└── fixtures/                        # Test data files (created during tests)
```

## 🚀 Running Tests

### Prerequisites

First, install the test dependencies:

```bash
# Using uv (recommended)
uv sync --group test

# Or with pip
pip install -e ".[test]"
```

### Run All Tests

```bash
pytest
```

### Run Specific Test Suites

```bash
# Run only unit tests
pytest tests/unit/

# Run only integration tests
pytest tests/integration/

# Run a specific test file
pytest tests/unit/test_validation.py

# Run a specific test class
pytest tests/unit/test_cache.py::TestThreadSafeMemoryCache

# Run a specific test function
pytest tests/unit/test_validation.py::TestValidateCurrency::test_validate_currency_with_valid_currencies_passes
```

### Run Tests by Markers

Tests are organized with markers for flexible execution:

```bash
# Run only fast tests (exclude slow tests)
pytest -m "not slow"

# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration

# Run only cache-related tests
pytest -m cache
```

### Run Tests in Parallel

For faster execution, run tests in parallel using pytest-xdist:

```bash
# Run with 4 parallel workers
pytest -n 4

# Run with auto-detected number of CPUs
pytest -n auto
```

### Generate Coverage Report

```bash
# Run tests with coverage
pytest --cov=src/oda_data --cov-report=html --cov-report=term

# View HTML coverage report
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

## 📊 Test Categories

### Unit Tests

Unit tests focus on testing individual functions and methods in isolation:

- **test_validation.py**: Tests for input parameter validation (currencies, measures, types)
- **test_cleaning.py**: Tests for column name cleaning, DataFrame cleaning, batch processing
- **test_cache.py**: Tests for ThreadSafeMemoryCache, BulkCacheManager, QueryCacheManager
- **test_oecd_client.py**: Tests for OECDClient methods (initialization, filtering, data loading)
- **indicators/test_dac1_functions.py**: Tests for DAC1 custom processing functions

### Integration Tests

Integration tests validate complete workflows with multiple components:

- **test_oecd_client_flow.py**: End-to-end indicator retrieval workflows
- **test_cache_coordination.py**: Cache tier fallback and coordination

### Slow Tests

Some tests are marked as `@pytest.mark.slow` because they:
- Manipulate time using freezegun
- Perform file I/O operations
- Test concurrent access with threading

To exclude slow tests during development:

```bash
pytest -m "not slow"
```

## 🔧 Test Fixtures

### Shared Fixtures (conftest.py)

The `conftest.py` file provides reusable fixtures:

- **Sample DataFrames**: `sample_dac1_df`, `sample_dac2a_df`, `sample_crs_df`, `sample_gni_df`
- **Temporary Directories**: `temp_cache_dir`, `temp_data_path`
- **Mocks**: `mock_oda_reader`, `mock_pydeflate`, `mock_bulk_fetcher`
- **Utilities**: `assert_dataframes_equal`, `assert_series_equal`

### Using Fixtures in Tests

```python
def test_my_function(sample_dac1_df, temp_cache_dir):
    """Test using shared fixtures."""
    # sample_dac1_df is a pre-populated DataFrame
    # temp_cache_dir is a temporary directory that's cleaned up after the test
    pass
```

## 🎯 Writing New Tests

### Test Naming Convention

Follow this pattern for test function names:

```
test_<function_or_method>_<scenario>_<expected_outcome>
```

Examples:
- `test_validate_currency_invalid_currency_raises_value_error`
- `test_clean_column_name_camelcase_returns_snake_case`
- `test_bulk_cache_manager_ensure_uses_cache_when_fresh`

### Test Structure (Arrange-Act-Assert)

```python
def test_example(sample_fixture):
    """Clear docstring explaining what this test validates."""
    # Arrange - Set up test data and conditions
    input_data = sample_fixture.copy()
    expected_result = [...]

    # Act - Execute the function being tested
    result = function_under_test(input_data)

    # Assert - Verify the expected outcome
    assert result == expected_result
```

### Adding Markers

```python
@pytest.mark.slow
@pytest.mark.integration
def test_long_running_integration():
    """Integration test that takes significant time."""
    pass
```

### Mocking External Dependencies

Always mock external dependencies (oda-reader, pydeflate) in tests:

```python
@patch("oda_data.clean_data.common.oecd_dac_exchange")
def test_currency_conversion(mock_exchange, sample_df):
    """Test with mocked pydeflate function."""
    def mock_exchange_function(data, **kwargs):
        df = data.copy()
        df["currency"] = kwargs.get("target_currency", "EUR")
        return df

    mock_exchange.side_effect = mock_exchange_function

    # Run test with mocked function
    result = convert_units(sample_df, currency="EUR")
    assert (result["currency"] == "EUR").all()
```

## 🐛 Debugging Tests

### Run Tests with Verbose Output

```bash
pytest -v  # Verbose
pytest -vv  # More verbose
```

### Show Print Statements

```bash
pytest -s
```

### Run with Python Debugger

```bash
pytest --pdb  # Drop into debugger on first failure
pytest --trace  # Start debugger at beginning of each test
```

### Show Locals on Failure

```bash
pytest -l  # Show local variables in tracebacks
```

## 📈 Coverage Goals

Current coverage targets:
- **Overall**: 70-85% (meaningful coverage, not 100%)
- **Priority modules**: 80%+ coverage
  - `tools/cache.py`
  - `clean_data/validation.py`
  - `clean_data/common.py`
  - `api/oecd.py`

Coverage reports exclude:
- `__init__.py` files
- `config.py`
- `logger.py`
- Test files themselves

## 🔍 Continuous Integration

The test suite is designed to run in CI/CD environments. Recommended CI configuration:

```yaml
# Example GitHub Actions workflow
- name: Run tests
  run: |
    uv sync --group test
    pytest --cov --cov-report=xml

- name: Upload coverage
  uses: codecov/codecov-action@v3
```

## 📚 Additional Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [pytest-mock Documentation](https://pytest-mock.readthedocs.io/)
- [Testing Best Practices](https://testdriven.io/blog/testing-best-practices/)
- [ODA Data Package Documentation](../CLAUDE.md)

## 🆘 Common Issues

### Import Errors

If you see import errors, ensure the package is installed in development mode:

```bash
uv sync
# or
pip install -e .
```

### Fixture Not Found

Fixtures must be either:
1. Defined in `conftest.py` (available to all tests)
2. Defined in the same test file
3. Imported from pytest fixtures

### Slow Test Execution

Use parallel execution:

```bash
pytest -n auto
```

Or skip slow tests during development:

```bash
pytest -m "not slow"
```

---

**Last Updated**: 2025-10-16
