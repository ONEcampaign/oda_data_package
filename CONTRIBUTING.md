# Contributing to ODA Data Package

Thank you for your interest in contributing to the ODA Data Package! This guide will help you get started with development.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Development Workflow](#development-workflow)
- [Continuous Integration](#continuous-integration)
- [Code Quality](#code-quality)
- [Running Tests](#running-tests)
- [Adding New Features](#adding-new-features)
- [Submitting Changes](#submitting-changes)
- [Project Structure](#project-structure)

## Getting Started

### Prerequisites

- Python 3.11 or higher
- [uv](https://github.com/astral-sh/uv) package manager
- Git

### Fork and Clone

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/oda_data_package.git
   cd oda_data_package
   ```

3. Add the upstream repository:
   ```bash
   git remote add upstream https://github.com/ONEcampaign/oda_data_package.git
   ```

## Development Setup

### 1. Install Dependencies

Install all dependencies including development and test dependencies:

```bash
uv sync --all-groups
```

This will install:
- Core package dependencies
- Development tools (pre-commit, etc.)
- Testing dependencies (pytest, etc.)

### 2. Install Pre-commit Hooks

Pre-commit hooks automatically check your code before each commit:

```bash
uv run pre-commit install
```

The hooks will:
- Format code with Ruff
- Check for common Python mistakes
- Validate JSON/YAML files
- Detect accidentally committed secrets
- Remove trailing whitespace
- Ensure files end with newlines

### 3. Configure Data Path (Optional)

For testing with real data:

```python
from oda_data import set_data_path

set_data_path("/path/to/your/data/cache")
```

## Development Workflow

### Creating a Branch

Create a new branch for your feature or fix:

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/issue-description
```

Use descriptive branch names:
- `feature/add-new-indicator`
- `fix/caching-bug`
- `docs/update-readme`

### Making Changes

1. Make your changes in the appropriate files
2. Add tests for new functionality
3. Update documentation if needed
4. Run tests locally (see below)

### Committing Changes

When you commit, pre-commit hooks will automatically run:

```bash
git add .
git commit -m "Add feature: description of your changes"
```

#### What Happens During Commit

The pre-commit hooks will:
1. **Format your code** with Ruff (auto-fixes)
2. **Lint your code** with Ruff (auto-fixes when possible)
3. **Check file quality** (trailing whitespace, EOF, line endings)
4. **Validate JSON/YAML** files
5. **Check for secrets** (private keys, etc.)

**Note:** Tests are NOT run during commit to keep commits fast. Instead, tests run automatically in CI when you open or update a pull request.

If any hook fails:
- Review the error messages
- Make necessary fixes
- Stage the fixes: `git add .`
- Commit again

#### Skipping Hooks (Use Sparingly)

In rare cases, you may need to skip hooks:

```bash
git commit --no-verify -m "Your message"
```

**Only skip hooks when:**
- You're committing work-in-progress that you'll fix later
- You're reverting a commit
- The hook is incorrectly flagging something (and you'll fix the hook later)

**Never skip hooks when:**
- Submitting a pull request
- Committing to main/master
- The failure indicates a real issue with your code

### Running Pre-commit Manually

Run all hooks on all files:
```bash
uv run pre-commit run --all-files
```

Run a specific hook:
```bash
uv run pre-commit run ruff --all-files
```

Update hooks to latest versions:
```bash
uv run pre-commit autoupdate
```

### Continuous Integration

Tests run automatically via GitHub Actions when you open or update a pull request. The test suite runs on:

- **Operating Systems**: Ubuntu, macOS, and Windows
- **Python Versions**: 3.11, 3.12, and 3.13

This gives us a comprehensive test matrix of **9 combinations** (3 OS × 3 Python versions) to ensure the package works across different platforms.

You can view test results in the "Checks" tab of your pull request. All tests must pass before your PR can be merged.

**Coverage reports** are generated and uploaded to Codecov (if configured) for the Ubuntu + Python 3.11 combination, helping track test coverage over time.

## Code Quality

### Code Style

We use **Ruff** for both linting and formatting:

```bash
# Format code
uv run ruff format .

# Lint and auto-fix
uv run ruff check --fix .

# Lint without auto-fix
uv run ruff check .
```

Ruff replaces Black, isort, flake8, and other tools with a single fast tool.

### Type Hints

While not strictly enforced, type hints are encouraged:

```python
def get_indicators(
    self,
    indicators: str | list[str],
    base_year: int | None = None
) -> pd.DataFrame:
    ...
```

### Docstrings

Use clear docstrings for public functions and classes:

```python
def add_gni_share_column(client: OECDClient, indicator: str) -> pd.DataFrame:
    """Add GNI share percentage column to ODA data.

    Args:
        client: Configured OECDClient instance
        indicator: Indicator code to fetch

    Returns:
        DataFrame with 'gni_share_pct' column added
    """
```

## Running Tests

### Run All Tests

```bash
uv run pytest
```

### Run Specific Tests

```bash
# Run a specific test file
uv run pytest tests/test_api.py

# Run a specific test function
uv run pytest tests/test_api.py::test_oecd_client

# Run tests matching a pattern
uv run pytest -k "test_indicator"
```

### Run Tests by Marker

```bash
# Skip slow tests (useful during development)
uv run pytest -m "not slow"

# Run only slow tests
uv run pytest -m "slow"
```

### Test Coverage

Check test coverage:

```bash
uv run pytest --cov=oda_data --cov-report=html
```

Then open `htmlcov/index.html` in your browser.

### Writing Tests

Place tests in the `tests/` directory:

```python
import pytest
from oda_data import OECDClient

def test_oecd_client_initialization():
    """Test that OECDClient initializes correctly."""
    client = OECDClient(years=range(2020, 2023))
    assert client.years == range(2020, 2023)

@pytest.mark.slow
def test_bulk_download():
    """Test bulk download functionality (slow)."""
    # Mark slow tests that hit external APIs
    ...
```

## Adding New Features

### Adding a New Indicator

1. **Add indicator definition** to the appropriate JSON file:
   - DAC1: `src/oda_data/indicators/dac1/dac1_indicators.json`
   - DAC2A: `src/oda_data/indicators/dac2a/dac2a_indicators.json`
   - CRS: `src/oda_data/indicators/crs/crs_indicators.json`

2. **If custom processing is needed**, add a function:
   - DAC1: `src/oda_data/indicators/dac1/dac1_functions.py`
   - DAC2A: `src/oda_data/indicators/dac2a/dac2a_functions.py`
   - CRS: `src/oda_data/indicators/crs/crs_functions.py`

3. **Reference the function** in the indicator's `custom_function` field

4. **Add tests** for the new indicator

Example indicator definition:
```json
{
  "code": "DAC1.NEW.INDICATOR",
  "name": "My New Indicator",
  "description": "Description of what this indicator measures",
  "sources": ["DAC1"],
  "filters": {
    "DAC1": {
      "measure": "net_disbursement",
      "flow_type": ["1010"]
    }
  },
  "custom_function": "process_new_indicator"
}
```

### Modifying Data Sources

When modifying source classes in `src/oda_data/api/sources.py`:

1. **Override methods** as needed:
   - `__init__()` - Define filter parameters
   - `_create_bulk_fetcher()` - Bulk download logic
   - `download()` - API-based retrieval

2. **Use `_init_filters()`** for standard filters (years, providers, recipients)

3. **Test with both API and bulk downloads**

4. **Update documentation** if the API changes

## Submitting Changes

### Before Submitting a Pull Request

- [ ] All tests pass locally
- [ ] Pre-commit hooks pass
- [ ] New features have tests
- [ ] Documentation is updated
- [ ] CHANGELOG.md is updated (if applicable)
- [ ] Code follows project conventions

### Creating a Pull Request

1. **Push your branch** to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

2. **Create a pull request** on GitHub

3. **Describe your changes**:
   - What does this PR do?
   - Why is this change needed?
   - How has it been tested?
   - Are there any breaking changes?

4. **Link related issues** if applicable

### PR Review Process

- Maintainers will review your PR
- Address any feedback or requested changes
- Once approved, a maintainer will merge your PR

## Project Structure

```
oda_data_package/
├── src/oda_data/              # Main package code
│   ├── api/                   # API clients and data sources
│   │   ├── oecd.py           # OECDClient (main entry point)
│   │   └── sources.py        # Data source classes
│   ├── indicators/           # Indicator definitions
│   │   ├── dac1/            # DAC1 indicators and functions
│   │   ├── dac2a/           # DAC2A indicators and functions
│   │   └── crs/             # CRS indicators and functions
│   ├── clean_data/          # Data cleaning and schema
│   │   ├── schema.py        # Column mappings
│   │   ├── common.py        # Cleaning functions
│   │   └── validation.py    # Input validation
│   ├── settings/            # Configuration and reference data
│   ├── tools/               # Utility functions
│   │   ├── cache.py        # Caching system
│   │   ├── groupings.py    # Provider/recipient groupings
│   │   ├── gni.py          # GNI calculations
│   │   └── names/          # Name mapping utilities
│   └── __init__.py         # Package exports
├── tests/                   # Test files
├── .github/workflows/       # GitHub Actions
├── .pre-commit-config.yaml  # Pre-commit hooks configuration
├── pyproject.toml          # Project metadata and dependencies
├── CLAUDE.md               # AI assistant guidance
├── CONTRIBUTING.md         # This file
└── README.md               # User documentation
```

### Key Components

- **OECDClient**: High-level API for users (in `api/oecd.py`)
- **Data Sources**: DAC1Data, DAC2AData, CRSData classes (in `api/sources.py`)
- **Indicators**: JSON definitions + custom processing functions
- **Caching**: 3-tier system (memory, query cache, bulk cache)
- **Schema**: Column name normalization and mappings

## Getting Help

- **Issues**: Check existing [issues](https://github.com/ONEcampaign/oda_data_package/issues) or create a new one
- **Discussions**: Start a discussion for questions or ideas
- **Documentation**: See [README.md](README.md)


## License

By contributing, you agree that your contributions will be licensed under the same license as the project.
