# Star-Spreader Tests

This directory contains the test suite for star-spreader, organized into unit tests and functional tests.

## Directory Structure

```
tests/
├── unit/              # Unit tests (fast, use mocks)
│   ├── test_sql_generator.py      # SQL generation logic tests
│   ├── test_databricks_schema.py  # Schema fetching with mocked responses
│   └── test_integration.py        # End-to-end workflow with mocks
├── functional/        # Functional tests (slower, use real Databricks)
│   ├── test_functional.py         # Tests against real Databricks workspace
│   └── FUNCTIONAL_TESTS.md        # Detailed functional test documentation
└── README.md          # This file
```

## Test Types

### Unit Tests (`tests/unit/`)

**Characteristics:**
- ✅ Fast execution (~1-2 seconds total)
- ✅ No external dependencies (use mocks)
- ✅ Run in CI on every commit
- ✅ No credentials required

**Coverage:**
- SQL generation logic for all complex types
- Schema parsing and type detection
- Edge cases and error handling
- Complete workflow with mocked Databricks responses

**Run unit tests:**
```bash
# All unit tests
pytest tests/unit/ -v

# Specific file
pytest tests/unit/test_sql_generator.py -v

# Using marker (excludes functional tests)
pytest -m "not functional" -v
```

### Functional Tests (`tests/functional/`)

**Characteristics:**
- ⚠️ Slower execution (~30-60 seconds total)
- ⚠️ Require real Databricks workspace
- ⚠️ Require credentials and warehouse
- ✅ Validate against actual Databricks behavior
- ✅ Compare real EXPLAIN plans

**Coverage:**
- All complex type scenarios from unit tests
- Actual schema fetching from Databricks
- Real EXPLAIN plan comparison
- Validation that generated SQL is truly equivalent to SELECT *

**Run functional tests:**
```bash
# Set required environment variables first
export DATABRICKS_HOST="https://workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi123..."
export DATABRICKS_WAREHOUSE_ID="abc123"

# Run functional tests
pytest tests/functional/ -v

# Or using marker
pytest -m functional -v
```

See [functional/FUNCTIONAL_TESTS.md](functional/FUNCTIONAL_TESTS.md) for detailed setup and usage.

## Running All Tests

```bash
# Run all tests (unit + functional)
pytest

# Run all tests with coverage
pytest --cov=star_spreader --cov-report=html

# Run only unit tests (skip functional)
pytest -m "not functional"
```

## Test Organization

### `test_sql_generator.py`
Tests for the SQL generation logic, including:
- Simple columns
- STRUCT types (simple, nested, deeply nested)
- ARRAY types (primitives, structs, nested)
- MAP types
- Mixed complex types
- Edge cases (STRUCT with key/value fields, multiple independent arrays)
- Arbitrary nesting depth

### `test_databricks_schema.py`
Tests for Databricks schema fetching, including:
- Schema parsing for all column types
- Complex type detection (STRUCT, ARRAY, MAP)
- Nested type parsing
- Error handling

### `test_integration.py`
Integration tests with mocked Databricks, including:
- Complete workflows (fetch → generate → validate)
- All complex type combinations
- Mocked EXPLAIN plan comparisons

### `test_functional.py`
Functional tests against real Databricks, including:
- Real table creation with complex schemas
- Actual schema fetching
- Real EXPLAIN plan validation
- Comprehensive type coverage

## Best Practices

### When Adding New Tests

1. **Unit tests first**: Add unit test for new functionality
2. **Test both unit and functional**: Ensure unit test passes, then add functional test
3. **Descriptive names**: Use clear, descriptive test names
4. **Good assertions**: Include helpful error messages
5. **Clean up**: Functional tests should clean up after themselves

### Test Naming Convention

```python
# Unit tests
def test_<feature>_<scenario>():
    """Test <description>."""
    
# Functional tests
def test_<type>_<scenario>():
    """Test <description> against real Databricks."""
```

### Running Tests During Development

```bash
# Quick feedback loop (unit tests only)
pytest tests/unit/test_sql_generator.py -k "test_struct" -v

# Test specific functionality
pytest tests/unit/ -k "array_of_struct" -v

# Before committing (all unit tests)
pytest tests/unit/ -v

# Before merging (all tests including functional)
pytest -v
```

## Continuous Integration

### Recommended CI Setup

**On every PR:**
```bash
pytest tests/unit/ -v --cov=star_spreader
```

**On main branch only:**
```bash
pytest -v --cov=star_spreader
```

This keeps PR checks fast while ensuring main branch has full validation.

## Coverage

Current test coverage:

- **SQL Generator**: ~100% (all code paths tested)
- **Schema Fetching**: ~95% (error handling tested)
- **Validation**: ~90% (EXPLAIN parsing edge cases tested)
- **Overall**: ~95%

View detailed coverage:
```bash
pytest --cov=star_spreader --cov-report=html
open htmlcov/index.html
```

## Troubleshooting

### Import Errors

If you see import errors, ensure the package is installed:
```bash
pip install -e ".[dev]"
```

### Functional Tests Skip

If functional tests are skipped, check environment variables:
```bash
echo $DATABRICKS_HOST
echo $DATABRICKS_TOKEN
echo $DATABRICKS_WAREHOUSE_ID
```

### Test Discovery Issues

Pytest should auto-discover tests in both directories. If not:
```bash
pytest --collect-only  # Show what tests would run
```

## Contributing

When adding new features:

1. Add unit test in `tests/unit/`
2. Verify it passes: `pytest tests/unit/test_<file>.py -v`
3. Add functional test in `tests/functional/`
4. Verify against real Databricks: `pytest tests/functional/ -v`
5. Ensure all tests pass: `pytest -v`

## Questions?

- Unit test questions: See existing tests in `tests/unit/`
- Functional test questions: See `tests/functional/FUNCTIONAL_TESTS.md`
- General questions: Check main README or open an issue
