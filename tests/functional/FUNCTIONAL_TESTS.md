# Functional Tests

The functional tests in `test_functional.py` validate star-spreader against a **real Databricks workspace**. Unlike unit tests which use mocks, these tests:

1. Create actual tables with complex schemas in Databricks
2. Populate tables with representative test data
3. Run the star-spreader workflow (fetch schema → generate SQL)
4. Execute both `SELECT *` and the generated explicit query
5. **Compare the actual returned data** to ensure they're identical

This approach validates that the generated SQL truly produces the same results as `SELECT *`, not just similar schemas or plans.

## Prerequisites

### Required Environment Variables

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi1234567890abcdef"
# Use the full HTTP path (recommended - copy from Databricks SQL Warehouse UI)
export DATABRICKS_WAREHOUSE_ID="/sql/1.0/warehouses/abc123xyz"
# Or just the warehouse ID also works:
# export DATABRICKS_WAREHOUSE_ID="abc123xyz"
```

### Optional Environment Variables

```bash
# Catalog to use for tests (default: 'main')
export DATABRICKS_CATALOG="main"

# Schema name prefix (default: 'star_spreader_test')
# A timestamp will be appended to create unique schema per run
export FUNCTIONAL_TEST_SCHEMA="star_spreader_test"
```

### Permissions Required

Your Databricks credentials must have:
- `CREATE SCHEMA` permission in the target catalog
- `CREATE TABLE` permission in the schema
- `INSERT` permission on created tables
- `SELECT` permission on created tables
- Access to run SQL queries on the specified warehouse

## Running Functional Tests

### Run All Functional Tests

```bash
pytest tests/functional/test_functional.py -v
```

### Run Specific Test Classes

```bash
# Test only simple types
pytest tests/functional/test_functional.py::TestSimpleTypes -v

# Test only struct types
pytest tests/functional/test_functional.py::TestStructTypes -v

# Test only array types
pytest tests/functional/test_functional.py::TestArrayTypes -v

# Test nested arrays
pytest tests/functional/test_functional.py::TestNestedArrays -v

# Test complex mixed types
pytest tests/functional/test_functional.py::TestMixedComplexTypes -v
```

### Run Specific Test Cases

```bash
# Test a specific scenario
pytest tests/functional/test_functional.py::TestArrayTypes::test_array_of_struct -v
```

## What Gets Tested

The functional tests cover comprehensive scenarios with actual data:

### Simple Types
- ✅ Tables with simple columns (INT, STRING, DECIMAL, BOOLEAN, TIMESTAMP)
- ✅ NULL values in simple columns

### STRUCT Types
- ✅ Simple STRUCT columns
- ✅ Nested STRUCTs (multiple levels)
- ✅ NULL values within STRUCTs

### ARRAY Types
- ✅ ARRAY of primitives (e.g., `ARRAY<STRING>`)
- ✅ ARRAY of STRUCT
- ✅ Empty arrays
- ✅ NULL values within array elements

### Nested Arrays
- ✅ ARRAY<STRUCT<ARRAY<STRUCT>>> (2 levels)

### MAP Types
- ✅ Simple MAP columns
- ✅ Empty maps

### Mixed Complex Types
- ✅ Real-world schemas combining STRUCTs, ARRAYs, and MAPs
- ✅ All complex patterns with realistic data

## How Tests Work

### 1. Setup Phase
Each test:
1. Creates a table with the target schema
2. **Inserts representative test data** including:
   - Multiple rows with different values
   - NULL values where appropriate
   - Empty collections (arrays, maps)
   - Nested structures with varied content

### 2. Execution Phase
The test:
1. Fetches the schema using `DatabricksSchemaFetcher`
2. Generates explicit SQL using the schema tree
3. Executes `SELECT *` query
4. Executes the generated explicit query

### 3. Validation Phase
The test:
1. **Compares the complete result sets** row-by-row
2. Ensures every row, column, and nested value matches exactly
3. Fails if any difference is detected

This approach is **much more reliable** than comparing EXPLAIN plans because:
- It validates actual behavior, not just optimizer predictions
- It catches issues with value ordering, NULL handling, or type coercion
- It ensures nested structures are correctly reconstructed

## Test Schema Management

### Automatic Cleanup

The test suite automatically:
1. Creates a timestamped schema at the start of the test session (e.g., `star_spreader_test_1234567890`)
2. Creates and populates tables within that schema for each test
3. Drops the entire schema (CASCADE) at the end of the test session

### Manual Cleanup (if tests crash)

If tests crash before cleanup:

```sql
-- List test schemas
SHOW SCHEMAS IN main LIKE 'star_spreader_test_*';

-- Drop a specific test schema
DROP SCHEMA IF EXISTS main.star_spreader_test_1234567890 CASCADE;
```

## Understanding Test Failures

### Assertion Error: Query Results Don't Match

If a test fails with "Query results don't match!", the test output shows:

1. **Row count** - Number of rows returned
2. **SELECT * results** - The actual data from `SELECT *`
3. **Explicit results** - The actual data from the generated query
4. **Explicit query** - The generated SQL for inspection

Example failure:
```
AssertionError: Query results don't match!
Row count: 2
SELECT * results: [[1, {'street': '123 Main', 'city': 'NYC'}], [2, {'street': '456 Oak', 'city': 'SF'}]]
Explicit results: [[1, {'street': '123 Main', 'city': 'NYC'}], [2, None]]
Explicit query:
SELECT `id`,
       STRUCT(`address`.`street` AS `street`, `address`.`city` AS `city`) AS `address`
FROM `main`.`test_schema`.`table_name`
```

This indicates the second row's struct is being reconstructed as NULL instead of the actual values.

### Connection Errors

If tests fail with connection errors, verify:
- `DATABRICKS_HOST` is set correctly
- `DATABRICKS_TOKEN` is valid
- Your network can reach the Databricks workspace

### Permission Errors

If tests fail with permission errors, verify:
- You have `USE CATALOG` permission on the target catalog
- You have `CREATE SCHEMA` permission
- The warehouse is running and accessible

### Warehouse Errors

If tests fail with warehouse errors:
- Ensure `DATABRICKS_WAREHOUSE_ID` is correct
- Verify the warehouse is running (not stopped/suspended)
- Check you have permission to use that warehouse

## Performance Considerations

### Test Duration

Functional tests are **significantly slower** than unit tests because they:
- Create real tables in Databricks
- Insert actual data
- Execute SQL queries twice per test
- Wait for warehouse responses

Expect:
- **Unit tests**: ~1-2 seconds total
- **Functional tests**: ~30-90 seconds total (depending on warehouse startup time and data volume)

### Cost Considerations

Running functional tests against Databricks incurs costs:
- SQL warehouse compute time (most significant)
- Storage for temporary tables (minimal, cleaned up after tests)

Recommendations:
- Use a small/development warehouse for tests
- Run functional tests on main branch only in CI
- Don't run on every PR unless needed

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - run: pip install -e ".[dev]"
      - run: pytest tests/unit/ -v
  
  functional-tests:
    runs-on: ubuntu-latest
    # Only run on main branch or PRs with specific label
    if: github.ref == 'refs/heads/main' || contains(github.event.pull_request.labels.*.name, 'test-functional')
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - run: pip install -e ".[dev]"
      - run: pytest tests/functional/ -v
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
          DATABRICKS_WAREHOUSE_ID: ${{ secrets.DATABRICKS_WAREHOUSE_ID }}
          DATABRICKS_CATALOG: main
```

## Troubleshooting

### Tests Skip with "DATABRICKS_WAREHOUSE_ID not set"

Set the required environment variable (see Prerequisites).

### Schema Already Exists Error

The schema name includes a timestamp, so collisions are rare. If it happens:
- Another test run may be in progress
- Previous test run crashed and didn't clean up

Solution: Wait a second and retry, or manually drop the schema.

### "Table not found" Errors

Usually means table creation failed. Check:
- Warehouse is running
- You have CREATE TABLE permission
- The SQL syntax is valid for your Databricks runtime version

### Data Mismatch Failures

If a test consistently fails with mismatched results:
1. Check the generated SQL manually
2. Run both queries in Databricks SQL editor
3. Compare results visually
4. Verify the STRUCT/ARRAY reconstruction syntax
5. File a bug report with the schema and generated SQL

## Example Test Run

```bash
$ pytest tests/functional/test_functional.py::TestStructTypes::test_simple_struct -v

tests/functional/test_functional.py::TestStructTypes::test_simple_struct 
=== Creating test schema: main.star_spreader_test_1705363200 ===
✓ Created schema: main.star_spreader_test_1705363200
PASSED

=== Cleaning up test schema: main.star_spreader_test_1705363200 ===
✓ Dropped schema: main.star_spreader_test_1705363200

============================== 1 passed in 18.43s ==============================
```

## Contributing

When adding new test cases:

1. Add the test to the appropriate test class (or create a new class)
2. Follow the naming convention: `test_<description>`
3. Use descriptive table names: `<type>_<scenario>_table`
4. **Include INSERT statements with realistic data**
5. Include multiple rows with varied data (including NULLs and edge cases)
6. Use helpful assertion messages
7. Test locally before committing

## Questions?

See the main README or open an issue on GitHub.
