# End-to-End Integration Example

This example demonstrates the complete star-spreader workflow for safely converting `SELECT *` queries to explicit column lists.

## Overview

The `end_to_end_example.py` script showcases the recommended pattern for using star-spreader:

1. **Fetch Schema** - Retrieve complete table schema from Databricks including complex nested types
2. **Generate SQL** - Create an explicit SELECT statement with all columns expanded
3. **Validate** - Confirm that the generated query is equivalent to the original using EXPLAIN plans
4. **Deploy** - Use the validated explicit column list in production

## Quick Start

### Prerequisites

```bash
# Install star-spreader with dependencies
pip install -e ".[dev]"

# Set up Databricks credentials
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"

# Optional: Set warehouse ID for validation
export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"
```

### Basic Usage

```bash
# Specify the table to process
export DATABRICKS_TABLE="users"
export DATABRICKS_CATALOG="main"
export DATABRICKS_SCHEMA="default"

# Run the example
python examples/end_to_end_example.py
```

## Configuration

The example uses environment variables for configuration:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABRICKS_TABLE` | Yes | - | Table name to process |
| `DATABRICKS_CATALOG` | No | `main` | Catalog name |
| `DATABRICKS_SCHEMA` | No | `default` | Schema/database name |
| `DATABRICKS_WAREHOUSE_ID` | No | - | SQL warehouse ID for validation |
| `DATABRICKS_HOST` | Yes* | - | Workspace URL |
| `DATABRICKS_TOKEN` | Yes* | - | Personal access token |

\* Required if not configured in `~/.databrickscfg`

## Example Output

### Simple Table

For a table with columns `id`, `name`, `email`, `created_at`:

```
================================================================================
Star-Spreader End-to-End Integration Example
================================================================================

Processing table: main.default.users

Step 1: Initializing Databricks connection...
✓ Successfully connected to Databricks workspace

Step 2: Fetching table schema from Databricks...
✓ Retrieved schema with 4 columns

  Columns found:
    - id: BIGINT NOT NULL
    - name: STRING NOT NULL
    - email: STRING NULL
    - created_at: TIMESTAMP NOT NULL

Step 3: Generating explicit SELECT statement...
✓ Generated explicit SELECT statement

  Generated SQL:
    SELECT `id`,
           `name`,
           `email`,
           `created_at`
    FROM `main`.`default`.`users`

Step 4: Validating query equivalence...
✓ SUCCESS: Queries are equivalent!

================================================================================
WORKFLOW COMPLETE
================================================================================
```

### Table with STRUCT Columns

For a table with nested structures:

```sql
-- Original table schema:
-- id: BIGINT
-- name: STRING
-- address: STRUCT<street: STRING, city: STRING, state: STRING, zip: STRING>

-- Generated SQL:
SELECT `id`,
       `name`,
       `address`.`street` AS `address_street`,
       `address`.`city` AS `address_city`,
       `address`.`state` AS `address_state`,
       `address`.`zip` AS `address_zip`
FROM `main`.`default`.`customers`
```

### Complex Nested Types

For deeply nested structures:

```sql
-- Table with: user: STRUCT<name: STRING, contact: STRUCT<email: STRING, phone: STRING>>

-- Generated SQL:
SELECT `id`,
       `user`.`name` AS `user_name`,
       `user`.`contact`.`email` AS `user_contact_email`,
       `user`.`contact`.`phone` AS `user_contact_phone`
FROM `main`.`default`.`profiles`
```

## Understanding the Workflow

### 1. Schema Fetching

The `DatabricksSchemaFetcher` uses the Databricks Unity Catalog API to retrieve complete schema information:

- Handles simple types (STRING, INT, BIGINT, etc.)
- Parses complex types (STRUCT, ARRAY, MAP)
- Recursively processes nested structures
- Preserves nullability information

### 2. SQL Generation

The `SQLGenerator` expands columns into explicit SELECT lists:

- **Simple columns**: Included as-is with backtick quoting
- **STRUCT columns**: Expanded using dotted notation (e.g., `parent.child`) with aliases
- **ARRAY columns**: Included as-is (not expanded)
- **MAP columns**: Included as-is (not expanded)

### 3. Query Validation

The `ExplainValidator` compares execution plans to ensure equivalence:

- Executes `EXPLAIN` on both the original and generated queries
- Extracts and normalizes logical plans
- Compares plans to detect any differences
- Reports whether queries are logically equivalent

**Note**: Validation requires a running SQL warehouse. Skip this step if you don't have a warehouse configured.

## Advanced Usage

### Using in Code

```python
from databricks.sdk import WorkspaceClient
from star_spreader.schema.databricks import DatabricksSchemaFetcher
from star_spreader.generator.sql import SQLGenerator
from star_spreader.validator.explain import ExplainValidator

# Initialize components
workspace = WorkspaceClient()
fetcher = DatabricksSchemaFetcher(workspace_client=workspace)
validator = ExplainValidator(
    workspace_client=workspace,
    warehouse_id="your-warehouse-id"
)

# Fetch schema
schema = fetcher.fetch_schema("main", "default", "users")

# Generate explicit SELECT
generator = SQLGenerator(schema)
explicit_select = generator.generate_select()

# Validate equivalence
original = "SELECT * FROM `main`.`default`.`users`"
result = validator.validate_equivalence(
    select_star_query=original,
    explicit_query=explicit_select,
    catalog="main",
    schema="default"
)

if result["equivalent"]:
    print("✓ Validated! Safe to use explicit SELECT")
    print(explicit_select)
else:
    print("✗ Warning: Queries may differ")
    print(result["differences"])
```

### Viewing EXPLAIN Plans

To see the full EXPLAIN plans during validation:

```bash
export SHOW_EXPLAIN_PLANS=true
python examples/end_to_end_example.py
```

### Error Handling

The example includes comprehensive error handling for common scenarios:

- **Connection failures**: Invalid credentials or unreachable workspace
- **Table not found**: Non-existent catalog/schema/table
- **Permission errors**: Insufficient access to table or warehouse
- **Validation errors**: Warehouse not running or query syntax issues

## Use Cases

### 1. Migrating Legacy Code

Convert old queries using `SELECT *` to explicit column lists:

```python
# Before
query = "SELECT * FROM production.orders WHERE date >= '2024-01-01'"

# After running star-spreader
query = """
SELECT `order_id`,
       `customer_id`,
       `order_date`,
       `total_amount`,
       `status`
FROM production.orders
WHERE date >= '2024-01-01'
"""
```

### 2. Schema Evolution Safety

When table schemas change, explicit column lists prevent breaking changes:

- New columns added to the table won't unexpectedly appear in query results
- Column reordering doesn't affect query output
- Type changes are caught early rather than causing runtime errors

### 3. Performance Optimization

Explicit column lists can improve query performance:

- Reduces data transfer by only selecting needed columns
- Enables better query optimization by the engine
- Makes query intent clear for both humans and optimizers

### 4. Code Review and Maintenance

Explicit queries are easier to understand and maintain:

- Column names are self-documenting
- Changes to column selection are visible in code reviews
- Easier to identify which columns are actually used

## Testing

The workflow includes comprehensive integration tests in `tests/test_integration.py`:

```bash
# Run all integration tests
pytest tests/test_integration.py -v

# Run specific test
pytest tests/test_integration.py::TestEndToEndIntegration::test_simple_table_workflow -v

# Run with coverage
pytest tests/test_integration.py --cov=star_spreader
```

The tests verify:
- ✓ Simple tables (primitive types only)
- ✓ Tables with STRUCT columns
- ✓ Nested STRUCT columns
- ✓ ARRAY columns
- ✓ MAP columns
- ✓ Mixed complex types
- ✓ Query validation workflow
- ✓ Error handling

## Troubleshooting

### "Table not found" error

```
✗ Failed to fetch schema: Table not found
```

**Solutions**:
- Verify catalog, schema, and table names are correct
- Check that you have SELECT permissions on the table
- Ensure the table exists in Unity Catalog

### "Validation failed" error

```
✗ Validation failed: Warehouse not available
```

**Solutions**:
- Verify the warehouse ID is correct
- Ensure the warehouse is running
- Check that you have permissions to use the warehouse
- Try skipping validation by not setting `DATABRICKS_WAREHOUSE_ID`

### "Connection failed" error

```
✗ Failed to initialize Databricks connection
```

**Solutions**:
- Verify `DATABRICKS_HOST` includes `https://`
- Check that `DATABRICKS_TOKEN` is valid and not expired
- Ensure network connectivity to Databricks workspace

## Further Reading

- [DatabricksSchemaFetcher Documentation](databricks_usage.py)
- [SQLGenerator Examples](sql_generator_example.py)
- [ExplainValidator Examples](explain_validator_example.py)
- [Integration Tests](../tests/test_integration.py)

## Support

For issues, questions, or contributions, please refer to the main project README.
