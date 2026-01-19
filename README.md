# Star Spreader

Convert `SELECT *` queries into explicit column listings by fetching schema information from databases.

## Features

- **Schema Fetching**: Retrieve complete table schemas from Databricks Unity Catalog
- **Complex Type Support**: Handle nested types (STRUCT, ARRAY, MAP) with explicit field selection and reconstruction
- **SQL Generation**: Automatically generate explicit SELECT statements that reconstruct complex types to match SELECT * output
  - STRUCT fields: Uses `STRUCT()` constructor with all fields explicitly listed
  - ARRAY<STRUCT<...>>: Uses `TRANSFORM()` with lambda to reconstruct each array element

## Installation

Install from the repository:

```bash
pip install -e .
```

For development with testing and linting tools:

```bash
pip install -e ".[dev]"
```

### Requirements

- Python 3.8 or higher
- Databricks workspace with Unity Catalog
- Valid Databricks credentials
- (For functional tests) A running SQL warehouse

## Quick Start

### Command Line Interface

Star Spreader provides a convenient CLI for common tasks:

```bash
# Generate explicit SELECT statement for a table
star-spreader main.default.my_table

# Save output to file
star-spreader main.default.my_table --output query.sql

# Use a specific profile
star-spreader main.default.my_table --profile production
```

### Python API

```python
from star_spreader.schema.databricks import DatabricksSchemaFetcher
from star_spreader.generator.sql_schema_tree import generate_select_from_schema_tree

# Fetch table schema using default profile
fetcher = DatabricksSchemaFetcher()
schema_tree = fetcher.get_schema_tree(
    catalog="main",
    schema="my_database",
    table="my_table"
)

# Generate explicit SELECT statement
explicit_sql = generate_select_from_schema_tree(schema_tree)
print(explicit_sql)
```

## Configuration

Star Spreader uses **Databricks Unified Authentication with profile support**. Authentication is handled through the Databricks CLI, which stores credentials in `~/.databrickscfg`.

### Authentication Setup

**Step 1: Authenticate with Databricks CLI**

```bash
# Create a default profile
databricks auth login --host https://your-workspace.cloud.databricks.com

# Or create a named profile for multiple workspaces
databricks auth login --profile production --host https://prod.cloud.databricks.com
databricks auth login --profile staging --host https://staging.cloud.databricks.com
```

**Step 2: Use star-spreader**

```bash
# Use the default profile
star-spreader main.default.my_table

# Or specify a named profile
star-spreader main.default.my_table --profile production
```

### How Profiles Work

Profiles are stored in `~/.databrickscfg` and contain:
- Workspace URL
- Authentication credentials
- Other workspace-specific settings

Star Spreader uses the `DEFAULT` profile by default. Use the `--profile` flag to select a different profile.

For more details, see the [Databricks Unified Authentication documentation](https://docs.databricks.com/dev-tools/auth/unified-auth.html).

### Using the Config Module

```python
from star_spreader.config import get_workspace_client

# Get a workspace client using the default profile
workspace = get_workspace_client()

# Or specify a named profile
workspace = get_workspace_client(profile="production")
```

## CLI Reference

### Commands

#### `generate`
Generate explicit SELECT statement for a table.

```bash
star-spreader <table_name> [OPTIONS]

Arguments:
  table_name           Fully qualified table name (catalog.schema.table)

Options:
  --output, -o PATH    Output file path (stdout if not specified)
  --profile TEXT       Databricks profile to use (default: DEFAULT)
  --help               Show help message
```

Example:
```bash
# Output to console using default profile
star-spreader main.analytics.user_events

# Save to file
star-spreader main.analytics.user_events --output select.sql

# Use a specific profile (if you have multiple workspaces)
star-spreader main.analytics.user_events --profile production
```

### CLI Configuration

The CLI uses profiles from `~/.databrickscfg`. No environment variables are needed for workspace or table information - everything is specified via command-line arguments.

```bash
# Authenticate once (if not already done)
databricks auth login

# Then use star-spreader directly
star-spreader main.default.my_table

# Use a specific profile for a different workspace
star-spreader main.default.my_table --profile production
```

## Usage Examples

### Basic Schema Fetching

```python
from star_spreader.schema.databricks import DatabricksSchemaFetcher

# Initialize using default profile
fetcher = DatabricksSchemaFetcher()

# Or use a named profile
fetcher = DatabricksSchemaFetcher(profile="production")

# Fetch schema for a table
schema_tree = fetcher.get_schema_tree(
    catalog="main",
    schema="analytics",
    table="user_events"
)

# Inspect columns
for col in schema_tree.columns:
    print(f"{col.name}: {col.data_type}")
```

### Working with Complex Types

```python
# Tables with nested structures are automatically parsed
schema = fetcher.fetch_schema(
    catalog="main",
    schema="analytics",
    table="events_with_metadata"
)

# Access nested column information
for col in schema.columns:
    if col.is_complex and col.children:
        print(f"Complex column: {col.name}")
        for child in col.children:
            print(f"  - {child.name}: {child.data_type}")
```

### Generating SQL

```python
from star_spreader.generator.sql import generate_select

# Generate explicit SELECT (convenience function)
sql = generate_select(schema)
print(sql)

# Output example (structs explicitly reconstructed to match SELECT *):
# SELECT `id`,
#        `name`,
#        STRUCT(`profile`.`age` AS `age`, `profile`.`email` AS `email`) AS `profile`,
#        `tags`
# FROM `main`.`analytics`.`users`

# For ARRAY<STRUCT<...>>, uses TRANSFORM to reconstruct each element:
# SELECT `order_id`,
#        TRANSFORM(`line_items`, item -> STRUCT(
#          item.`product_id` AS `product_id`,
#          item.`quantity` AS `quantity`,
#          item.`price` AS `price`
#        )) AS `line_items`
# FROM `main`.`analytics`.`orders`
```



## Examples Directory

The `examples/` directory contains complete working examples:

- `databricks_usage.py` - Basic schema fetching patterns
- `sql_generator_example.py` - SQL generation examples
- `end_to_end_example.py` - Complete workflow from schema fetch to SQL generation

Run examples:

```bash
# Authenticate with Databricks CLI (if not already done)
databricks auth login

# Examples may need to be updated to use the new profile-based API
# Check each example file for specific usage instructions
python examples/end_to_end_example.py
```

## Development

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/your-repo/star-spreader.git
cd star-spreader

# Install in development mode with dev dependencies
pip install -e ".[dev]"
```

### Running Tests

Star-spreader includes both **unit tests** (fast, use mocks) and **functional tests** (slower, validate against real Databricks with actual data).

#### Unit Tests Only (Fast)

```bash
# Run unit tests (no Databricks connection required)
pytest tests/unit/ -v

# Run specific unit test file
pytest tests/unit/test_sql_generation.py

# Generate coverage report
pytest --cov=star_spreader --cov-report=html tests/unit/
```

#### Functional Tests (Require Databricks)

Functional tests validate against a real Databricks workspace by comparing actual query results. See [tests/functional/FUNCTIONAL_TESTS.md](tests/functional/FUNCTIONAL_TESTS.md) for detailed setup instructions.

```bash
# Authenticate with Databricks CLI
databricks auth login

# Set warehouse ID for SQL execution
export DATABRICKS_WAREHOUSE_ID="/sql/1.0/warehouses/abc123xyz"

# Run functional tests only
pytest tests/functional/ -v

# Run all tests (unit + functional)
pytest
```

**Note:** Functional tests create temporary tables with test data and compare actual query results to ensure correctness.

### Code Quality

Format code with Black:

```bash
black src tests examples
```

Lint code with Ruff:

```bash
ruff check src tests examples
```

Type check with mypy:

```bash
mypy src
```

## License

MIT License - see LICENSE file for details

