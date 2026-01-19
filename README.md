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
star-spreader generate main.default.my_table

# Save output to file
star-spreader generate main.default.my_table --output query.sql
```

### Python API

```python
from star_spreader.config import load_config
from star_spreader.schema.databricks import DatabricksSchemaFetcher
from star_spreader.generator.sql import SQLGenerator

# Load configuration from environment variables
config = load_config()

# Fetch table schema
fetcher = DatabricksSchemaFetcher(workspace_client=config.get_workspace_client())
schema = fetcher.fetch_schema(
    catalog=config.databricks_catalog,
    schema="my_database",
    table="my_table"
)

# Generate explicit SELECT statement
generator = SQLGenerator(schema)
explicit_sql = generator.generate_select()
print(explicit_sql)
```

## Configuration

Star Spreader uses environment variables for configuration. Create a `.env` file or set these variables in your environment:

```bash
# Required for Databricks connection
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi1234567890abcdef

# Optional: Default catalog and schema
DATABRICKS_CATALOG=main
DATABRICKS_SCHEMA=default
```

### Using the Config Module

```python
from star_spreader.config import Config

# Load from environment variables
config = Config()

# Access configuration
print(config.databricks_catalog)  # 'main'
print(config.databricks_schema)   # 'default'

# Get a workspace client
workspace = config.get_workspace_client()

# Validate configuration
status = config.validate_config()
if not status["workspace_configured"]:
    print("Warning: Databricks credentials not configured")
```

## CLI Reference

### Commands

#### `generate`
Generate explicit SELECT statement for a table.

```bash
star-spreader generate <table_name> [OPTIONS]

Options:
  --output, -o PATH    Output file path (stdout if not specified)
  --host TEXT         Databricks workspace host URL
  --token TEXT        Databricks access token
  --help              Show help message
```

Example:
```bash
# Output to console
star-spreader generate main.analytics.user_events

# Save to file
star-spreader generate main.analytics.user_events --output select.sql

# Override host and token
star-spreader generate main.analytics.user_events \
  --host https://myworkspace.cloud.databricks.com \
  --token dapi123456
```

### CLI Configuration

The CLI uses the same environment variables as the Python API. You can also override settings using command-line options:

```bash
# Using environment variables
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapi1234567890abcdef

star-spreader generate main.default.my_table

# Or override via command-line options
star-spreader generate main.default.my_table \
  --host https://your-workspace.cloud.databricks.com \
  --token dapi1234567890abcdef
```

## Usage Examples

### Basic Schema Fetching

```python
from star_spreader.schema.databricks import DatabricksSchemaFetcher

# Initialize with explicit credentials
fetcher = DatabricksSchemaFetcher(
    host="https://your-workspace.cloud.databricks.com",
    token="your-token"
)

# Fetch schema for a table
schema = fetcher.fetch_schema(
    catalog="main",
    schema="analytics",
    table="user_events"
)

# Inspect columns
for col in schema.columns:
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
# Set required environment variables first
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=your-token
export DATABRICKS_TABLE=your_table_name
export DATABRICKS_CATALOG=main  # Optional
export DATABRICKS_SCHEMA=default  # Optional

# Run the end-to-end example
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
# Set required environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi1234567890abcdef"
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

