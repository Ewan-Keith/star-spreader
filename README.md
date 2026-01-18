# Star Spreader

Convert `SELECT *` queries into explicit column listings by fetching schema information from databases.

## Features

- **Schema Fetching**: Retrieve complete table schemas from Databricks Unity Catalog
- **Complex Type Support**: Handle nested types (STRUCT, ARRAY, MAP) with explicit field selection and reconstruction
- **SQL Generation**: Automatically generate explicit SELECT statements that reconstruct complex types to match SELECT * output
  - STRUCT fields: Uses `STRUCT()` constructor with all fields explicitly listed
  - ARRAY<STRUCT<...>>: Uses `TRANSFORM()` with lambda to reconstruct each array element
- **Query Validation**: Verify equivalence using EXPLAIN plan comparison to ensure correctness

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

## Quick Start

### Command Line Interface

Star Spreader provides a convenient CLI for common tasks:

```bash
# Generate explicit SELECT statement for a table
star-spreader generate main.default.my_table

# Save output to file
star-spreader generate main.default.my_table --output query.sql

# Display table schema
star-spreader show-schema main.default.my_table

# Validate generated query against SELECT *
star-spreader validate main.default.my_table --warehouse-id abc123
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

# Optional: For query validation (can be ID or HTTP path from UI)
DATABRICKS_WAREHOUSE_ID=abc123xyz
# Or use the HTTP path: DATABRICKS_WAREHOUSE_ID=/sql/1.0/warehouses/abc123xyz

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

#### `validate`
Validate generated query against SELECT * using EXPLAIN.

```bash
star-spreader validate <table_name> [OPTIONS]

Options:
  --warehouse-id TEXT  Databricks SQL warehouse ID (required)
  --output, -o PATH   Output file for validation report
  --host TEXT         Databricks workspace host URL
  --token TEXT        Databricks access token
  --help              Show help message
```

Example:
```bash
# Validate query
star-spreader validate main.analytics.user_events --warehouse-id abc123

# Save validation report
star-spreader validate main.analytics.user_events \
  --warehouse-id abc123 \
  --output validation_report.txt
```

#### `show-schema`
Display table schema in a readable format.

```bash
star-spreader show-schema <table_name> [OPTIONS]

Options:
  --output, -o PATH   Output file path (stdout if not specified)
  --format, -f TEXT   Output format: table or text (default: table)
  --host TEXT         Databricks workspace host URL
  --token TEXT        Databricks access token
  --help              Show help message
```

Example:
```bash
# Display schema as table
star-spreader show-schema main.analytics.user_events

# Display as plain text
star-spreader show-schema main.analytics.user_events --format text

# Save to file
star-spreader show-schema main.analytics.user_events \
  --format text \
  --output schema.txt
```

### CLI Configuration

The CLI uses the same environment variables as the Python API. You can also override settings using command-line options:

```bash
# Using environment variables
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapi1234567890abcdef
export DATABRICKS_WAREHOUSE_ID=abc123xyz

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

### End-to-End with Validation

```python
from star_spreader.config import load_config
from star_spreader.schema.databricks import DatabricksSchemaFetcher
from star_spreader.generator.sql import SQLGenerator
from star_spreader.validator.explain import ExplainValidator

# Load configuration
config = load_config()
workspace = config.get_workspace_client()

# Fetch schema and generate SQL
fetcher = DatabricksSchemaFetcher(workspace_client=workspace)
schema = fetcher.fetch_schema(catalog="main", schema="default", table="orders")
generator = SQLGenerator(schema)
explicit_query = generator.generate_select()

# Validate equivalence
validator = ExplainValidator(
    workspace_client=workspace,
    warehouse_id=config.databricks_warehouse_id
)

original_query = "SELECT * FROM `main`.`default`.`orders`"
result = validator.validate_equivalence(
    select_star_query=original_query,
    explicit_query=explicit_query,
    catalog="main",
    schema="default"
)

if result["equivalent"]:
    print("Queries are equivalent!")
else:
    print("Warning: Queries differ")
    print(result["differences"])
```

## Examples Directory

The `examples/` directory contains complete working examples:

- `databricks_usage.py` - Basic schema fetching patterns
- `sql_generator_example.py` - SQL generation examples
- `explain_validator_example.py` - Query validation patterns
- `end_to_end_example.py` - Complete workflow from schema fetch to validation

Run examples:

```bash
# Set required environment variables first
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=your-token
export DATABRICKS_TABLE=your_table_name
export DATABRICKS_CATALOG=main  # Optional
export DATABRICKS_SCHEMA=default  # Optional
export DATABRICKS_WAREHOUSE_ID=warehouse-id  # Optional, for validation

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

```bash
# Run all tests with coverage
pytest

# Run specific test file
pytest tests/test_sql_generator.py

# Run with verbose output
pytest -v

# Generate coverage report
pytest --cov=star_spreader --cov-report=html
```

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

