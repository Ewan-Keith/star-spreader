# Databricks Schema Fetcher

This module provides schema fetching capabilities for Databricks tables using the Databricks SDK.

## Features

- Fetch complete table schemas from Databricks Unity Catalog
- Support for complex nested types:
  - `STRUCT<...>` - Nested structures with named fields
  - `ARRAY<...>` - Arrays of any type
  - `MAP<K, V>` - Key-value maps
- Recursive parsing of arbitrarily nested types
- Full type preservation including nullability information

## Installation

The Databricks SDK is included as a dependency in the main `star-spreader` package:

```bash
pip install star-spreader
```

Or for development:

```bash
pip install -e ".[dev]"
```

## Usage

### Basic Usage

```python
from star_spreader.schema.databricks import DatabricksSchemaFetcher

# Initialize with credentials
fetcher = DatabricksSchemaFetcher(
    host="https://your-workspace.cloud.databricks.com",
    token="your-personal-access-token"
)

# Fetch schema
schema = fetcher.fetch_schema(
    catalog="main",
    schema="default",
    table="users"
)

# Access column information
for col in schema.columns:
    print(f"{col.name}: {col.data_type}")
```

### Using WorkspaceClient

You can also pass a pre-configured `WorkspaceClient`:

```python
from databricks.sdk import WorkspaceClient
from star_spreader.schema.databricks import DatabricksSchemaFetcher

# Create workspace client (uses .databrickscfg or environment variables)
workspace = WorkspaceClient()

# Initialize fetcher
fetcher = DatabricksSchemaFetcher(workspace_client=workspace)

schema = fetcher.fetch_schema("main", "default", "users")
```

### Working with Complex Types

The fetcher automatically parses complex nested types:

```python
# Example table schema:
# - id: BIGINT
# - name: STRING
# - profile: STRUCT<age: INT, addresses: ARRAY<STRUCT<street: STRING, city: STRING>>>

schema = fetcher.fetch_schema("main", "default", "users")

# Access nested columns
for col in schema.columns:
    if col.name == "profile" and col.is_complex:
        # col.children contains the nested fields
        for child in col.children:
            print(f"  {child.name}: {child.data_type}")
            if child.is_complex and child.children:
                for nested in child.children:
                    print(f"    {nested.name}: {nested.data_type}")
```

## API Reference

### DatabricksSchemaFetcher

The main class for fetching schemas from Databricks.

#### `__init__(host=None, token=None, workspace_client=None)`

Initialize the schema fetcher.

**Parameters:**
- `host` (str, optional): Databricks workspace URL
- `token` (str, optional): Personal access token
- `workspace_client` (WorkspaceClient, optional): Pre-configured client

**Raises:**
- `ValueError`: If neither `workspace_client` nor both `host` and `token` are provided

#### `fetch_schema(catalog, schema, table) -> TableSchema`

Fetch schema for a specific table.

**Parameters:**
- `catalog` (str): Catalog name (e.g., "main", "hive_metastore")
- `schema` (str): Schema/database name
- `table` (str): Table name

**Returns:**
- `TableSchema`: Complete table schema including all columns and nested structures

**Raises:**
- `Exception`: If table is not found or API call fails

## Type Parsing

The fetcher handles the following complex type patterns:

### STRUCT Types

```
STRUCT<field1: INT, field2: STRING>
```

Parsed into `ColumnInfo` objects where:
- `is_complex = True`
- `children` contains a list of `ColumnInfo` for each field

### ARRAY Types

```
ARRAY<INT>
ARRAY<STRUCT<x: INT, y: STRING>>
```

Parsed into a single child named `"element"` with the array's element type.

### MAP Types

```
MAP<STRING, INT>
MAP<STRING, STRUCT<x: INT>>
```

Parsed into two children:
- `"key"` with the key type (nullable=False)
- `"value"` with the value type (nullable=True)

### Nested Complex Types

The parser handles arbitrarily nested types:

```
STRUCT<
  user_id: BIGINT,
  profile: STRUCT<
    name: STRING,
    contacts: ARRAY<STRUCT<
      type: STRING,
      value: STRING
    >>
  >,
  metadata: MAP<STRING, ARRAY<STRING>>
>
```

## Databricks API Usage

The fetcher uses the following Databricks SDK APIs:

- `workspace.tables.get(full_name)`: Retrieves complete table metadata including column information

The table name must be in the format: `{catalog}.{schema}.{table}`

### Authentication

The Databricks SDK supports multiple authentication methods:

1. **Environment variables:**
   - `DATABRICKS_HOST`
   - `DATABRICKS_TOKEN`

2. **Configuration file** (`~/.databrickscfg`):
   ```ini
   [DEFAULT]
   host = https://your-workspace.cloud.databricks.com
   token = your-token
   ```

3. **Direct parameters** (as shown in examples above)

For more details, see the [Databricks SDK documentation](https://docs.databricks.com/dev-tools/sdk-python.html).

## Implementation Notes

### Type String Parsing

The module includes sophisticated type string parsing that:

- Respects nested brackets when splitting field definitions
- Handles whitespace and case-insensitivity
- Recursively parses nested structures

### Performance Considerations

- Each `fetch_schema()` call makes one API request to Databricks
- Complex type parsing is done locally after fetching metadata
- No caching is implemented; consider caching schemas if fetching repeatedly

### Limitations

- Requires Databricks Unity Catalog (three-level namespace)
- Type string parsing assumes standard Databricks SQL type syntax
- Very deeply nested types (>10 levels) may be slow to parse

## Examples

See `examples/databricks_usage.py` for complete working examples including:
- Basic schema fetching
- Working with complex types
- Generating SELECT statements from schema
