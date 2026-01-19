"""Command-line interface for star-spreader.

This module provides a CLI for generating explicit SELECT statements
and validating queries.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

from star_spreader.config import Config
from star_spreader.generator.sql_schema_tree import generate_select_from_schema_tree
from star_spreader.schema.databricks import DatabricksSchemaFetcher


def parse_table_name(table_name: str) -> tuple[str, str, str]:
    """Parse a fully qualified table name into catalog, schema, and table.

    Args:
        table_name: Table name in format catalog.schema.table

    Returns:
        Tuple of (catalog, schema, table)

    Raises:
        ValueError: If table name format is invalid
    """
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Invalid table name format: {table_name}. Expected format: catalog.schema.table"
        )
    return parts[0], parts[1], parts[2]


def get_config(
    host: Optional[str] = None,
    warehouse_id: Optional[str] = None,
) -> Config:
    """Get configuration from environment or CLI options.

    Uses Databricks Unified Authentication by default. Authentication is
    discovered automatically from your local environment (databricks CLI,
    Azure CLI, environment variables, ~/.databrickscfg, etc.).

    Args:
        host: Override Databricks host from environment
        warehouse_id: Override warehouse ID from environment

    Returns:
        Config instance
    """
    config = Config()

    # Override config values if provided via CLI
    if host:
        config.databricks_host = host
    if warehouse_id:
        config.databricks_warehouse_id = warehouse_id

    return config


def main() -> None:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        prog="star-spreader",
        description="Convert SELECT * to explicit column lists using database schema",
    )

    parser.add_argument(
        "table_name",
        help="Table name in format catalog.schema.table",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Output file path (stdout if not specified)",
    )
    parser.add_argument(
        "--host",
        help="Databricks workspace host URL (optional, uses unified auth discovery if not set)",
    )

    args = parser.parse_args()

    try:
        # Parse table name
        catalog, schema, table = parse_table_name(args.table_name)

        # Get configuration
        config = get_config(args.host)

        # Create schema fetcher
        workspace = config.get_workspace_client()
        fetcher = DatabricksSchemaFetcher(workspace_client=workspace)

        # Fetch schema
        schema_tree = fetcher.get_schema_tree(catalog, schema, table)

        # Generate SELECT statement
        select_statement = generate_select_from_schema_tree(schema_tree)

        # Output result
        if args.output:
            args.output.write_text(select_statement)
        else:
            print(select_statement)

    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
