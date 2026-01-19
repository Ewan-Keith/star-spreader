"""Command-line interface for star-spreader.

This module provides a CLI for generating explicit SELECT statements
from Databricks tables.
"""

import argparse
import sys
from pathlib import Path

from star_spreader.config import get_workspace_client
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
        "--profile",
        default="DEFAULT",
        help="Databricks authentication profile to use (default: DEFAULT)",
    )

    args = parser.parse_args()

    try:
        # Parse table name
        catalog, schema, table = parse_table_name(args.table_name)

        # Create workspace client using specified profile
        workspace = get_workspace_client(profile=args.profile)

        # Create schema fetcher
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
