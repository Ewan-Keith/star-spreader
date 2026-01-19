"""Command-line interface for star-spreader.

This module provides a CLI for generating explicit SELECT statements
and validating queries.
"""

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from typing_extensions import Annotated

from star_spreader.config import Config
from star_spreader.generator.sql_schema_tree import generate_select_from_schema_tree
from star_spreader.schema.databricks import DatabricksSchemaFetcher
from star_spreader.validator.explain import ExplainValidator

app = typer.Typer(
    name="star-spreader",
    help="Convert SELECT * to explicit column lists using database schema",
    add_completion=False,
)
console = Console()


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
    token: Optional[str] = None,
    warehouse_id: Optional[str] = None,
) -> Config:
    """Get configuration from environment or CLI options.

    Args:
        host: Override Databricks host from environment
        token: Override Databricks token from environment
        warehouse_id: Override warehouse ID from environment

    Returns:
        Config instance
    """
    config = Config()

    # Override config values if provided via CLI
    if host:
        config.databricks_host = host
    if token:
        config.databricks_token = token
    if warehouse_id:
        config.databricks_warehouse_id = warehouse_id

    return config


@app.command()
def generate(
    table_name: Annotated[str, typer.Argument(help="Table name in format catalog.schema.table")],
    output: Annotated[
        Optional[Path],
        typer.Option("--output", "-o", help="Output file path (stdout if not specified)"),
    ] = None,
    host: Annotated[
        Optional[str], typer.Option("--host", help="Databricks workspace host URL")
    ] = None,
    token: Annotated[Optional[str], typer.Option("--token", help="Databricks access token")] = None,
) -> None:
    """Generate explicit SELECT statement for a table.

    This command fetches the schema for the specified table and generates
    a SELECT statement with all columns explicitly listed, expanding any
    struct fields into dotted notation with aliases.

    Example:
        star-spreader generate main.default.my_table

        star-spreader generate main.default.my_table --output query.sql
    """
    try:
        # Parse table name
        catalog, schema, table = parse_table_name(table_name)

        # Get configuration
        config = get_config(host, token)

        # Create schema fetcher
        workspace = config.get_workspace_client()
        fetcher = DatabricksSchemaFetcher(workspace_client=workspace)

        # Fetch schema
        console.print(f"[blue]Fetching schema for {table_name}...[/blue]")
        schema_tree = fetcher.get_schema_tree(catalog, schema, table)

        # Generate SELECT statement
        console.print("[blue]Generating SELECT statement...[/blue]")
        select_statement = generate_select_from_schema_tree(schema_tree)

        # Output result
        if output:
            output.write_text(select_statement)
            console.print(f"[green]✓[/green] SELECT statement written to {output}")
        else:
            console.print("\n" + select_statement + "\n")

    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}", err=True)
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}", err=True)
        raise typer.Exit(1)


@app.command()
def validate(
    table_name: Annotated[str, typer.Argument(help="Table name in format catalog.schema.table")],
    host: Annotated[
        Optional[str], typer.Option("--host", help="Databricks workspace host URL")
    ] = None,
    token: Annotated[Optional[str], typer.Option("--token", help="Databricks access token")] = None,
    warehouse_id: Annotated[
        Optional[str],
        typer.Option("--warehouse-id", help="Databricks SQL warehouse HTTP path or ID"),
    ] = None,
    output: Annotated[
        Optional[Path], typer.Option("--output", "-o", help="Output file for validation report")
    ] = None,
) -> None:
    """Validate generated query against SELECT * using EXPLAIN.

    This command generates an explicit SELECT statement and validates it
    against the original SELECT * query by comparing their EXPLAIN plans
    in Databricks.

    Example:
        star-spreader validate main.default.my_table --warehouse-id /sql/1.0/warehouses/abc123

        star-spreader validate main.default.my_table --output validation.txt
    """
    try:
        # Parse table name
        catalog, schema, table = parse_table_name(table_name)

        # Get configuration
        config = get_config(host, token, warehouse_id)

        if not config.databricks_warehouse_id:
            console.print(
                "[red]Error:[/red] Warehouse ID is required for validation. "
                "Set DATABRICKS_WAREHOUSE_ID environment variable or use --warehouse-id option.",
                err=True,
            )
            raise typer.Exit(1)

        # Create schema fetcher and validator
        workspace = config.get_workspace_client()
        fetcher = DatabricksSchemaFetcher(workspace_client=workspace)
        validator = ExplainValidator(
            workspace_client=workspace, warehouse_id=config.databricks_warehouse_id
        )

        # Fetch schema
        console.print(f"[blue]Fetching schema for {table_name}...[/blue]")
        schema_tree = fetcher.get_schema_tree(catalog, schema, table)

        # Generate explicit SELECT statement
        console.print("[blue]Generating explicit SELECT statement...[/blue]")
        explicit_query = generate_select_from_schema_tree(schema_tree)

        # Create SELECT * query
        select_star_query = f"SELECT * FROM `{catalog}`.`{schema}`.`{table}`"

        # Validate
        console.print("[blue]Validating query equivalence...[/blue]")
        result = validator.validate_equivalence(select_star_query, explicit_query, catalog, schema)

        # Format output
        report_lines = []
        report_lines.append(f"Validation Report for {table_name}")
        report_lines.append("=" * 80)
        report_lines.append("")

        if result["equivalent"]:
            report_lines.append("✓ QUERIES ARE EQUIVALENT")
            console.print("[green]✓ Queries are equivalent![/green]")
        else:
            report_lines.append("✗ QUERIES ARE NOT EQUIVALENT")
            console.print("[red]✗ Queries are not equivalent[/red]")

        report_lines.append("")
        report_lines.append("SELECT * Plan:")
        report_lines.append("-" * 80)
        report_lines.append(result["select_star_plan"])
        report_lines.append("")
        report_lines.append("Explicit SELECT Plan:")
        report_lines.append("-" * 80)
        report_lines.append(result["explicit_plan"])
        report_lines.append("")

        if result["differences"]:
            report_lines.append("Differences:")
            report_lines.append("-" * 80)
            report_lines.append(result["differences"])

        report = "\n".join(report_lines)

        # Output result
        if output:
            output.write_text(report)
            console.print(f"[green]✓[/green] Validation report written to {output}")
        else:
            console.print("\n" + report + "\n")

        # Exit with error code if not equivalent
        if not result["equivalent"]:
            raise typer.Exit(1)

    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}", err=True)
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}", err=True)
        raise typer.Exit(1)


def main() -> None:
    """Main entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
