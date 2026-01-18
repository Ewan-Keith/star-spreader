"""End-to-end integration example for star-spreader.

This example demonstrates the complete workflow of:
1. Fetching table schema from Databricks
2. Generating an explicit SELECT statement from SELECT *
3. Validating that the generated query is equivalent to the original

This is the recommended pattern for safely expanding SELECT * queries
in production code.
"""

import os
import sys
from typing import Optional

from databricks.sdk import WorkspaceClient
from star_spreader.schema.databricks import DatabricksSchemaFetcher
from star_spreader.generator.sql import SQLGenerator
from star_spreader.validator.explain import ExplainValidator


def run_end_to_end_example(
    catalog: str,
    schema_name: str,
    table_name: str,
    warehouse_id: Optional[str] = None,
) -> None:
    """Run the complete star-spreader workflow.

    This function demonstrates the full process of converting a SELECT * query
    to an explicit column list and validating the result.

    Args:
        catalog: Databricks catalog name (e.g., 'main', 'hive_metastore')
        schema_name: Database/schema name
        table_name: Table name to process
        warehouse_id: Optional Databricks SQL warehouse ID for validation

    Configuration:
        This example uses the WorkspaceClient which reads credentials from:
        - Environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN)
        - ~/.databrickscfg configuration file
        - Azure CLI authentication (if on Azure)

    Raises:
        Exception: If any step in the workflow fails
    """
    print("=" * 80)
    print("Star-Spreader End-to-End Integration Example")
    print("=" * 80)
    print(f"\nProcessing table: {catalog}.{schema_name}.{table_name}\n")

    # -------------------------------------------------------------------------
    # Step 1: Initialize Databricks connection
    # -------------------------------------------------------------------------
    # The WorkspaceClient automatically reads credentials from environment
    # variables or configuration files. See Databricks SDK documentation:
    # https://docs.databricks.com/dev-tools/sdk-python.html
    # -------------------------------------------------------------------------
    print("Step 1: Initializing Databricks connection...")
    try:
        workspace = WorkspaceClient()
        print("✓ Successfully connected to Databricks workspace")
    except Exception as e:
        print(f"✗ Failed to initialize Databricks connection: {e}")
        print("\nConfiguration required:")
        print("  Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables, or")
        print("  Configure ~/.databrickscfg with your workspace credentials")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # Step 2: Fetch table schema
    # -------------------------------------------------------------------------
    # The DatabricksSchemaFetcher retrieves complete schema information
    # including complex nested types (STRUCT, ARRAY, MAP). This step uses
    # the Unity Catalog API to get detailed column metadata.
    # -------------------------------------------------------------------------
    print("\nStep 2: Fetching table schema from Databricks...")
    try:
        fetcher = DatabricksSchemaFetcher(workspace_client=workspace)
        table_schema = fetcher.fetch_schema(
            catalog=catalog,
            schema=schema_name,
            table=table_name,
        )
        print(f"✓ Retrieved schema with {len(table_schema.columns)} columns")

        # Display column information
        print("\n  Columns found:")
        for col in table_schema.columns:
            nullable_str = "NULL" if col.nullable else "NOT NULL"
            complex_str = " (complex)" if col.is_complex else ""
            print(f"    - {col.name}: {col.data_type} {nullable_str}{complex_str}")

            # Show nested structure for complex types
            if col.is_complex and col.children:
                _print_nested_columns(col.children, indent=6)

    except Exception as e:
        print(f"✗ Failed to fetch schema: {e}")
        print("\nPossible causes:")
        print("  - Table does not exist")
        print("  - Insufficient permissions to access the table")
        print("  - Catalog/schema name is incorrect")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # Step 3: Generate explicit SELECT statement
    # -------------------------------------------------------------------------
    # The SQLGenerator expands SELECT * into an explicit column list.
    # For STRUCT fields, it explicitly selects all nested fields and reconstructs
    # them using STRUCT() to produce output identical to what SELECT * would return.
    # For ARRAY<STRUCT<...>>, it uses TRANSFORM() with a lambda to explicitly
    # select and reconstruct each struct element in the array.
    # Simple ARRAY and MAP types are referenced as-is.
    # -------------------------------------------------------------------------
    print("\nStep 3: Generating explicit SELECT statement...")
    try:
        generator = SQLGenerator(table_schema)
        explicit_select = generator.generate_select()
        print("✓ Generated explicit SELECT statement")
        print("\n  Generated SQL:")
        # Indent each line for better readability
        for line in explicit_select.split("\n"):
            print(f"    {line}")

    except Exception as e:
        print(f"✗ Failed to generate SELECT statement: {e}")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # Step 4: Validate equivalence using EXPLAIN plans
    # -------------------------------------------------------------------------
    # The ExplainValidator compares the execution plans of both queries
    # to ensure they produce identical results. This is crucial for
    # verifying correctness before deploying the expanded query.
    # -------------------------------------------------------------------------
    print("\nStep 4: Validating query equivalence...")

    # Construct the original SELECT * query
    full_table_name = f"`{catalog}`.`{schema_name}`.`{table_name}`"
    select_star_query = f"SELECT * FROM {full_table_name}"

    # Note: Validation requires a running SQL warehouse
    if not warehouse_id:
        print("⚠ Skipping validation: warehouse_id not provided")
        print("\n  To enable validation, provide a warehouse_id parameter:")
        print("    export DATABRICKS_WAREHOUSE_ID=your-warehouse-id")
        print("\n  The explicit query has been generated but not validated.")
    else:
        try:
            validator = ExplainValidator(
                workspace_client=workspace,
                warehouse_id=warehouse_id,
            )

            # Compare execution plans
            result = validator.validate_equivalence(
                select_star_query=select_star_query,
                explicit_query=explicit_select,
                catalog=catalog,
                schema=schema_name,
            )

            # -------------------------------------------------------------------------
            # Step 5: Display validation results
            # -------------------------------------------------------------------------
            print("\n" + "=" * 80)
            print("VALIDATION RESULTS")
            print("=" * 80)

            if result["equivalent"]:
                print("\n✓ SUCCESS: Queries are equivalent!")
                print("  The explicit column list produces the same execution plan")
                print("  as SELECT * and can be safely used as a replacement.")
            else:
                print("\n✗ WARNING: Queries may not be equivalent!")
                print("  The execution plans differ. Review the differences below.")

                if result["differences"]:
                    print("\n  Differences detected:")
                    for line in result["differences"].split("\n"):
                        print(f"    {line}")

            # Show the EXPLAIN plans for verification
            print("\n" + "-" * 80)
            print("EXPLAIN Plans Comparison")
            print("-" * 80)
            print("\n[1] SELECT * EXPLAIN Plan:")
            print("-" * 80)
            print(result["select_star_plan"])
            print("\n" + "-" * 80)
            print("[2] Explicit SELECT EXPLAIN Plan:")
            print("-" * 80)
            print(result["explicit_plan"])
            print("-" * 80)

        except Exception as e:
            print(f"✗ Validation failed: {e}")
            print("\nPossible causes:")
            print("  - SQL warehouse is not running")
            print("  - Insufficient permissions to execute queries")
            print("  - Invalid warehouse_id")
            sys.exit(1)

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("WORKFLOW COMPLETE")
    print("=" * 80)
    print(f"\nOriginal query:  SELECT * FROM {full_table_name}")
    print(f"Columns found:   {len(table_schema.columns)}")
    print("Status:          Explicit SELECT generated successfully")
    print("\nNext steps:")
    print("  1. Review the generated SQL above")
    print("  2. Test the query in your development environment")
    print("  3. Replace SELECT * with the explicit column list in your code")
    print("=" * 80)


def _print_nested_columns(columns, indent: int = 0) -> None:
    """Helper function to recursively print nested column structures.

    Args:
        columns: List of ColumnInfo objects to print
        indent: Current indentation level (in spaces)
    """
    prefix = " " * indent
    for col in columns:
        nullable_str = "NULL" if col.nullable else "NOT NULL"
        complex_str = " (complex)" if col.is_complex else ""
        print(f"{prefix}- {col.name}: {col.data_type} {nullable_str}{complex_str}")
        if col.is_complex and col.children:
            _print_nested_columns(col.children, indent + 2)


def main() -> None:
    """Main entry point for the example.

    This function reads configuration from environment variables and
    executes the end-to-end workflow.
    """
    # Read configuration from environment variables
    catalog = os.getenv("DATABRICKS_CATALOG", "main")
    schema = os.getenv("DATABRICKS_SCHEMA", "default")
    table = os.getenv("DATABRICKS_TABLE")
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")

    # Check for required configuration
    if not table:
        print("Error: DATABRICKS_TABLE environment variable is required")
        print("\nUsage:")
        print("  export DATABRICKS_TABLE=your_table_name")
        print("  export DATABRICKS_CATALOG=main  # Optional, defaults to 'main'")
        print("  export DATABRICKS_SCHEMA=default  # Optional, defaults to 'default'")
        print("  export DATABRICKS_WAREHOUSE_ID=abc123  # Optional, for validation")
        print("  python examples/end_to_end_example.py")
        print("\nExample:")
        print("  export DATABRICKS_TABLE=users")
        print("  export DATABRICKS_CATALOG=production")
        print("  export DATABRICKS_SCHEMA=analytics")
        print("  export DATABRICKS_WAREHOUSE_ID=1234567890abcdef")
        print("  python examples/end_to_end_example.py")
        sys.exit(1)

    # Run the workflow
    run_end_to_end_example(
        catalog=catalog,
        schema_name=schema,
        table_name=table,
        warehouse_id=warehouse_id,
    )


if __name__ == "__main__":
    main()
