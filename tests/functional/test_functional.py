"""Functional tests for star-spreader against a real Databricks workspace.

These tests require actual Databricks credentials and a warehouse to run.
They create temporary tables in a test schema and validate that the generated
SQL produces the same EXPLAIN plan as SELECT *.

Configuration:
    Set the following environment variables:
    - DATABRICKS_HOST: Your workspace URL
    - DATABRICKS_TOKEN: Your access token
    - DATABRICKS_WAREHOUSE_ID: SQL warehouse HTTP path (e.g., '/sql/1.0/warehouses/abc123xyz')
                                or warehouse ID (e.g., 'abc123xyz'). HTTP path is recommended.
    - DATABRICKS_CATALOG: Catalog to use (default: 'main')
    - FUNCTIONAL_TEST_SCHEMA: Schema name for tests (default: 'star_spreader_test')
"""

import os
import time
import uuid
from typing import Generator

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from star_spreader.schema.databricks import DatabricksSchemaFetcher
from star_spreader.generator.sql_schema_tree import generate_select_from_schema_tree
from star_spreader.validator.explain import ExplainValidator


@pytest.fixture(scope="module")
def workspace_client() -> WorkspaceClient:
    """Create a WorkspaceClient for the test session."""
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")

    if not host or not token:
        pytest.skip("DATABRICKS_HOST and DATABRICKS_TOKEN environment variables not set")

    return WorkspaceClient(host=host, token=token)


@pytest.fixture(scope="module")
def warehouse_id() -> str:
    """Get the warehouse ID or HTTP path for running queries.

    Accepts either:
    - HTTP path: /sql/1.0/warehouses/abc123xyz (recommended)
    - Warehouse ID: abc123xyz
    """
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        pytest.skip("DATABRICKS_WAREHOUSE_ID environment variable not set")
    return warehouse_id


@pytest.fixture(scope="module")
def catalog() -> str:
    """Get the catalog to use for tests."""
    return os.getenv("DATABRICKS_CATALOG", "main")


@pytest.fixture(scope="module")
def test_schema(
    workspace_client: WorkspaceClient, catalog: str, warehouse_id: str
) -> Generator[str, None, None]:
    """Create a test schema for the test session and clean it up afterwards."""
    # Generate unique schema name with timestamp
    timestamp = int(time.time())
    base_name = os.getenv("FUNCTIONAL_TEST_SCHEMA", "star_spreader_test")
    schema_name = f"{base_name}_{timestamp}"

    print(f"\n=== Creating test schema: {catalog}.{schema_name} ===")

    # Create schema
    create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema_name}`"
    response = workspace_client.statement_execution.execute_statement(
        statement=create_schema_sql,
        warehouse_id=warehouse_id,
        wait_timeout="30s",
    )

    if response.status and response.status.state == StatementState.FAILED:
        error_msg = response.status.error.message if response.status.error else "Unknown error"
        pytest.fail(f"Failed to create test schema: {error_msg}")

    print(f"✓ Created schema: {catalog}.{schema_name}")

    yield schema_name

    # Cleanup: Drop schema and all tables
    print(f"\n=== Cleaning up test schema: {catalog}.{schema_name} ===")
    try:
        drop_schema_sql = f"DROP SCHEMA IF EXISTS `{catalog}`.`{schema_name}` CASCADE"
        workspace_client.statement_execution.execute_statement(
            statement=drop_schema_sql,
            warehouse_id=warehouse_id,
            wait_timeout="30s",
        )
        print(f"✓ Dropped schema: {catalog}.{schema_name}")
    except Exception as e:
        print(f"⚠ Warning: Failed to clean up schema: {e}")


def create_table(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_name: str,
    create_sql: str,
) -> None:
    """Helper to create a table in the test schema."""
    full_table_name = f"`{catalog}`.`{schema}`.`{table_name}`"

    # Drop table if exists
    drop_sql = f"DROP TABLE IF EXISTS {full_table_name}"
    workspace_client.statement_execution.execute_statement(
        statement=drop_sql,
        warehouse_id=warehouse_id,
        wait_timeout="30s",
    )

    # Create table
    response = workspace_client.statement_execution.execute_statement(
        statement=create_sql,
        warehouse_id=warehouse_id,
        wait_timeout="30s",
    )

    if response.status and response.status.state == StatementState.FAILED:
        error_msg = response.status.error.message if response.status.error else "Unknown error"
        raise Exception(f"Failed to create table {full_table_name}: {error_msg}")


def validate_query_equivalence(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_name: str,
) -> dict:
    """
    End-to-end validation: fetch schema, generate SQL, validate EXPLAIN plan.

    Returns the validation result dictionary.
    """
    # Step 1: Fetch schema tree
    fetcher = DatabricksSchemaFetcher(workspace_client=workspace_client)
    schema_tree = fetcher.get_schema_tree(catalog=catalog, schema=schema, table=table_name)

    # Step 2: Generate explicit SELECT
    explicit_query = generate_select_from_schema_tree(schema_tree)

    # Step 3: Validate equivalence
    validator = ExplainValidator(
        workspace_client=workspace_client,
        warehouse_id=warehouse_id,
    )

    full_table_name = f"`{catalog}`.`{schema}`.`{table_name}`"
    select_star_query = f"SELECT * FROM {full_table_name}"

    result = validator.validate_equivalence(
        select_star_query=select_star_query,
        explicit_query=explicit_query,
        catalog=catalog,
        schema=schema,
    )

    return result


class TestSimpleTypes:
    """Test tables with only simple column types."""

    def test_simple_columns(
        self,
        workspace_client: WorkspaceClient,
        warehouse_id: str,
        catalog: str,
        test_schema: str,
    ):
        """Test table with simple columns (no complex types)."""
        table_name = "simple_table"

        create_sql = f"""
        CREATE TABLE `{catalog}`.`{test_schema}`.`{table_name}` (
            id BIGINT,
            name STRING,
            age INT,
            salary DECIMAL(10, 2),
            is_active BOOLEAN,
            created_at TIMESTAMP
        )
        """

        create_table(workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql)

        result = validate_query_equivalence(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["equivalent"], (
            f"Queries are not equivalent!\n"
            f"Differences: {result['differences']}\n"
            f"SELECT * plan: {result['select_star_plan']}\n"
            f"Explicit plan: {result['explicit_plan']}"
        )


class TestStructTypes:
    """Test tables with STRUCT columns."""

    def test_simple_struct(
        self,
        workspace_client: WorkspaceClient,
        warehouse_id: str,
        catalog: str,
        test_schema: str,
    ):
        """Test table with a simple struct column."""
        table_name = "simple_struct_table"

        create_sql = f"""
        CREATE TABLE `{catalog}`.`{test_schema}`.`{table_name}` (
            id BIGINT,
            address STRUCT<street: STRING, city: STRING, zip: INT>
        )
        """

        create_table(workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql)

        result = validate_query_equivalence(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["equivalent"], (
            f"Queries are not equivalent!\nDifferences: {result['differences']}"
        )

    def test_nested_struct(
        self,
        workspace_client: WorkspaceClient,
        warehouse_id: str,
        catalog: str,
        test_schema: str,
    ):
        """Test table with nested struct columns."""
        table_name = "nested_struct_table"

        create_sql = f"""
        CREATE TABLE `{catalog}`.`{test_schema}`.`{table_name}` (
            id BIGINT,
            person STRUCT<
                name: STRING,
                contact: STRUCT<email: STRING, phone: STRING>
            >
        )
        """

        create_table(workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql)

        result = validate_query_equivalence(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["equivalent"], (
            f"Queries are not equivalent!\nDifferences: {result['differences']}"
        )

    def test_struct_with_key_value_fields(
        self,
        workspace_client: WorkspaceClient,
        warehouse_id: str,
        catalog: str,
        test_schema: str,
    ):
        """Test STRUCT with fields named 'key' and 'value' (not to be confused with MAP)."""
        table_name = "struct_key_value_table"

        create_sql = f"""
        CREATE TABLE `{catalog}`.`{test_schema}`.`{table_name}` (
            id BIGINT,
            details STRUCT<key: STRING, value: INT>
        )
        """

        create_table(workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql)

        result = validate_query_equivalence(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["equivalent"], (
            f"Queries are not equivalent!\nDifferences: {result['differences']}"
        )


class TestArrayTypes:
    """Test tables with ARRAY columns."""

    def test_array_of_primitives(
        self,
        workspace_client: WorkspaceClient,
        warehouse_id: str,
        catalog: str,
        test_schema: str,
    ):
        """Test table with array of primitive types."""
        table_name = "array_primitives_table"

        create_sql = f"""
        CREATE TABLE `{catalog}`.`{test_schema}`.`{table_name}` (
            id BIGINT,
            tags ARRAY<STRING>,
            scores ARRAY<INT>
        )
        """

        create_table(workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql)

        result = validate_query_equivalence(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["equivalent"], (
            f"Queries are not equivalent!\nDifferences: {result['differences']}"
        )

    def test_array_of_struct(
        self,
        workspace_client: WorkspaceClient,
        warehouse_id: str,
        catalog: str,
        test_schema: str,
    ):
        """Test table with array of structs."""
        table_name = "array_of_struct_table"

        create_sql = f"""
        CREATE TABLE `{catalog}`.`{test_schema}`.`{table_name}` (
            id BIGINT,
            line_items ARRAY<STRUCT<product_id: INT, quantity: INT, price: DECIMAL(10,2)>>
        )
        """

        create_table(workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql)

        result = validate_query_equivalence(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["equivalent"], (
            f"Queries are not equivalent!\nDifferences: {result['differences']}"
        )

    def test_array_of_struct_with_nested_struct(
        self,
        workspace_client: WorkspaceClient,
        warehouse_id: str,
        catalog: str,
        test_schema: str,
    ):
        """Test ARRAY<STRUCT> where struct contains nested STRUCT."""
        table_name = "array_struct_nested_table"

        create_sql = f"""
        CREATE TABLE `{catalog}`.`{test_schema}`.`{table_name}` (
            id BIGINT,
            employees ARRAY<STRUCT<
                name: STRING,
                address: STRUCT<city: STRING, zip: INT>
            >>
        )
        """

        create_table(workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql)

        result = validate_query_equivalence(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["equivalent"], (
            f"Queries are not equivalent!\nDifferences: {result['differences']}"
        )


class TestNestedArrays:
    """Test deeply nested array structures."""

    def test_array_struct_array_struct(
        self,
        workspace_client: WorkspaceClient,
        warehouse_id: str,
        catalog: str,
        test_schema: str,
    ):
        """Test ARRAY<STRUCT<ARRAY<STRUCT>>> - two levels of nesting."""
        table_name = "nested_arrays_table"

        create_sql = f"""
        CREATE TABLE `{catalog}`.`{test_schema}`.`{table_name}` (
            id BIGINT,
            departments ARRAY<STRUCT<
                dept_name: STRING,
                teams: ARRAY<STRUCT<team_name: STRING, size: INT>>
            >>
        )
        """

        create_table(workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql)

        result = validate_query_equivalence(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["equivalent"], (
            f"Queries are not equivalent!\nDifferences: {result['differences']}"
        )

    def test_three_level_nested_arrays(
        self,
        workspace_client: WorkspaceClient,
        warehouse_id: str,
        catalog: str,
        test_schema: str,
    ):
        """Test ARRAY<STRUCT<ARRAY<STRUCT<ARRAY<STRUCT>>>>> - three levels."""
        table_name = "three_level_nested_table"

        create_sql = f"""
        CREATE TABLE `{catalog}`.`{test_schema}`.`{table_name}` (
            id BIGINT,
            data STRUCT<
                regions: ARRAY<STRUCT<
                    region_name: STRING,
                    countries: ARRAY<STRUCT<
                        country_name: STRING,
                        cities: ARRAY<STRUCT<city_name: STRING, population: INT>>
                    >>
                >>
            >
        )
        """

        create_table(workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql)

        result = validate_query_equivalence(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["equivalent"], (
            f"Queries are not equivalent!\nDifferences: {result['differences']}"
        )


class TestMapTypes:
    """Test tables with MAP columns."""

    def test_simple_map(
        self,
        workspace_client: WorkspaceClient,
        warehouse_id: str,
        catalog: str,
        test_schema: str,
    ):
        """Test table with MAP column."""
        table_name = "map_table"

        create_sql = f"""
        CREATE TABLE `{catalog}`.`{test_schema}`.`{table_name}` (
            id BIGINT,
            metadata MAP<STRING, STRING>
        )
        """

        create_table(workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql)

        result = validate_query_equivalence(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["equivalent"], (
            f"Queries are not equivalent!\nDifferences: {result['differences']}"
        )


class TestMixedComplexTypes:
    """Test tables combining multiple complex type patterns."""

    def test_struct_with_array_and_map(
        self,
        workspace_client: WorkspaceClient,
        warehouse_id: str,
        catalog: str,
        test_schema: str,
    ):
        """Test STRUCT containing both ARRAY and MAP fields."""
        table_name = "mixed_complex_table"

        create_sql = f"""
        CREATE TABLE `{catalog}`.`{test_schema}`.`{table_name}` (
            id BIGINT,
            data STRUCT<
                tags: ARRAY<STRING>,
                metadata: MAP<STRING, STRING>,
                stats: STRUCT<count: INT, sum: DECIMAL(10,2)>
            >
        )
        """

        create_table(workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql)

        result = validate_query_equivalence(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["equivalent"], (
            f"Queries are not equivalent!\nDifferences: {result['differences']}"
        )

    def test_array_struct_with_mixed_fields(
        self,
        workspace_client: WorkspaceClient,
        warehouse_id: str,
        catalog: str,
        test_schema: str,
    ):
        """Test ARRAY<STRUCT> where struct contains ARRAY, MAP, and nested STRUCT."""
        table_name = "array_mixed_struct_table"

        create_sql = f"""
        CREATE TABLE `{catalog}`.`{test_schema}`.`{table_name}` (
            id BIGINT,
            records ARRAY<STRUCT<
                tags: ARRAY<STRING>,
                metadata: MAP<STRING, STRING>,
                details: STRUCT<key: STRING, value: INT>
            >>
        )
        """

        create_table(workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql)

        result = validate_query_equivalence(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["equivalent"], (
            f"Queries are not equivalent!\nDifferences: {result['differences']}"
        )

    def test_real_world_schema(
        self,
        workspace_client: WorkspaceClient,
        warehouse_id: str,
        catalog: str,
        test_schema: str,
    ):
        """Test a realistic complex schema combining all patterns."""
        table_name = "real_world_table"

        create_sql = f"""
        CREATE TABLE `{catalog}`.`{test_schema}`.`{table_name}` (
            id BIGINT,
            created_at TIMESTAMP,
            tags ARRAY<STRING>,
            user STRUCT<
                name: STRING,
                email: STRING,
                address: STRUCT<city: STRING, country: STRING>
            >,
            orders ARRAY<STRUCT<
                order_id: INT,
                shipping: STRUCT<carrier: STRING, tracking: STRING>
            >>,
            metadata MAP<STRING, STRING>
        )
        """

        create_table(workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql)

        result = validate_query_equivalence(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["equivalent"], (
            f"Queries are not equivalent!\nDifferences: {result['differences']}"
        )


class TestMultipleIndependentArrays:
    """Test tables with multiple independent nested array fields."""

    def test_two_independent_nested_arrays(
        self,
        workspace_client: WorkspaceClient,
        warehouse_id: str,
        catalog: str,
        test_schema: str,
    ):
        """Test two top-level ARRAY<STRUCT<ARRAY<STRUCT>>> fields."""
        table_name = "independent_arrays_table"

        create_sql = f"""
        CREATE TABLE `{catalog}`.`{test_schema}`.`{table_name}` (
            id BIGINT,
            sales_data ARRAY<STRUCT<
                region: STRING,
                orders: ARRAY<STRUCT<order_id: INT, amount: DECIMAL(10,2)>>
            >>,
            employee_data ARRAY<STRUCT<
                department: STRING,
                staff: ARRAY<STRUCT<emp_id: INT, salary: DECIMAL(10,2)>>
            >>
        )
        """

        create_table(workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql)

        result = validate_query_equivalence(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["equivalent"], (
            f"Queries are not equivalent!\nDifferences: {result['differences']}"
        )
