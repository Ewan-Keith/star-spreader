"""Functional tests for star-spreader against a real Databricks workspace.

These tests require actual Databricks credentials and a warehouse to run.
They create temporary tables in a test schema, populate them with data,
and validate that the generated SQL returns identical results to SELECT *.

Configuration:
    Set the following environment variables:
    - DATABRICKS_HOST: Your workspace URL
    - DATABRICKS_TOKEN: Your access token
    - DATABRICKS_WAREHOUSE_ID: SQL warehouse HTTP path (e.g., '/sql/1.0/warehouses/abc123xyz')
                                or warehouse ID (e.g., 'abc123xyz')
    - DATABRICKS_CATALOG: Catalog to use (default: 'main')
    - FUNCTIONAL_TEST_SCHEMA: Schema name for tests (default: 'star_spreader_test')
"""

import os
import time
from typing import Any, Generator, List

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from star_spreader.schema.databricks import DatabricksSchemaFetcher
from star_spreader.generator.sql_schema_tree import generate_select_from_schema_tree


@pytest.fixture(scope="module")
def workspace_client() -> WorkspaceClient:
    """Create a WorkspaceClient for the test session."""
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")

    return WorkspaceClient(host=host, token=token)


@pytest.fixture(scope="module")
def warehouse_id() -> str:
    """Get the warehouse ID or HTTP path for running queries.

    Accepts either:
    - HTTP path: /sql/1.0/warehouses/abc123xyz (recommended)
    - Warehouse ID: abc123xyz
    """
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    return str(warehouse_id)


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


def create_and_populate_table(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_name: str,
    create_sql: str,
    insert_sql: str,
) -> None:
    """Helper to create a table and populate it with data."""
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

    # Insert data
    response = workspace_client.statement_execution.execute_statement(
        statement=insert_sql,
        warehouse_id=warehouse_id,
        wait_timeout="30s",
    )

    if response.status and response.status.state == StatementState.FAILED:
        error_msg = response.status.error.message if response.status.error else "Unknown error"
        raise Exception(f"Failed to insert data into {full_table_name}: {error_msg}")


def execute_query(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    query: str,
) -> List[List[Any]]:
    """Execute a query and return the results as a list of rows."""
    response = workspace_client.statement_execution.execute_statement(
        statement=query,
        catalog=catalog,
        schema=schema,
        warehouse_id=warehouse_id,
        wait_timeout="30s",
    )

    if response.status and response.status.state == StatementState.FAILED:
        error_msg = response.status.error.message if response.status.error else "Unknown error"
        raise Exception(f"Query failed: {error_msg}")

    if not response.result or not response.result.data_array:
        return []

    return response.result.data_array


def compare_query_results(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_name: str,
) -> dict:
    """
    End-to-end validation: fetch schema, generate SQL, compare query results.

    Returns a dictionary with comparison results.
    """
    # Step 1: Fetch schema tree
    fetcher = DatabricksSchemaFetcher(workspace_client=workspace_client)
    schema_tree = fetcher.get_schema_tree(catalog=catalog, schema=schema, table=table_name)

    # Step 2: Generate explicit SELECT
    explicit_query = generate_select_from_schema_tree(schema_tree)

    # Step 3: Execute both queries
    full_table_name = f"`{catalog}`.`{schema}`.`{table_name}`"
    select_star_query = f"SELECT * FROM {full_table_name}"

    select_star_results = execute_query(
        workspace_client, warehouse_id, catalog, schema, select_star_query
    )
    explicit_results = execute_query(
        workspace_client, warehouse_id, catalog, schema, explicit_query
    )

    # Step 4: Compare results
    results_match = select_star_results == explicit_results

    return {
        "results_match": results_match,
        "select_star_query": select_star_query,
        "explicit_query": explicit_query,
        "select_star_results": select_star_results,
        "explicit_results": explicit_results,
        "row_count": len(select_star_results),
    }


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

        insert_sql = f"""
        INSERT INTO `{catalog}`.`{test_schema}`.`{table_name}` VALUES
            (1, 'Alice', 30, 75000.50, true, TIMESTAMP '2024-01-15 10:30:00'),
            (2, 'Bob', 25, 65000.00, false, TIMESTAMP '2024-01-16 09:15:00'),
            (3, NULL, 35, NULL, true, TIMESTAMP '2024-01-17 14:45:00')
        """

        create_and_populate_table(
            workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql, insert_sql
        )

        result = compare_query_results(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["results_match"], (
            f"Query results don't match!\n"
            f"Row count: {result['row_count']}\n"
            f"SELECT * results: {result['select_star_results']}\n"
            f"Explicit results: {result['explicit_results']}\n"
            f"Explicit query:\n{result['explicit_query']}"
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

        insert_sql = f"""
        INSERT INTO `{catalog}`.`{test_schema}`.`{table_name}` VALUES
            (1, STRUCT('123 Main St', 'New York', 10001)),
            (2, STRUCT('456 Oak Ave', 'San Francisco', 94102)),
            (3, STRUCT(NULL, 'Chicago', NULL))
        """

        create_and_populate_table(
            workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql, insert_sql
        )

        result = compare_query_results(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["results_match"], (
            f"Query results don't match!\n"
            f"Row count: {result['row_count']}\n"
            f"SELECT * results: {result['select_star_results']}\n"
            f"Explicit results: {result['explicit_results']}\n"
            f"Explicit query:\n{result['explicit_query']}"
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

        insert_sql = f"""
        INSERT INTO `{catalog}`.`{test_schema}`.`{table_name}` VALUES
            (1, STRUCT('Alice', STRUCT('alice@example.com', '555-0001'))),
            (2, STRUCT('Bob', STRUCT('bob@example.com', NULL))),
            (3, STRUCT(NULL, STRUCT(NULL, '555-0003')))
        """

        create_and_populate_table(
            workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql, insert_sql
        )

        result = compare_query_results(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["results_match"], (
            f"Query results don't match!\n"
            f"Row count: {result['row_count']}\n"
            f"SELECT * results: {result['select_star_results']}\n"
            f"Explicit results: {result['explicit_results']}\n"
            f"Explicit query:\n{result['explicit_query']}"
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

        insert_sql = f"""
        INSERT INTO `{catalog}`.`{test_schema}`.`{table_name}` VALUES
            (1, ARRAY('tag1', 'tag2', 'tag3'), ARRAY(95, 87, 92)),
            (2, ARRAY('tag4'), ARRAY(88)),
            (3, ARRAY(), ARRAY())
        """

        create_and_populate_table(
            workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql, insert_sql
        )

        result = compare_query_results(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["results_match"], (
            f"Query results don't match!\n"
            f"Row count: {result['row_count']}\n"
            f"SELECT * results: {result['select_star_results']}\n"
            f"Explicit results: {result['explicit_results']}\n"
            f"Explicit query:\n{result['explicit_query']}"
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

        insert_sql = f"""
        INSERT INTO `{catalog}`.`{test_schema}`.`{table_name}` VALUES
            (1, ARRAY(
                STRUCT(101, 2, 29.99),
                STRUCT(102, 1, 49.99)
            )),
            (2, ARRAY(
                STRUCT(103, 5, 9.99)
            )),
            (3, ARRAY())
        """

        create_and_populate_table(
            workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql, insert_sql
        )

        result = compare_query_results(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["results_match"], (
            f"Query results don't match!\n"
            f"Row count: {result['row_count']}\n"
            f"SELECT * results: {result['select_star_results']}\n"
            f"Explicit results: {result['explicit_results']}\n"
            f"Explicit query:\n{result['explicit_query']}"
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

        insert_sql = f"""
        INSERT INTO `{catalog}`.`{test_schema}`.`{table_name}` VALUES
            (1, ARRAY(
                STRUCT('Engineering', ARRAY(
                    STRUCT('Backend', 10),
                    STRUCT('Frontend', 8)
                )),
                STRUCT('Sales', ARRAY(
                    STRUCT('Enterprise', 5)
                ))
            )),
            (2, ARRAY(
                STRUCT('HR', ARRAY(STRUCT('Recruiting', 3)))
            ))
        """

        create_and_populate_table(
            workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql, insert_sql
        )

        result = compare_query_results(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["results_match"], (
            f"Query results don't match!\n"
            f"Row count: {result['row_count']}\n"
            f"SELECT * results: {result['select_star_results']}\n"
            f"Explicit results: {result['explicit_results']}\n"
            f"Explicit query:\n{result['explicit_query']}"
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

        insert_sql = f"""
        INSERT INTO `{catalog}`.`{test_schema}`.`{table_name}` VALUES
            (1, MAP('key1', 'value1', 'key2', 'value2')),
            (2, MAP('key3', 'value3')),
            (3, MAP())
        """

        create_and_populate_table(
            workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql, insert_sql
        )

        result = compare_query_results(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["results_match"], (
            f"Query results don't match!\n"
            f"Row count: {result['row_count']}\n"
            f"SELECT * results: {result['select_star_results']}\n"
            f"Explicit results: {result['explicit_results']}\n"
            f"Explicit query:\n{result['explicit_query']}"
        )


class TestMixedComplexTypes:
    """Test tables combining multiple complex type patterns."""

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

        insert_sql = f"""
        INSERT INTO `{catalog}`.`{test_schema}`.`{table_name}` VALUES
            (
                1,
                TIMESTAMP '2024-01-15 10:30:00',
                ARRAY('premium', 'verified'),
                STRUCT('Alice', 'alice@example.com', STRUCT('New York', 'USA')),
                ARRAY(
                    STRUCT(1001, STRUCT('UPS', 'TRACK123')),
                    STRUCT(1002, STRUCT('FedEx', 'TRACK456'))
                ),
                MAP('source', 'web', 'campaign', 'summer2024')
            ),
            (
                2,
                TIMESTAMP '2024-01-16 14:20:00',
                ARRAY('new_user'),
                STRUCT('Bob', 'bob@example.com', STRUCT('London', 'UK')),
                ARRAY(
                    STRUCT(2001, STRUCT('DHL', 'TRACK789'))
                ),
                MAP('source', 'mobile')
            )
        """

        create_and_populate_table(
            workspace_client, warehouse_id, catalog, test_schema, table_name, create_sql, insert_sql
        )

        result = compare_query_results(
            workspace_client, warehouse_id, catalog, test_schema, table_name
        )

        assert result["results_match"], (
            f"Query results don't match!\n"
            f"Row count: {result['row_count']}\n"
            f"SELECT * results: {result['select_star_results']}\n"
            f"Explicit results: {result['explicit_results']}\n"
            f"Explicit query:\n{result['explicit_query']}"
        )
