"""Integration tests for star-spreader end-to-end workflow.

This module tests the complete workflow of fetching schema, generating SQL,
and validating query equivalence. It uses mocked Databricks responses to
test various scenarios without requiring a live connection.
"""

import pytest
from unittest.mock import MagicMock, Mock, patch

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ColumnInfo as DatabricksColumnInfo, TableInfo
from databricks.sdk.service.sql import StatementState, StatementStatus, ResultData

from star_spreader.schema.databricks import DatabricksSchemaFetcher
from star_spreader.schema.base import ColumnInfo, TableSchema
from star_spreader.generator.sql import SQLGenerator
from star_spreader.validator.explain import ExplainValidator


class TestEndToEndIntegration:
    """Integration tests for the complete star-spreader workflow."""

    def test_simple_table_workflow(self) -> None:
        """Test end-to-end workflow with a simple table (no complex types)."""
        # -------------------------------------------------------------------------
        # Setup: Create mock Databricks client with simple table schema
        # -------------------------------------------------------------------------
        mock_client = MagicMock(spec=WorkspaceClient)

        # Mock table with simple columns: id, name, email, created_at
        mock_table = Mock(spec=TableInfo)
        mock_table.columns = [
            DatabricksColumnInfo(
                name="id",
                type_text="BIGINT",
                type_name="BIGINT",
                nullable=False,
            ),
            DatabricksColumnInfo(
                name="name",
                type_text="STRING",
                type_name="STRING",
                nullable=False,
            ),
            DatabricksColumnInfo(
                name="email",
                type_text="STRING",
                type_name="STRING",
                nullable=True,
            ),
            DatabricksColumnInfo(
                name="created_at",
                type_text="TIMESTAMP",
                type_name="TIMESTAMP",
                nullable=False,
            ),
        ]
        mock_client.tables.get.return_value = mock_table

        # -------------------------------------------------------------------------
        # Step 1: Fetch schema
        # -------------------------------------------------------------------------
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)
        schema = fetcher.fetch_schema("main", "default", "users")

        # Verify schema was fetched correctly
        assert schema.catalog == "main"
        assert schema.schema_name == "default"
        assert schema.table_name == "users"
        assert len(schema.columns) == 4

        # Verify all columns are simple (not complex)
        for col in schema.columns:
            assert not col.is_complex
            assert col.children is None

        # -------------------------------------------------------------------------
        # Step 2: Generate explicit SELECT
        # -------------------------------------------------------------------------
        generator = SQLGenerator(schema)
        explicit_select = generator.generate_select()

        # Verify the generated SQL
        expected_sql = """SELECT `id`,
       `name`,
       `email`,
       `created_at`
FROM `main`.`default`.`users`"""
        assert explicit_select == expected_sql

        # -------------------------------------------------------------------------
        # Step 3: Verify the workflow produces valid SQL
        # -------------------------------------------------------------------------
        # Check that the SQL contains all expected column names
        assert "`id`" in explicit_select
        assert "`name`" in explicit_select
        assert "`email`" in explicit_select
        assert "`created_at`" in explicit_select
        assert "FROM `main`.`default`.`users`" in explicit_select

    def test_struct_column_workflow(self) -> None:
        """Test end-to-end workflow with a table containing STRUCT columns."""
        # -------------------------------------------------------------------------
        # Setup: Create mock table with STRUCT column
        # -------------------------------------------------------------------------
        mock_client = MagicMock(spec=WorkspaceClient)

        # Mock table: id, name, address (STRUCT)
        mock_table = Mock(spec=TableInfo)
        mock_table.columns = [
            DatabricksColumnInfo(
                name="id",
                type_text="BIGINT",
                type_name="BIGINT",
                nullable=False,
            ),
            DatabricksColumnInfo(
                name="name",
                type_text="STRING",
                type_name="STRING",
                nullable=False,
            ),
            DatabricksColumnInfo(
                name="address",
                type_text="STRUCT<street: STRING, city: STRING, state: STRING, zip: STRING>",
                type_name="STRUCT",
                nullable=True,
            ),
        ]
        mock_client.tables.get.return_value = mock_table

        # -------------------------------------------------------------------------
        # Step 1: Fetch schema
        # -------------------------------------------------------------------------
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)
        schema = fetcher.fetch_schema("main", "default", "customers")

        # Verify the address column was parsed as a struct
        address_col = schema.columns[2]
        assert address_col.name == "address"
        assert address_col.is_complex
        assert address_col.children is not None
        assert len(address_col.children) == 4

        # Verify struct fields
        assert address_col.children[0].name == "street"
        assert address_col.children[1].name == "city"
        assert address_col.children[2].name == "state"
        assert address_col.children[3].name == "zip"

        # -------------------------------------------------------------------------
        # Step 2: Generate explicit SELECT with struct reconstruction
        # -------------------------------------------------------------------------
        generator = SQLGenerator(schema)
        explicit_select = generator.generate_select()

        # Verify struct fields are explicitly selected and reconstructed
        assert "STRUCT(" in explicit_select
        assert "`address`.`street`" in explicit_select
        assert "`address`.`city`" in explicit_select
        assert "`address`.`state`" in explicit_select
        assert "`address`.`zip`" in explicit_select
        assert "AS `address`" in explicit_select

        # Verify the complete SQL structure
        expected_sql = """SELECT `id`,
       `name`,
       STRUCT(`address`.`street` AS `street`, `address`.`city` AS `city`, `address`.`state` AS `state`, `address`.`zip` AS `zip`) AS `address`
FROM `main`.`default`.`customers`"""
        assert explicit_select == expected_sql

    def test_nested_struct_workflow(self) -> None:
        """Test end-to-end workflow with nested STRUCT columns."""
        # -------------------------------------------------------------------------
        # Setup: Create mock table with nested struct
        # -------------------------------------------------------------------------
        mock_client = MagicMock(spec=WorkspaceClient)

        # Mock table with nested struct: user (STRUCT with contact STRUCT inside)
        mock_table = Mock(spec=TableInfo)
        mock_table.columns = [
            DatabricksColumnInfo(
                name="id",
                type_text="BIGINT",
                type_name="BIGINT",
                nullable=False,
            ),
            DatabricksColumnInfo(
                name="user",
                type_text="STRUCT<name: STRING, contact: STRUCT<email: STRING, phone: STRING>>",
                type_name="STRUCT",
                nullable=True,
            ),
        ]
        mock_client.tables.get.return_value = mock_table

        # -------------------------------------------------------------------------
        # Step 1: Fetch schema
        # -------------------------------------------------------------------------
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)
        schema = fetcher.fetch_schema("main", "default", "profiles")

        # Verify nested structure
        user_col = schema.columns[1]
        assert user_col.name == "user"
        assert user_col.is_complex
        assert len(user_col.children) == 2

        # Verify the nested contact field
        contact_col = user_col.children[1]
        assert contact_col.name == "contact"
        assert contact_col.is_complex
        assert len(contact_col.children) == 2
        assert contact_col.children[0].name == "email"
        assert contact_col.children[1].name == "phone"

        # -------------------------------------------------------------------------
        # Step 2: Generate explicit SELECT with nested struct reconstruction
        # -------------------------------------------------------------------------
        generator = SQLGenerator(schema)
        explicit_select = generator.generate_select()

        # Verify nested struct fields are explicitly selected and reconstructed
        assert "STRUCT(" in explicit_select
        assert "`user`.`name`" in explicit_select
        assert "`user`.`contact`.`email`" in explicit_select
        assert "`user`.`contact`.`phone`" in explicit_select
        assert "AS `user`" in explicit_select

        # Verify the complete SQL structure
        expected_sql = """SELECT `id`,
       STRUCT(`user`.`name` AS `name`, STRUCT(`user`.`contact`.`email` AS `email`, `user`.`contact`.`phone` AS `phone`) AS `contact`) AS `user`
FROM `main`.`default`.`profiles`"""
        assert explicit_select == expected_sql

    def test_array_column_workflow(self) -> None:
        """Test end-to-end workflow with ARRAY columns."""
        # -------------------------------------------------------------------------
        # Setup: Create mock table with ARRAY columns
        # -------------------------------------------------------------------------
        mock_client = MagicMock(spec=WorkspaceClient)

        # Mock table with array: id, tags (ARRAY<STRING>), categories (ARRAY<STRING>)
        mock_table = Mock(spec=TableInfo)
        mock_table.columns = [
            DatabricksColumnInfo(
                name="id",
                type_text="BIGINT",
                type_name="BIGINT",
                nullable=False,
            ),
            DatabricksColumnInfo(
                name="tags",
                type_text="ARRAY<STRING>",
                type_name="ARRAY",
                nullable=True,
            ),
            DatabricksColumnInfo(
                name="categories",
                type_text="ARRAY<STRING>",
                type_name="ARRAY",
                nullable=True,
            ),
        ]
        mock_client.tables.get.return_value = mock_table

        # -------------------------------------------------------------------------
        # Step 1: Fetch schema
        # -------------------------------------------------------------------------
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)
        schema = fetcher.fetch_schema("main", "default", "products")

        # Verify array columns are marked as complex
        assert schema.columns[1].is_complex
        assert schema.columns[1].children is not None
        assert schema.columns[2].is_complex

        # -------------------------------------------------------------------------
        # Step 2: Generate explicit SELECT (arrays not expanded)
        # -------------------------------------------------------------------------
        generator = SQLGenerator(schema)
        explicit_select = generator.generate_select()

        # Arrays should be included as-is without expansion
        expected_sql = """SELECT `id`,
       `tags`,
       `categories`
FROM `main`.`default`.`products`"""
        assert explicit_select == expected_sql

    def test_array_of_struct_workflow(self) -> None:
        """Test end-to-end workflow with ARRAY<STRUCT<...>> columns."""
        # -------------------------------------------------------------------------
        # Setup: Create mock table with array of structs
        # -------------------------------------------------------------------------
        mock_client = MagicMock(spec=WorkspaceClient)

        # Mock table with array of structs: id, line_items (ARRAY<STRUCT<...>>)
        mock_table = Mock(spec=TableInfo)
        mock_table.columns = [
            DatabricksColumnInfo(
                name="id",
                type_text="BIGINT",
                type_name="BIGINT",
                nullable=False,
            ),
            DatabricksColumnInfo(
                name="line_items",
                type_text="ARRAY<STRUCT<product_id: INT, quantity: INT, price: DECIMAL>>",
                type_name="ARRAY",
                nullable=True,
            ),
        ]
        mock_client.tables.get.return_value = mock_table

        # -------------------------------------------------------------------------
        # Step 1: Fetch schema
        # -------------------------------------------------------------------------
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)
        schema = fetcher.fetch_schema("main", "default", "orders")

        # Verify array of struct is parsed correctly
        line_items_col = schema.columns[1]
        assert line_items_col.is_complex
        assert line_items_col.children is not None
        assert len(line_items_col.children) == 1

        # Verify the element is a struct
        element_col = line_items_col.children[0]
        assert element_col.name == "element"
        assert element_col.is_complex
        assert len(element_col.children) == 3

        # -------------------------------------------------------------------------
        # Step 2: Generate explicit SELECT with TRANSFORM
        # -------------------------------------------------------------------------
        generator = SQLGenerator(schema)
        explicit_select = generator.generate_select()

        # Verify TRANSFORM is used to reconstruct array of structs
        assert "TRANSFORM(" in explicit_select
        assert "`line_items`" in explicit_select
        assert "item ->" in explicit_select
        assert "item.`product_id`" in explicit_select
        assert "item.`quantity`" in explicit_select
        assert "item.`price`" in explicit_select

        # Verify the complete SQL structure
        expected_sql = """SELECT `id`,
       TRANSFORM(`line_items`, item -> STRUCT(item.`product_id` AS `product_id`, item.`quantity` AS `quantity`, item.`price` AS `price`)) AS `line_items`
FROM `main`.`default`.`orders`"""
        assert explicit_select == expected_sql

    def test_mixed_complex_types_workflow(self) -> None:
        """Test workflow with a table containing mixed complex types."""
        # -------------------------------------------------------------------------
        # Setup: Create mock table with STRUCT, ARRAY, and simple columns
        # -------------------------------------------------------------------------
        mock_client = MagicMock(spec=WorkspaceClient)

        mock_table = Mock(spec=TableInfo)
        mock_table.columns = [
            DatabricksColumnInfo(
                name="id",
                type_text="BIGINT",
                type_name="BIGINT",
                nullable=False,
            ),
            DatabricksColumnInfo(
                name="name",
                type_text="STRING",
                type_name="STRING",
                nullable=False,
            ),
            DatabricksColumnInfo(
                name="metadata",
                type_text="STRUCT<created_at: TIMESTAMP, updated_at: TIMESTAMP>",
                type_name="STRUCT",
                nullable=True,
            ),
            DatabricksColumnInfo(
                name="tags",
                type_text="ARRAY<STRING>",
                type_name="ARRAY",
                nullable=True,
            ),
        ]
        mock_client.tables.get.return_value = mock_table

        # -------------------------------------------------------------------------
        # Complete workflow test
        # -------------------------------------------------------------------------
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)
        schema = fetcher.fetch_schema("main", "analytics", "events")

        generator = SQLGenerator(schema)
        explicit_select = generator.generate_select()

        # Verify struct is reconstructed and array is preserved as-is
        expected_sql = """SELECT `id`,
       `name`,
       STRUCT(`metadata`.`created_at` AS `created_at`, `metadata`.`updated_at` AS `updated_at`) AS `metadata`,
       `tags`
FROM `main`.`analytics`.`events`"""
        assert explicit_select == expected_sql

    def test_validation_workflow_with_mock_explain(self) -> None:
        """Test the validation workflow with mocked EXPLAIN results."""
        # -------------------------------------------------------------------------
        # Setup: Create mocks for schema fetching and EXPLAIN execution
        # -------------------------------------------------------------------------
        mock_client = MagicMock(spec=WorkspaceClient)

        # Mock simple table
        mock_table = Mock(spec=TableInfo)
        mock_table.columns = [
            DatabricksColumnInfo(
                name="id",
                type_text="BIGINT",
                type_name="BIGINT",
                nullable=False,
            ),
            DatabricksColumnInfo(
                name="name",
                type_text="STRING",
                type_name="STRING",
                nullable=False,
            ),
        ]
        mock_client.tables.get.return_value = mock_table

        # Mock EXPLAIN execution to return identical plans
        mock_explain_response = Mock()
        mock_explain_response.status = Mock(spec=StatementStatus)
        mock_explain_response.status.state = StatementState.SUCCEEDED
        mock_explain_response.result = Mock(spec=ResultData)

        # Simplified EXPLAIN plan for testing
        explain_plan = [
            ["== Analyzed Logical Plan =="],
            ["Project [id#1L, name#2]"],
            ["+- Relation[id#1L,name#2] parquet"],
        ]
        mock_explain_response.result.data_array = explain_plan

        mock_client.statement_execution.execute_statement.return_value = mock_explain_response

        # -------------------------------------------------------------------------
        # Step 1: Fetch schema and generate SQL
        # -------------------------------------------------------------------------
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)
        schema = fetcher.fetch_schema("main", "default", "test_table")

        generator = SQLGenerator(schema)
        explicit_select = generator.generate_select()

        # -------------------------------------------------------------------------
        # Step 2: Validate equivalence
        # -------------------------------------------------------------------------
        validator = ExplainValidator(
            workspace_client=mock_client,
            warehouse_id="test-warehouse-123",
        )

        select_star_query = "SELECT * FROM `main`.`default`.`test_table`"
        result = validator.validate_equivalence(
            select_star_query=select_star_query,
            explicit_query=explicit_select,
            catalog="main",
            schema="default",
        )

        # -------------------------------------------------------------------------
        # Verify validation results
        # -------------------------------------------------------------------------
        # Both queries should return identical EXPLAIN plans
        assert result["equivalent"] is True
        assert result["select_star_plan"] is not None
        assert result["explicit_plan"] is not None
        assert result["differences"] is None

        # Verify EXPLAIN was called twice (once for each query)
        assert mock_client.statement_execution.execute_statement.call_count == 2

    def test_validation_detects_differences(self) -> None:
        """Test that validation correctly detects when queries are not equivalent."""
        # -------------------------------------------------------------------------
        # Setup: Create mocks that return different EXPLAIN plans
        # -------------------------------------------------------------------------
        mock_client = MagicMock(spec=WorkspaceClient)

        # Create two different EXPLAIN plan responses
        def mock_execute_statement(**kwargs):
            mock_response = Mock()
            mock_response.status = Mock(spec=StatementStatus)
            mock_response.status.state = StatementState.SUCCEEDED
            mock_response.result = Mock(spec=ResultData)

            # Return different plans based on the query
            if "SELECT *" in kwargs["statement"]:
                mock_response.result.data_array = [
                    ["== Analyzed Logical Plan =="],
                    ["Project [id#1L, name#2, email#3]"],
                ]
            else:
                mock_response.result.data_array = [
                    ["== Analyzed Logical Plan =="],
                    ["Project [id#1L, name#2]"],  # Missing email column
                ]

            return mock_response

        mock_client.statement_execution.execute_statement.side_effect = mock_execute_statement

        # -------------------------------------------------------------------------
        # Test validation with different queries
        # -------------------------------------------------------------------------
        validator = ExplainValidator(
            workspace_client=mock_client,
            warehouse_id="test-warehouse-123",
        )

        result = validator.validate_equivalence(
            select_star_query="SELECT * FROM test_table",
            explicit_query="SELECT id, name FROM test_table",  # Missing email
            catalog="main",
            schema="default",
        )

        # -------------------------------------------------------------------------
        # Verify differences are detected
        # -------------------------------------------------------------------------
        assert result["equivalent"] is False
        assert result["differences"] is not None
        # The differences should mention the mismatch in plans
        assert "differ" in result["differences"].lower()

    def test_complex_table_with_all_types(self) -> None:
        """Test workflow with a table containing all supported complex types."""
        # -------------------------------------------------------------------------
        # Setup: Create a comprehensive mock table
        # -------------------------------------------------------------------------
        mock_client = MagicMock(spec=WorkspaceClient)

        mock_table = Mock(spec=TableInfo)
        mock_table.columns = [
            # Simple types
            DatabricksColumnInfo(
                name="id",
                type_text="BIGINT",
                type_name="BIGINT",
                nullable=False,
            ),
            DatabricksColumnInfo(
                name="name",
                type_text="STRING",
                type_name="STRING",
                nullable=False,
            ),
            # STRUCT with nested STRUCT
            DatabricksColumnInfo(
                name="profile",
                type_text="STRUCT<bio: STRING, location: STRUCT<city: STRING, country: STRING>>",
                type_name="STRUCT",
                nullable=True,
            ),
            # ARRAY
            DatabricksColumnInfo(
                name="tags",
                type_text="ARRAY<STRING>",
                type_name="ARRAY",
                nullable=True,
            ),
            # MAP
            DatabricksColumnInfo(
                name="attributes",
                type_text="MAP<STRING, STRING>",
                type_name="MAP",
                nullable=True,
            ),
        ]
        mock_client.tables.get.return_value = mock_table

        # -------------------------------------------------------------------------
        # Execute complete workflow
        # -------------------------------------------------------------------------
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)
        schema = fetcher.fetch_schema("main", "default", "complex_table")

        # Verify schema parsing
        assert len(schema.columns) == 5

        # Verify simple columns
        assert not schema.columns[0].is_complex
        assert not schema.columns[1].is_complex

        # Verify STRUCT parsing
        profile_col = schema.columns[2]
        assert profile_col.is_complex
        assert len(profile_col.children) == 2
        assert profile_col.children[0].name == "bio"
        assert profile_col.children[1].name == "location"
        assert profile_col.children[1].is_complex

        # Verify ARRAY parsing
        assert schema.columns[3].is_complex
        assert schema.columns[3].children is not None

        # Verify MAP parsing
        assert schema.columns[4].is_complex
        assert schema.columns[4].children is not None
        assert len(schema.columns[4].children) == 2  # key and value

        # -------------------------------------------------------------------------
        # Generate SQL and verify struct reconstruction
        # -------------------------------------------------------------------------
        generator = SQLGenerator(schema)
        explicit_select = generator.generate_select()

        # Verify all column types are handled correctly
        assert "`id`" in explicit_select
        assert "`name`" in explicit_select
        assert "STRUCT(" in explicit_select  # Struct reconstructed
        assert "`profile`.`bio`" in explicit_select
        assert "`profile`.`location`.`city`" in explicit_select
        assert "`profile`.`location`.`country`" in explicit_select
        assert "`tags`" in explicit_select  # Array preserved as-is
        assert "`attributes`" in explicit_select  # Map preserved as-is

        # Verify the complete SQL structure
        expected_sql = """SELECT `id`,
       `name`,
       STRUCT(`profile`.`bio` AS `bio`, STRUCT(`profile`.`location`.`city` AS `city`, `profile`.`location`.`country` AS `country`) AS `location`) AS `profile`,
       `tags`,
       `attributes`
FROM `main`.`default`.`complex_table`"""
        assert explicit_select == expected_sql


class TestErrorHandling:
    """Test error handling in the integration workflow."""

    def test_invalid_table_name(self) -> None:
        """Test that fetching a non-existent table raises an appropriate error."""
        mock_client = MagicMock(spec=WorkspaceClient)
        mock_client.tables.get.side_effect = Exception("Table not found")

        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)

        with pytest.raises(Exception, match="Table not found"):
            fetcher.fetch_schema("main", "default", "nonexistent_table")

    def test_explain_query_failure(self) -> None:
        """Test handling of EXPLAIN query failures."""
        mock_client = MagicMock(spec=WorkspaceClient)

        # Mock a failed EXPLAIN execution
        mock_response = Mock()
        mock_response.status = Mock(spec=StatementStatus)
        mock_response.status.state = StatementState.FAILED
        mock_response.status.error = Mock()
        mock_response.status.error.message = "SQL syntax error"

        mock_client.statement_execution.execute_statement.return_value = mock_response

        validator = ExplainValidator(
            workspace_client=mock_client,
            warehouse_id="test-warehouse",
        )

        with pytest.raises(Exception, match="EXPLAIN query failed"):
            validator.validate_equivalence(
                select_star_query="SELECT * FROM invalid_syntax_table",
                explicit_query="SELECT id FROM test",
                catalog="main",
                schema="default",
            )
