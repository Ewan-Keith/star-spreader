"""Tests for Databricks schema fetcher."""

import pytest
from unittest.mock import MagicMock, Mock

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ColumnInfo as DatabricksColumnInfo, TableInfo

from star_spreader.schema.databricks import DatabricksSchemaFetcher
from star_spreader.schema.base import ColumnInfo, TableSchema


class TestDatabricksSchemaFetcher:
    """Test suite for DatabricksSchemaFetcher."""

    def test_init_with_host_and_token(self) -> None:
        """Test initialization with host and token."""
        fetcher = DatabricksSchemaFetcher(host="https://example.databricks.com", token="test_token")
        assert fetcher.workspace is not None

    def test_init_with_workspace_client(self) -> None:
        """Test initialization with pre-configured workspace client."""
        mock_client = MagicMock(spec=WorkspaceClient)
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)
        assert fetcher.workspace is mock_client

    def test_init_without_credentials_raises_error(self) -> None:
        """Test that initialization without credentials raises ValueError."""
        with pytest.raises(ValueError, match="Either workspace_client or both host and token"):
            DatabricksSchemaFetcher()

    def test_is_complex_type(self) -> None:
        """Test complex type detection."""
        mock_client = MagicMock(spec=WorkspaceClient)
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)

        assert fetcher._is_complex_type("STRUCT<x: INT>")
        assert fetcher._is_complex_type("ARRAY<STRING>")
        assert fetcher._is_complex_type("MAP<STRING, INT>")
        assert not fetcher._is_complex_type("INT")
        assert not fetcher._is_complex_type("STRING")
        assert not fetcher._is_complex_type("DECIMAL(10,2)")

    def test_split_fields(self) -> None:
        """Test splitting struct fields."""
        mock_client = MagicMock(spec=WorkspaceClient)
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)

        # Simple fields
        fields = fetcher._split_fields("field1: INT, field2: STRING")
        assert fields == ["field1: INT", "field2: STRING"]

        # Nested struct
        fields = fetcher._split_fields("field1: INT, field2: STRUCT<nested: STRING>")
        assert fields == ["field1: INT", "field2: STRUCT<nested: STRING>"]

        # Multiple nested levels
        fields = fetcher._split_fields("a: INT, b: STRUCT<x: INT, y: ARRAY<STRING>>, c: STRING")
        assert fields == ["a: INT", "b: STRUCT<x: INT, y: ARRAY<STRING>>", "c: STRING"]

    def test_split_field_definition(self) -> None:
        """Test splitting field name and type."""
        mock_client = MagicMock(spec=WorkspaceClient)
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)

        assert fetcher._split_field_definition("field1: INT") == ("field1", "INT")
        assert fetcher._split_field_definition("my_struct: STRUCT<x: INT>") == (
            "my_struct",
            "STRUCT<x: INT>",
        )

    def test_split_map_key_value(self) -> None:
        """Test splitting MAP key and value types."""
        mock_client = MagicMock(spec=WorkspaceClient)
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)

        assert fetcher._split_map_key_value("STRING, INT") == ["STRING", "INT"]
        assert fetcher._split_map_key_value("STRING, STRUCT<x: INT>") == [
            "STRING",
            "STRUCT<x: INT>",
        ]

    def test_parse_struct_type(self) -> None:
        """Test parsing STRUCT types."""
        mock_client = MagicMock(spec=WorkspaceClient)
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)

        columns = fetcher._parse_struct_type("STRUCT<field1: INT, field2: STRING>")
        assert len(columns) == 2
        assert columns[0].name == "field1"
        assert columns[0].data_type == "INT"
        assert not columns[0].is_complex
        assert columns[1].name == "field2"
        assert columns[1].data_type == "STRING"
        assert not columns[1].is_complex

    def test_parse_array_type(self) -> None:
        """Test parsing ARRAY types."""
        mock_client = MagicMock(spec=WorkspaceClient)
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)

        columns = fetcher._parse_array_type("ARRAY<INT>")
        assert len(columns) == 1
        assert columns[0].name == "element"
        assert columns[0].data_type == "INT"
        assert not columns[0].is_complex

    def test_parse_map_type(self) -> None:
        """Test parsing MAP types."""
        mock_client = MagicMock(spec=WorkspaceClient)
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)

        columns = fetcher._parse_map_type("MAP<STRING, INT>")
        assert len(columns) == 2
        assert columns[0].name == "key"
        assert columns[0].data_type == "STRING"
        assert not columns[0].nullable
        assert columns[1].name == "value"
        assert columns[1].data_type == "INT"
        assert columns[1].nullable

    def test_parse_nested_struct(self) -> None:
        """Test parsing nested STRUCT types."""
        mock_client = MagicMock(spec=WorkspaceClient)
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)

        columns = fetcher._parse_struct_type(
            "STRUCT<field1: INT, nested: STRUCT<x: STRING, y: INT>>"
        )
        assert len(columns) == 2
        assert columns[0].name == "field1"
        assert columns[1].name == "nested"
        assert columns[1].is_complex
        assert columns[1].children is not None
        assert len(columns[1].children) == 2
        assert columns[1].children[0].name == "x"
        assert columns[1].children[1].name == "y"

    def test_fetch_schema(self) -> None:
        """Test fetching schema from Databricks."""
        # Create mock workspace client
        mock_client = MagicMock(spec=WorkspaceClient)

        # Create mock table info
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
                nullable=True,
            ),
            DatabricksColumnInfo(
                name="metadata",
                type_text="STRUCT<created_at: TIMESTAMP, tags: ARRAY<STRING>>",
                type_name="STRUCT",
                nullable=True,
            ),
        ]

        # Configure mock to return the table info
        mock_client.tables.get.return_value = mock_table

        # Create fetcher and fetch schema
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)
        schema = fetcher.fetch_schema("main", "default", "users")

        # Verify results
        assert schema.catalog == "main"
        assert schema.schema_name == "default"
        assert schema.table_name == "users"
        assert len(schema.columns) == 3

        # Check simple columns
        assert schema.columns[0].name == "id"
        assert schema.columns[0].data_type == "BIGINT"
        assert not schema.columns[0].is_complex

        # Check complex column
        assert schema.columns[2].name == "metadata"
        assert schema.columns[2].is_complex
        assert schema.columns[2].children is not None
        assert len(schema.columns[2].children) == 2
        assert schema.columns[2].children[0].name == "created_at"
        assert schema.columns[2].children[1].name == "tags"
        assert schema.columns[2].children[1].is_complex

        # Verify the API was called correctly
        mock_client.tables.get.assert_called_once_with(full_name="main.default.users")

    def test_parse_column_with_enum_type_name(self) -> None:
        """Test that _parse_column correctly handles ColumnTypeName enum objects."""
        from enum import Enum

        # Create a mock ColumnTypeName enum class
        class MockColumnTypeName(Enum):
            STRING = "STRING"
            INT = "INT"
            STRUCT = "STRUCT"

        # Create mock workspace client
        mock_client = MagicMock(spec=WorkspaceClient)
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)

        # Create a mock DatabricksColumnInfo with enum type_name
        mock_column = Mock(spec=DatabricksColumnInfo)
        mock_column.name = "test_column"
        mock_column.type_text = "STRING"
        mock_column.type_name = MockColumnTypeName.STRING
        mock_column.nullable = True

        # Test that _parse_column handles the enum correctly
        result = fetcher._parse_column(mock_column)

        # Verify the result
        assert result.name == "test_column"
        assert result.data_type == "STRING"
        assert not result.is_complex
        assert result.nullable is True
        assert result.children is None
