"""Tests for Databricks schema fetcher."""

import pytest
from unittest.mock import MagicMock, Mock

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ColumnInfo as DatabricksColumnInfo, TableInfo

from star_spreader.schema.databricks import DatabricksSchemaFetcher


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
        from star_spreader.schema_tree.nodes import StructNode, SimpleColumnNode

        mock_client = MagicMock(spec=WorkspaceClient)
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)

        node = fetcher._parse_struct_type(
            "my_struct", "STRUCT<field1: INT, field2: STRING>", nullable=True
        )
        assert isinstance(node, StructNode)
        assert node.name == "my_struct"
        assert len(node.fields) == 2
        assert node.fields[0].name == "field1"
        assert node.fields[0].data_type == "INT"
        assert isinstance(node.fields[0], SimpleColumnNode)
        assert node.fields[1].name == "field2"
        assert node.fields[1].data_type == "STRING"
        assert isinstance(node.fields[1], SimpleColumnNode)

    def test_parse_array_type(self) -> None:
        """Test parsing ARRAY types."""
        from star_spreader.schema_tree.nodes import ArrayNode, SimpleColumnNode

        mock_client = MagicMock(spec=WorkspaceClient)
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)

        node = fetcher._parse_array_type("my_array", "ARRAY<INT>", nullable=True)
        assert isinstance(node, ArrayNode)
        assert node.name == "my_array"
        assert isinstance(node.element_type, SimpleColumnNode)
        assert node.element_type.name == "element"
        assert node.element_type.data_type == "INT"

    def test_parse_map_type(self) -> None:
        """Test parsing MAP types."""
        from star_spreader.schema_tree.nodes import MapNode, SimpleColumnNode

        mock_client = MagicMock(spec=WorkspaceClient)
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)

        node = fetcher._parse_map_type("my_map", "MAP<STRING, INT>", nullable=True)
        assert isinstance(node, MapNode)
        assert node.name == "my_map"
        assert isinstance(node.key_type, SimpleColumnNode)
        assert node.key_type.name == "key"
        assert node.key_type.data_type == "STRING"
        assert not node.key_type.nullable
        assert isinstance(node.value_type, SimpleColumnNode)
        assert node.value_type.name == "value"
        assert node.value_type.data_type == "INT"
        assert node.value_type.nullable

    def test_parse_nested_struct(self) -> None:
        """Test parsing nested STRUCT types."""
        from star_spreader.schema_tree.nodes import StructNode, SimpleColumnNode

        mock_client = MagicMock(spec=WorkspaceClient)
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)

        node = fetcher._parse_struct_type(
            "my_struct", "STRUCT<field1: INT, nested: STRUCT<x: STRING, y: INT>>", nullable=True
        )
        assert isinstance(node, StructNode)
        assert len(node.fields) == 2
        assert node.fields[0].name == "field1"
        assert isinstance(node.fields[0], SimpleColumnNode)
        assert node.fields[1].name == "nested"
        assert isinstance(node.fields[1], StructNode)
        assert len(node.fields[1].fields) == 2
        assert node.fields[1].fields[0].name == "x"
        assert node.fields[1].fields[1].name == "y"

    def test_get_schema_tree(self) -> None:
        """Test fetching schema tree from Databricks."""
        from star_spreader.schema_tree.nodes import ArrayNode, SimpleColumnNode, StructNode

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

        # Create fetcher and fetch schema tree
        fetcher = DatabricksSchemaFetcher(workspace_client=mock_client)
        schema_tree = fetcher.get_schema_tree("main", "default", "users")

        # Verify results
        assert schema_tree.catalog == "main"
        assert schema_tree.schema_name == "default"
        assert schema_tree.table_name == "users"
        assert len(schema_tree.columns) == 3

        # Check simple columns
        assert isinstance(schema_tree.columns[0], SimpleColumnNode)
        assert schema_tree.columns[0].name == "id"
        assert schema_tree.columns[0].data_type == "BIGINT"

        assert isinstance(schema_tree.columns[1], SimpleColumnNode)
        assert schema_tree.columns[1].name == "name"
        assert schema_tree.columns[1].data_type == "STRING"

        # Check complex column (STRUCT)
        assert isinstance(schema_tree.columns[2], StructNode)
        assert schema_tree.columns[2].name == "metadata"
        assert len(schema_tree.columns[2].fields) == 2

        # Check struct fields
        assert isinstance(schema_tree.columns[2].fields[0], SimpleColumnNode)
        assert schema_tree.columns[2].fields[0].name == "created_at"
        assert schema_tree.columns[2].fields[0].data_type == "TIMESTAMP"

        assert isinstance(schema_tree.columns[2].fields[1], ArrayNode)
        assert schema_tree.columns[2].fields[1].name == "tags"
        assert isinstance(schema_tree.columns[2].fields[1].element_type, SimpleColumnNode)
        assert schema_tree.columns[2].fields[1].element_type.data_type == "STRING"

        # Verify the API was called correctly
        mock_client.tables.get.assert_called_once_with(full_name="main.default.users")

    def test_parse_column_with_enum_type_name(self) -> None:
        """Test that _parse_column correctly handles ColumnTypeName enum objects."""
        from enum import Enum
        from star_spreader.schema_tree.nodes import SimpleColumnNode

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
        assert isinstance(result, SimpleColumnNode)
        assert result.name == "test_column"
        assert result.data_type == "STRING"
        assert result.nullable is True
