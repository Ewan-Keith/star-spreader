"""Unit tests for the schema tree module.

These tests demonstrate the new schema tree-based architecture and ensure
the schema tree builder, nodes, and SQL generation work correctly.
"""

import pytest
from star_spreader.schema_tree.nodes import (
    ArrayNode,
    MapNode,
    SimpleColumnNode,
    StructNode,
    TableSchemaNode,
)
from star_spreader.schema_tree.builder import SchemaTreeBuilder
from star_spreader.generator.sql_schema_tree import SchemaTreeSQLGenerator
from star_spreader.schema.base import ColumnInfo, TableSchema


def test_simple_column_ast():
    """Test simple column schema tree node creation."""
    node = SimpleColumnNode(name="id", data_type="INT", nullable=False)

    assert node.name == "id"
    assert node.data_type == "INT"
    assert node.nullable is False


def test_struct_node_ast():
    """Test struct schema tree node creation."""
    field1 = SimpleColumnNode(name="name", data_type="STRING", nullable=True)
    field2 = SimpleColumnNode(name="age", data_type="INT", nullable=True)

    struct_node = StructNode(
        name="person",
        data_type="STRUCT<name: STRING, age: INT>",
        nullable=True,
        fields=[field1, field2],
    )

    assert struct_node.name == "person"
    assert len(struct_node.fields) == 2
    assert struct_node.fields[0].name == "name"
    assert struct_node.fields[1].name == "age"


def test_array_node_ast():
    """Test array schema tree node creation."""
    element = SimpleColumnNode(name="element", data_type="INT", nullable=True)

    array_node = ArrayNode(
        name="numbers", data_type="ARRAY<INT>", nullable=True, element_type=element
    )

    assert array_node.name == "numbers"
    assert array_node.element_type.name == "element"
    assert array_node.element_type.data_type == "INT"


def test_map_node_ast():
    """Test map schema tree node creation."""
    key = SimpleColumnNode(name="key", data_type="STRING", nullable=False)
    value = SimpleColumnNode(name="value", data_type="INT", nullable=True)

    map_node = MapNode(
        name="tags", data_type="MAP<STRING, INT>", nullable=True, key_type=key, value_type=value
    )

    assert map_node.name == "tags"
    assert map_node.key_type.data_type == "STRING"
    assert map_node.value_type.data_type == "INT"


def test_ast_builder_simple_columns():
    """Test schema tree builder with simple columns."""
    schema = TableSchema(
        catalog="test_cat",
        schema_name="test_schema",
        table_name="test_table",
        columns=[
            ColumnInfo(name="id", data_type="INT", is_complex=False, nullable=False),
            ColumnInfo(name="name", data_type="STRING", is_complex=False, nullable=True),
        ],
    )

    ast = SchemaTreeBuilder.build_from_table_schema(schema)

    assert isinstance(ast, TableSchemaNode)
    assert ast.catalog == "test_cat"
    assert ast.schema_name == "test_schema"
    assert ast.table_name == "test_table"
    assert len(ast.columns) == 2
    assert isinstance(ast.columns[0], SimpleColumnNode)
    assert isinstance(ast.columns[1], SimpleColumnNode)


def test_ast_builder_struct():
    """Test schema tree builder with struct column."""
    schema = TableSchema(
        catalog="test_cat",
        schema_name="test_schema",
        table_name="test_table",
        columns=[
            ColumnInfo(
                name="person",
                data_type="STRUCT<name: STRING, age: INT>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(name="name", data_type="STRING", is_complex=False, nullable=True),
                    ColumnInfo(name="age", data_type="INT", is_complex=False, nullable=True),
                ],
            )
        ],
    )

    ast = SchemaTreeBuilder.build_from_table_schema(schema)

    assert len(ast.columns) == 1
    assert isinstance(ast.columns[0], StructNode)
    assert ast.columns[0].name == "person"
    assert len(ast.columns[0].fields) == 2


def test_ast_builder_array_of_struct():
    """Test schema tree builder with array of struct."""
    schema = TableSchema(
        catalog="test_cat",
        schema_name="test_schema",
        table_name="test_table",
        columns=[
            ColumnInfo(
                name="items",
                data_type="ARRAY<STRUCT<id: INT, name: STRING>>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(
                        name="element",
                        data_type="STRUCT<id: INT, name: STRING>",
                        is_complex=True,
                        nullable=True,
                        children=[
                            ColumnInfo(name="id", data_type="INT", is_complex=False, nullable=True),
                            ColumnInfo(
                                name="name", data_type="STRING", is_complex=False, nullable=True
                            ),
                        ],
                    )
                ],
            )
        ],
    )

    ast = SchemaTreeBuilder.build_from_table_schema(schema)

    assert len(ast.columns) == 1
    assert isinstance(ast.columns[0], ArrayNode)
    assert ast.columns[0].name == "items"
    assert isinstance(ast.columns[0].element_type, StructNode)
    assert len(ast.columns[0].element_type.fields) == 2


def test_ast_sql_generation_simple():
    """Test SQL generation from schema tree for simple columns."""
    ast = TableSchemaNode(
        catalog="test_cat",
        schema_name="test_schema",
        table_name="test_table",
        columns=[
            SimpleColumnNode(name="id", data_type="INT", nullable=False),
            SimpleColumnNode(name="name", data_type="STRING", nullable=True),
        ],
    )

    generator = SchemaTreeSQLGenerator(ast)
    sql = generator.generate_select()

    assert "`id`" in sql
    assert "`name`" in sql
    assert "FROM `test_cat`.`test_schema`.`test_table`" in sql


def test_ast_sql_generation_struct():
    """Test SQL generation from schema tree for struct."""
    ast = TableSchemaNode(
        catalog="test_cat",
        schema_name="test_schema",
        table_name="test_table",
        columns=[
            StructNode(
                name="person",
                data_type="STRUCT<name: STRING, age: INT>",
                nullable=True,
                fields=[
                    SimpleColumnNode(name="name", data_type="STRING", nullable=True),
                    SimpleColumnNode(name="age", data_type="INT", nullable=True),
                ],
            )
        ],
    )

    generator = SchemaTreeSQLGenerator(ast)
    sql = generator.generate_select()

    assert "STRUCT(" in sql
    assert "`person`.`name`" in sql
    assert "`person`.`age`" in sql
    assert "AS `person`" in sql


def test_ast_sql_generation_array_of_struct():
    """Test SQL generation from schema tree for array of struct."""
    ast = TableSchemaNode(
        catalog="test_cat",
        schema_name="test_schema",
        table_name="test_table",
        columns=[
            ArrayNode(
                name="items",
                data_type="ARRAY<STRUCT<id: INT, name: STRING>>",
                nullable=True,
                element_type=StructNode(
                    name="element",
                    data_type="STRUCT<id: INT, name: STRING>",
                    nullable=True,
                    fields=[
                        SimpleColumnNode(name="id", data_type="INT", nullable=True),
                        SimpleColumnNode(name="name", data_type="STRING", nullable=True),
                    ],
                ),
            )
        ],
    )

    generator = SchemaTreeSQLGenerator(ast)
    sql = generator.generate_select()

    assert "TRANSFORM(" in sql
    assert "item ->" in sql
    assert "item.`id`" in sql
    assert "item.`name`" in sql


def test_fetch_schema_ast_method():
    """Test that SchemaFetcher.fetch_schema_ast() returns schema tree."""
    from unittest.mock import Mock
    from star_spreader.schema.base import SchemaFetcher

    # Create a mock fetcher
    class MockFetcher(SchemaFetcher):
        def fetch_schema(self, catalog: str, schema: str, table: str) -> TableSchema:
            return TableSchema(
                catalog=catalog,
                schema_name=schema,
                table_name=table,
                columns=[
                    ColumnInfo(name="id", data_type="INT", is_complex=False, nullable=False),
                ],
            )

    fetcher = MockFetcher()
    ast = fetcher.fetch_schema_ast("cat", "schema", "table")

    assert isinstance(ast, TableSchemaNode)
    assert ast.catalog == "cat"
    assert ast.schema_name == "schema"
    assert ast.table_name == "table"
    assert len(ast.columns) == 1
    assert isinstance(ast.columns[0], SimpleColumnNode)


def test_ast_distinguishes_map_from_struct_with_key_value():
    """Test that schema tree builder correctly distinguishes MAP from STRUCT with key/value fields."""
    # STRUCT with fields named "key" and "value"
    struct_schema = TableSchema(
        catalog="test",
        schema_name="test",
        table_name="test",
        columns=[
            ColumnInfo(
                name="pair",
                data_type="STRUCT<key: STRING, value: INT>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(name="key", data_type="STRING", is_complex=False, nullable=True),
                    ColumnInfo(name="value", data_type="INT", is_complex=False, nullable=True),
                ],
            )
        ],
    )

    ast = SchemaTreeBuilder.build_from_table_schema(struct_schema)
    assert isinstance(ast.columns[0], StructNode)

    # MAP type
    map_schema = TableSchema(
        catalog="test",
        schema_name="test",
        table_name="test",
        columns=[
            ColumnInfo(
                name="tags",
                data_type="MAP<STRING, INT>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(name="key", data_type="STRING", is_complex=False, nullable=False),
                    ColumnInfo(name="value", data_type="INT", is_complex=False, nullable=True),
                ],
            )
        ],
    )

    ast = SchemaTreeBuilder.build_from_table_schema(map_schema)
    assert isinstance(ast.columns[0], MapNode)
