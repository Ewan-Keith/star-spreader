"""Unit tests for the schema tree module.

These tests demonstrate the schema tree-based architecture and ensure
the schema tree nodes and SQL generation work correctly.
"""

import pytest
from star_spreader.schema_tree.nodes import (
    ArrayNode,
    MapNode,
    SimpleColumnNode,
    StructNode,
    TableSchemaNode,
)
from star_spreader.generator.sql_schema_tree import SchemaTreeSQLGenerator


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
