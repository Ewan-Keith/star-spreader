"""Builder for converting ColumnInfo structures to schema tree nodes.

This module provides utilities to convert the legacy ColumnInfo/TableSchema
structure into the new schema tree representation.
"""

from typing import List

from star_spreader.schema_tree.nodes import (
    SchemaTreeNode,
    ArrayNode,
    MapNode,
    SimpleColumnNode,
    StructNode,
    TableSchemaNode,
)
from star_spreader.schema.base import ColumnInfo, TableSchema


class SchemaTreeBuilder:
    """Builds schema tree nodes from ColumnInfo structures.

    This class handles the conversion from the raw ColumnInfo tree structure
    (which is returned by schema fetchers) into a proper schema tree representation.
    """

    @staticmethod
    def build_from_table_schema(table_schema: TableSchema) -> TableSchemaNode:
        """Convert a TableSchema to a TableSchemaNode schema tree.

        Args:
            table_schema: The legacy TableSchema object

        Returns:
            A TableSchemaNode with all columns converted to schema tree nodes
        """
        column_nodes = [SchemaTreeBuilder._build_column_node(col) for col in table_schema.columns]

        return TableSchemaNode(
            catalog=table_schema.catalog,
            schema_name=table_schema.schema_name,
            table_name=table_schema.table_name,
            columns=column_nodes,
        )

    @staticmethod
    def _build_column_node(column: ColumnInfo) -> SchemaTreeNode:
        """Convert a ColumnInfo to an appropriate schema tree node.

        Args:
            column: The ColumnInfo to convert

        Returns:
            A schema tree node (SimpleColumnNode, StructNode, ArrayNode, or MapNode)
        """
        if not column.is_complex or not column.children:
            # Simple column
            return SimpleColumnNode(
                name=column.name,
                data_type=column.data_type,
                nullable=column.nullable,
            )

        # Check what type of complex column this is
        # First check the data_type to disambiguate between MAP and STRUCT with key/value fields
        data_type_upper = column.data_type.upper().strip()

        if data_type_upper.startswith("ARRAY<"):
            # ARRAY type
            element_child = column.children[0]
            element_node = SchemaTreeBuilder._build_column_node(element_child)

            return ArrayNode(
                name=column.name,
                data_type=column.data_type,
                nullable=column.nullable,
                element_type=element_node,
            )

        elif data_type_upper.startswith("MAP<"):
            # MAP type - use data_type to confirm
            key_child = next(c for c in column.children if c.name == "key")
            value_child = next(c for c in column.children if c.name == "value")

            key_node = SchemaTreeBuilder._build_column_node(key_child)
            value_node = SchemaTreeBuilder._build_column_node(value_child)

            return MapNode(
                name=column.name,
                data_type=column.data_type,
                nullable=column.nullable,
                key_type=key_node,
                value_type=value_node,
            )

        else:
            # STRUCT type (including STRUCTs with fields named "key" and "value")
            field_nodes = [SchemaTreeBuilder._build_column_node(child) for child in column.children]

            return StructNode(
                name=column.name,
                data_type=column.data_type,
                nullable=column.nullable,
                fields=field_nodes,
            )
