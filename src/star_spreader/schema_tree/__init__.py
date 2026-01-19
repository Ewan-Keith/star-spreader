"""Schema tree module for representing database schema as a tree structure.

This module provides schema tree nodes to represent database schema in a modular,
extensible way that decouples schema representation from SQL generation.
"""

from star_spreader.schema_tree.nodes import (
    SchemaTreeNode,
    ArrayNode,
    MapNode,
    SimpleColumnNode,
    StructNode,
    TableSchemaNode,
)
from star_spreader.schema_tree.visitor import SchemaTreeVisitor

__all__ = [
    "SchemaTreeNode",
    "SimpleColumnNode",
    "StructNode",
    "ArrayNode",
    "MapNode",
    "TableSchemaNode",
    "SchemaTreeVisitor",
]
