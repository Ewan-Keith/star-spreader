"""Star Spreader - Convert SELECT * to explicit column lists."""

from star_spreader.config import get_workspace_client
from star_spreader.generator.sql_schema_tree import (
    SchemaTreeSQLGenerator,
    generate_select_from_schema_tree,
)
from star_spreader.schema.databricks import DatabricksSchemaFetcher
from star_spreader.schema_tree.nodes import (
    ArrayNode,
    MapNode,
    SchemaTreeNode,
    SimpleColumnNode,
    StructNode,
    TableSchemaNode,
)

__version__ = "0.1.0"

__all__ = [
    "get_workspace_client",
    "SchemaTreeSQLGenerator",
    "generate_select_from_schema_tree",
    "DatabricksSchemaFetcher",
    "SchemaTreeNode",
    "SimpleColumnNode",
    "StructNode",
    "ArrayNode",
    "MapNode",
    "TableSchemaNode",
]
