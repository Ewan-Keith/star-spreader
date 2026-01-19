"""Schema tree node definitions for representing database schema structure.

This module defines the schema tree nodes that represent different
column types and table structures in a modular, type-safe way.
"""

from abc import ABC, abstractmethod
from typing import List

from pydantic import BaseModel, ConfigDict, Field


class SchemaTreeNode(ABC, BaseModel):
    """Base class for all schema tree nodes.

    Schema tree nodes represent the structure of database schemas in a way that's
    decoupled from the specific source (Databricks, PostgreSQL, etc.) and
    from the SQL generation logic.
    """

    model_config = ConfigDict(frozen=False)

    name: str = Field(..., description="The name of this node (column/field name)")
    data_type: str = Field(..., description="The raw data type string")
    nullable: bool = Field(default=True, description="Whether this column accepts NULL values")

    @abstractmethod
    def accept(self, visitor: "SchemaTreeVisitor") -> str:
        """Accept a visitor for the visitor pattern.

        Args:
            visitor: The visitor to accept

        Returns:
            Result of the visitor's visit operation
        """
        pass


class SimpleColumnNode(SchemaTreeNode):
    """Represents a simple (non-complex) column type.

    Examples: INT, STRING, BIGINT, TIMESTAMP, BOOLEAN, etc.
    """

    def accept(self, visitor: "SchemaTreeVisitor") -> str:
        """Accept a visitor for simple column nodes."""
        return visitor.visit_simple_column(self)


class StructNode(SchemaTreeNode):
    """Represents a STRUCT type with named fields.

    Example: STRUCT<name: STRING, age: INT, contact: STRUCT<email: STRING>>

    Attributes:
        fields: List of child schema tree nodes representing struct fields
    """

    fields: List[SchemaTreeNode] = Field(..., description="List of struct field nodes")

    def accept(self, visitor: "SchemaTreeVisitor") -> str:
        """Accept a visitor for struct nodes."""
        return visitor.visit_struct(self)


class ArrayNode(SchemaTreeNode):
    """Represents an ARRAY type.

    Examples:
        - ARRAY<INT>
        - ARRAY<STRUCT<product_id: INT, quantity: INT>>

    Attributes:
        element_type: The schema tree node representing the array element type
    """

    element_type: SchemaTreeNode = Field(..., description="The element type of this array")

    def accept(self, visitor: "SchemaTreeVisitor") -> str:
        """Accept a visitor for array nodes."""
        return visitor.visit_array(self)


class MapNode(SchemaTreeNode):
    """Represents a MAP type with key and value types.

    Example: MAP<STRING, INT>

    Attributes:
        key_type: The schema tree node representing the map key type
        value_type: The schema tree node representing the map value type
    """

    key_type: SchemaTreeNode = Field(..., description="The key type of this map")
    value_type: SchemaTreeNode = Field(..., description="The value type of this map")

    def accept(self, visitor: "SchemaTreeVisitor") -> str:
        """Accept a visitor for map nodes."""
        return visitor.visit_map(self)


class TableSchemaNode(BaseModel):
    """Represents the complete schema of a table as a schema tree.

    This is the root node of the schema tree that contains all column definitions.

    Attributes:
        catalog: The catalog name
        schema_name: The schema/database name
        table_name: The table name
        columns: List of top-level column schema tree nodes
    """

    model_config = ConfigDict(frozen=False)

    catalog: str = Field(..., description="The catalog name")
    schema_name: str = Field(..., description="The schema/database name")
    table_name: str = Field(..., description="The table name")
    columns: List[SchemaTreeNode] = Field(..., description="List of top-level column nodes")

    def get_full_table_name(self) -> str:
        """Get the fully qualified table name.

        Returns:
            Table name in format: catalog.schema.table
        """
        return f"{self.catalog}.{self.schema_name}.{self.table_name}"
