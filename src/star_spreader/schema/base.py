"""Base schema classes for star-spreader.

This module defines the core data models and abstract interfaces for schema fetching.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from star_spreader.schema_tree.nodes import TableSchemaNode


class ColumnInfo(BaseModel):
    """Represents a single column in a database table.

    This model can represent both simple columns (like string, int) and complex
    nested types (like struct, array, map) through the children attribute.

    Attributes:
        name: The column name.
        data_type: The raw data type string from the database (e.g., 'string', 'struct<...>').
        is_complex: Whether this column has a complex type (struct/array/map).
        children: For complex types, the nested column structure. None for simple types.
        nullable: Whether this column accepts NULL values.
    """

    model_config = ConfigDict(frozen=False)

    name: str = Field(..., description="The column name")
    data_type: str = Field(..., description="The raw type string from the database")
    is_complex: bool = Field(..., description="Whether it's a struct/array/map")
    children: Optional[List["ColumnInfo"]] = Field(
        default=None, description="For nested fields in structs"
    )
    nullable: bool = Field(..., description="Whether the column accepts NULL values")


class TableSchema(BaseModel):
    """Represents the complete schema of a database table.

    Attributes:
        catalog: The catalog name (e.g., 'hive_metastore' in Databricks).
        schema_name: The schema/database name.
        table_name: The table name.
        columns: List of all columns in the table, including nested structures.
    """

    model_config = ConfigDict(frozen=False)

    catalog: str = Field(..., description="The catalog name")
    schema_name: str = Field(..., description="The schema/database name")
    table_name: str = Field(..., description="The table name")
    columns: List[ColumnInfo] = Field(..., description="List of all columns in the table")


class SchemaFetcher(ABC):
    """Abstract base class for fetching table schemas from different data sources.

    Implementations of this class should handle the specifics of connecting to
    and retrieving schema information from different database systems (e.g.,
    Databricks, PostgreSQL, etc.).
    """

    @abstractmethod
    def fetch_schema(self, catalog: str, schema: str, table: str) -> TableSchema:
        """Fetch the schema for a specific table.

        Args:
            catalog: The catalog name containing the table.
            schema: The schema/database name containing the table.
            table: The table name to fetch schema for.

        Returns:
            A TableSchema object containing the complete table schema information.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
            Any implementation-specific exceptions for connection or query errors.
        """
        raise NotImplementedError("Subclasses must implement fetch_schema")

    def fetch_schema_ast(self, catalog: str, schema: str, table: str):
        """Fetch the schema for a specific table and return it as a schema tree.

        This is the primary method that should be used by application logic.
        It fetches the schema and converts it to a schema tree representation for
        maximum modularity.

        Args:
            catalog: The catalog name containing the table.
            schema: The schema/database name containing the table.
            table: The table name to fetch schema for.

        Returns:
            A TableSchemaNode AST representing the complete table schema.

        Raises:
            Any implementation-specific exceptions for connection or query errors.
        """
        from star_spreader.schema_tree.builder import SchemaTreeBuilder

        table_schema = self.fetch_schema(catalog, schema, table)
        return SchemaTreeBuilder.build_from_table_schema(table_schema)
