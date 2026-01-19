"""Base schema classes for star-spreader.

This module defines the abstract interface for schema fetching from different data sources.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from star_spreader.schema_tree.nodes import TableSchemaNode


class SchemaFetcher(ABC):
    """Abstract base class for fetching table schemas from different data sources.

    Implementations of this class should handle the specifics of connecting to
    and retrieving schema information from different database systems (e.g.,
    Databricks, PostgreSQL, etc.).

    Each implementation must directly convert the database schema to a schema tree
    representation without intermediate models, ensuring maximum flexibility for
    different database types.
    """

    @abstractmethod
    def get_schema_tree(self, catalog: str, schema: str, table: str) -> "TableSchemaNode":
        """Fetch the schema for a specific table and return it as a schema tree.

        This method should directly query the database and convert the result
        to our schema tree representation without intermediate conversions.

        Args:
            catalog: The catalog name containing the table.
            schema: The schema/database name containing the table.
            table: The table name to fetch schema for.

        Returns:
            A TableSchemaNode representing the complete table schema.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
            Any implementation-specific exceptions for connection or query errors.
        """
        raise NotImplementedError("Subclasses must implement get_schema_tree")
