"""Visitor pattern for traversing and processing schema tree nodes.

This module provides the abstract visitor interface that can be implemented
to perform different operations on the schema tree (SQL generation, validation, etc.).
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from star_spreader.schema_tree.nodes import (
        ArrayNode,
        MapNode,
        SimpleColumnNode,
        StructNode,
    )


class SchemaTreeVisitor(ABC):
    """Abstract base class for schema tree visitors.

    Implementations of this class can traverse the schema tree and perform operations
    like SQL generation, validation, schema analysis, etc.
    """

    @abstractmethod
    def visit_simple_column(self, node: "SimpleColumnNode") -> str:
        """Visit a simple column node.

        Args:
            node: The simple column node to visit

        Returns:
            String representation or processed result
        """
        pass

    @abstractmethod
    def visit_struct(self, node: "StructNode") -> str:
        """Visit a struct node.

        Args:
            node: The struct node to visit

        Returns:
            String representation or processed result
        """
        pass

    @abstractmethod
    def visit_array(self, node: "ArrayNode") -> str:
        """Visit an array node.

        Args:
            node: The array node to visit

        Returns:
            String representation or processed result
        """
        pass

    @abstractmethod
    def visit_map(self, node: "MapNode") -> str:
        """Visit a map node.

        Args:
            node: The map node to visit

        Returns:
            String representation or processed result
        """
        pass
