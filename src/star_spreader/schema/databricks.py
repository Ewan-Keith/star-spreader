"""Databricks schema fetcher implementation.

This module provides schema fetching capabilities for Databricks tables using the
Databricks SDK. It handles complex types like STRUCT, ARRAY, and MAP by recursively
parsing type strings and directly building schema tree nodes.
"""

import re
from typing import List, Optional, Tuple

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ColumnInfo as DatabricksColumnInfo

from star_spreader.schema.base import SchemaFetcher
from star_spreader.schema_tree.nodes import (
    ArrayNode,
    MapNode,
    SchemaTreeNode,
    SimpleColumnNode,
    StructNode,
    TableSchemaNode,
)


class DatabricksSchemaFetcher(SchemaFetcher):
    """Fetches table schemas from Databricks using the Databricks SDK.

    This fetcher connects to a Databricks workspace and retrieves detailed schema
    information including complex nested types (STRUCT, ARRAY, MAP).

    Attributes:
        workspace: The Databricks WorkspaceClient instance for API calls.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        token: Optional[str] = None,
        workspace_client: Optional[WorkspaceClient] = None,
    ) -> None:
        """Initialize the Databricks schema fetcher.

        Args:
            host: Databricks workspace host URL (e.g., 'https://company.cloud.databricks.com').
                  Required if workspace_client is not provided.
            token: Databricks personal access token. Required if workspace_client is not provided.
            workspace_client: Pre-configured WorkspaceClient instance. If provided, host and
                            token are ignored.

        Raises:
            ValueError: If neither workspace_client nor (host and token) are provided.
        """
        if workspace_client is not None:
            self.workspace = workspace_client
        elif host is not None and token is not None:
            self.workspace = WorkspaceClient(host=host, token=token)
        else:
            raise ValueError("Either workspace_client or both host and token must be provided")

    def get_schema_tree(self, catalog: str, schema: str, table: str) -> TableSchemaNode:
        """Fetch schema information for a Databricks table and return as schema tree.

        Uses the Databricks Unity Catalog API to retrieve full table metadata
        and directly converts it to a schema tree representation.

        Args:
            catalog: Catalog name (e.g., 'main', 'hive_metastore').
            schema: Schema/database name.
            table: Table name.

        Returns:
            TableSchemaNode representing the complete table schema.

        Raises:
            Exception: If the table is not found or the API call fails.
        """
        # Construct the full table name for the API call
        full_table_name = f"{catalog}.{schema}.{table}"

        # Fetch table information from Databricks
        table_info = self.workspace.tables.get(full_name=full_table_name)

        # Parse columns from the table info directly to schema tree nodes
        columns = []
        if table_info.columns:
            for db_column in table_info.columns:
                column_node = self._parse_column(db_column)
                columns.append(column_node)

        return TableSchemaNode(
            catalog=catalog,
            schema_name=schema,
            table_name=table,
            columns=columns,
        )

    def _parse_column(self, db_column: DatabricksColumnInfo) -> SchemaTreeNode:
        """Parse a Databricks ColumnInfo into a schema tree node.

        Args:
            db_column: Column information from Databricks API.

        Returns:
            SchemaTreeNode representing the column (SimpleColumnNode, StructNode, ArrayNode, or MapNode).
        """
        type_text = db_column.type_text or ""
        # Convert type_name to string - handle both enum and string types
        if db_column.type_name:
            # Handle ColumnTypeName enum by accessing .value, or convert to string
            type_name = getattr(db_column.type_name, "value", None) or str(db_column.type_name)
        else:
            type_name = type_text

        # Use type_text for complex type detection if available, as it has full definition
        type_for_check = type_text if type_text else type_name
        data_type = type_text or type_name
        nullable = db_column.nullable or False
        column_name = db_column.name or ""

        # Determine if this is a complex type and return appropriate node
        if self._is_complex_type(type_for_check):
            return self._parse_complex_type(column_name, data_type, nullable)
        else:
            # Simple column
            return SimpleColumnNode(
                name=column_name,
                data_type=data_type,
                nullable=nullable,
            )

    def _is_complex_type(self, type_name: str) -> bool:
        """Check if a type is complex (STRUCT, ARRAY, or MAP).

        Args:
            type_name: The type name to check.

        Returns:
            True if the type is complex, False otherwise.
        """
        if not type_name:
            return False
        type_upper = str(type_name).upper().strip()
        return any(type_upper.startswith(prefix) for prefix in ["STRUCT<", "ARRAY<", "MAP<"])

    def _parse_complex_type(self, name: str, type_text: str, nullable: bool) -> SchemaTreeNode:
        """Parse a complex type string into a schema tree node.

        Handles STRUCT, ARRAY, and MAP types by recursively parsing their
        type definitions.

        Args:
            name: The name of this column/field.
            type_text: The full type string (e.g., "STRUCT<field1: INT, field2: STRING>").
            nullable: Whether this column accepts NULL values.

        Returns:
            SchemaTreeNode (StructNode, ArrayNode, or MapNode) representing the complex type.

        Raises:
            ValueError: If the type_text doesn't match a known complex type pattern.
        """
        type_upper = type_text.upper().strip()

        if type_upper.startswith("STRUCT<"):
            return self._parse_struct_type(name, type_text, nullable)
        elif type_upper.startswith("ARRAY<"):
            return self._parse_array_type(name, type_text, nullable)
        elif type_upper.startswith("MAP<"):
            return self._parse_map_type(name, type_text, nullable)

        raise ValueError(f"Unknown complex type: {type_text}")

    def _parse_struct_type(self, name: str, type_text: str, nullable: bool) -> StructNode:
        """Parse a STRUCT type into a StructNode.

        Example: "STRUCT<field1: INT, field2: STRUCT<nested: STRING>>"

        Args:
            name: The name of this struct column.
            type_text: The STRUCT type string.
            nullable: Whether this struct accepts NULL values.

        Returns:
            StructNode with all fields as child nodes.
        """
        # Extract content between STRUCT< and >
        match = re.match(r"STRUCT<(.+)>", type_text, re.IGNORECASE | re.DOTALL)
        if not match:
            return StructNode(name=name, data_type=type_text, nullable=nullable, fields=[])

        fields_text = match.group(1)
        field_defs = self._split_fields(fields_text)

        field_nodes = []
        for field_def in field_defs:
            # Parse field as "name: type"
            field_parts = self._split_field_definition(field_def)
            if field_parts:
                field_name, field_type = field_parts

                # Check if field is complex and create appropriate node
                if self._is_complex_type(field_type):
                    field_node = self._parse_complex_type(field_name, field_type, nullable=True)
                else:
                    field_node = SimpleColumnNode(
                        name=field_name,
                        data_type=field_type,
                        nullable=True,
                    )
                field_nodes.append(field_node)

        return StructNode(
            name=name,
            data_type=type_text,
            nullable=nullable,
            fields=field_nodes,
        )

    def _parse_array_type(self, name: str, type_text: str, nullable: bool) -> ArrayNode:
        """Parse an ARRAY type into an ArrayNode.

        Example: "ARRAY<INT>" -> ArrayNode with SimpleColumnNode element
        Example: "ARRAY<STRUCT<...>>" -> ArrayNode with StructNode element

        Args:
            name: The name of this array column.
            type_text: The ARRAY type string.
            nullable: Whether this array accepts NULL values.

        Returns:
            ArrayNode with the appropriate element type node.
        """
        # Extract content between ARRAY< and >
        match = re.match(r"ARRAY<(.+)>", type_text, re.IGNORECASE | re.DOTALL)
        if not match:
            # Fallback for invalid array definition
            element_node = SimpleColumnNode(name="element", data_type="UNKNOWN", nullable=True)
            return ArrayNode(
                name=name,
                data_type=type_text,
                nullable=nullable,
                element_type=element_node,
            )

        element_type = match.group(1).strip()

        # Create the element node
        if self._is_complex_type(element_type):
            element_node = self._parse_complex_type("element", element_type, nullable=True)
        else:
            element_node = SimpleColumnNode(
                name="element",
                data_type=element_type,
                nullable=True,
            )

        return ArrayNode(
            name=name,
            data_type=type_text,
            nullable=nullable,
            element_type=element_node,
        )

    def _parse_map_type(self, name: str, type_text: str, nullable: bool) -> MapNode:
        """Parse a MAP type into a MapNode.

        Example: "MAP<STRING, INT>" -> MapNode with key and value nodes

        Args:
            name: The name of this map column.
            type_text: The MAP type string.
            nullable: Whether this map accepts NULL values.

        Returns:
            MapNode with key and value type nodes.
        """
        # Extract content between MAP< and >
        match = re.match(r"MAP<(.+)>", type_text, re.IGNORECASE | re.DOTALL)
        if not match:
            # Fallback for invalid map definition
            key_node = SimpleColumnNode(name="key", data_type="UNKNOWN", nullable=False)
            value_node = SimpleColumnNode(name="value", data_type="UNKNOWN", nullable=True)
            return MapNode(
                name=name,
                data_type=type_text,
                nullable=nullable,
                key_type=key_node,
                value_type=value_node,
            )

        content = match.group(1).strip()

        # Split into key and value types
        parts = self._split_map_key_value(content)
        if len(parts) != 2:
            # Fallback
            key_node = SimpleColumnNode(name="key", data_type="UNKNOWN", nullable=False)
            value_node = SimpleColumnNode(name="value", data_type="UNKNOWN", nullable=True)
            return MapNode(
                name=name,
                data_type=type_text,
                nullable=nullable,
                key_type=key_node,
                value_type=value_node,
            )

        key_type, value_type = parts

        # Create key node
        if self._is_complex_type(key_type):
            key_node = self._parse_complex_type("key", key_type, nullable=False)
        else:
            key_node = SimpleColumnNode(name="key", data_type=key_type, nullable=False)

        # Create value node
        if self._is_complex_type(value_type):
            value_node = self._parse_complex_type("value", value_type, nullable=True)
        else:
            value_node = SimpleColumnNode(name="value", data_type=value_type, nullable=True)

        return MapNode(
            name=name,
            data_type=type_text,
            nullable=nullable,
            key_type=key_node,
            value_type=value_node,
        )

    def _split_fields(self, fields_text: str) -> List[str]:
        """Split a struct's field definitions, respecting nested brackets.

        Example: "field1: INT, field2: STRUCT<nested: STRING>, field3: ARRAY<INT>"
        Returns: ["field1: INT", "field2: STRUCT<nested: STRING>", "field3: ARRAY<INT>"]

        Args:
            fields_text: The fields text from inside a STRUCT<...>.

        Returns:
            List of individual field definition strings.
        """
        fields = []
        current_field = []
        bracket_depth = 0

        for char in fields_text:
            if char == "<":
                bracket_depth += 1
                current_field.append(char)
            elif char == ">":
                bracket_depth -= 1
                current_field.append(char)
            elif char == "," and bracket_depth == 0:
                # This comma separates fields
                field_str = "".join(current_field).strip()
                if field_str:
                    fields.append(field_str)
                current_field = []
            else:
                current_field.append(char)

        # Add the last field
        field_str = "".join(current_field).strip()
        if field_str:
            fields.append(field_str)

        return fields

    def _split_field_definition(self, field_text: str) -> Optional[Tuple[str, str]]:
        """Split a field definition into name and type.

        Example: "field1: INT" -> ("field1", "INT")
        Example: "my_struct: STRUCT<x: INT>" -> ("my_struct", "STRUCT<x: INT>")

        Args:
            field_text: The field definition string.

        Returns:
            Tuple of (field_name, field_type) or None if invalid.
        """
        # Find the first colon that's not inside brackets
        bracket_depth = 0
        colon_pos = -1

        for i, char in enumerate(field_text):
            if char == "<":
                bracket_depth += 1
            elif char == ">":
                bracket_depth -= 1
            elif char == ":" and bracket_depth == 0:
                colon_pos = i
                break

        if colon_pos == -1:
            return None

        name = field_text[:colon_pos].strip()
        field_type = field_text[colon_pos + 1 :].strip()

        return (name, field_type)

    def _split_map_key_value(self, content: str) -> List[str]:
        """Split MAP content into key and value types.

        Example: "STRING, INT" -> ["STRING", "INT"]
        Example: "STRING, STRUCT<x: INT>" -> ["STRING", "STRUCT<x: INT>"]

        Args:
            content: The content between MAP< and >.

        Returns:
            List with two elements: [key_type, value_type].
        """
        # Find the comma that separates key and value (not inside brackets)
        bracket_depth = 0
        comma_pos = -1

        for i, char in enumerate(content):
            if char == "<":
                bracket_depth += 1
            elif char == ">":
                bracket_depth -= 1
            elif char == "," and bracket_depth == 0:
                comma_pos = i
                break

        if comma_pos == -1:
            return []

        key_type = content[:comma_pos].strip()
        value_type = content[comma_pos + 1 :].strip()

        return [key_type, value_type]
