"""SQL generation module using schema tree visitor pattern.

This module provides a clean, modular SQL generator that works with the schema tree
representation of database schemas.
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
from star_spreader.schema_tree.visitor import SchemaTreeVisitor


class SQLGeneratorVisitor(SchemaTreeVisitor):
    """Schema tree visitor that generates SQL expressions for each node type.

    This visitor traverses the schema tree and generates appropriate SQL expressions
    for each column type, handling complex nested structures with proper
    STRUCT reconstruction and TRANSFORM for arrays.
    """

    def __init__(self, parent_path: str = "", lambda_var: str = "", depth: int = 0):
        """Initialize the SQL generator visitor.

        Args:
            parent_path: The parent column path for nested traversal
            lambda_var: The lambda variable name for array contexts
            depth: Current nesting depth for lambda variable generation
        """
        self.parent_path = parent_path
        self.lambda_var = lambda_var
        self.depth = depth

    def visit_simple_column(self, node: SimpleColumnNode) -> str:
        """Visit a simple column node and generate SQL reference.

        Args:
            node: The simple column node

        Returns:
            Backtick-quoted column reference
        """
        if self.lambda_var:
            # We're inside an array context - use lambda variable with proper path quoting
            return self._build_lambda_field_reference(node.name)
        else:
            # Top-level or struct field - build full path
            if self.parent_path:
                path = f"{self.parent_path}.{node.name}"
            else:
                path = node.name
            return self._quote_column_path(path)

    def visit_struct(self, node: StructNode) -> str:
        """Visit a struct node and generate STRUCT() expression.

        Args:
            node: The struct node

        Returns:
            STRUCT() SQL expression with all fields
        """
        if self.lambda_var:
            # Inside array context
            # Special case: if this struct's name is "element" and we have no parent_path,
            # it means this is the array element struct itself, not a nested struct
            # In this case, don't use the name "element" in the path
            if node.name == "element" and not self.parent_path:
                struct_path = ""
            elif self.parent_path:
                struct_path = f"{self.parent_path}.{node.name}"
            else:
                struct_path = node.name
        else:
            # Top-level or nested struct
            if self.parent_path:
                struct_path = f"{self.parent_path}.{node.name}"
            else:
                struct_path = node.name

        # Build STRUCT() with all fields
        field_expressions = []
        for field in node.fields:
            # Create visitor for field with updated parent path
            field_visitor = SQLGeneratorVisitor(
                parent_path=struct_path, lambda_var=self.lambda_var, depth=self.depth
            )
            field_expr = field.accept(field_visitor)
            field_expressions.append(f"{field_expr} AS `{field.name}`")

        return f"STRUCT({', '.join(field_expressions)})"

    def visit_array(self, node: ArrayNode) -> str:
        """Visit an array node and generate appropriate SQL expression.

        For ARRAY<primitive>, returns simple reference.
        For ARRAY<STRUCT>, generates TRANSFORM expression.

        Args:
            node: The array node

        Returns:
            SQL expression for the array
        """
        # Build the array path
        if self.lambda_var:
            # Inside another array context - use proper path quoting
            array_path = self._build_lambda_field_reference(node.name)
        else:
            # Top-level array
            if self.parent_path:
                array_path = self._quote_column_path(f"{self.parent_path}.{node.name}")
            else:
                array_path = self._quote_column_path(node.name)

        # Check if element is complex (STRUCT, nested ARRAY, or MAP)
        element = node.element_type
        if isinstance(element, (StructNode, ArrayNode, MapNode)):
            # Need TRANSFORM for complex element types (mainly STRUCT)
            # Generate unique lambda variable - start at depth 0 for first level
            if self.lambda_var:
                # We're already in a lambda context, increment depth
                new_depth = self.depth + 1
            else:
                # First lambda context
                new_depth = 0
            new_lambda_var = self._generate_lambda_var(new_depth)

            # For struct elements, we don't need parent_path since the lambda variable
            # directly references the struct
            element_visitor = SQLGeneratorVisitor(
                parent_path="", lambda_var=new_lambda_var, depth=new_depth
            )
            element_expr = element.accept(element_visitor)

            return f"TRANSFORM({array_path}, {new_lambda_var} -> {element_expr})"
        else:
            # Simple array - just reference it
            return array_path

    def visit_map(self, node: MapNode) -> str:
        """Visit a map node and generate SQL reference.

        Maps are referenced as-is without reconstruction.

        Args:
            node: The map node

        Returns:
            Backtick-quoted map reference
        """
        if self.lambda_var:
            # Inside array context - use proper path quoting
            return self._build_lambda_field_reference(node.name)
        else:
            # Top-level map
            if self.parent_path:
                path = f"{self.parent_path}.{node.name}"
            else:
                path = node.name
            return self._quote_column_path(path)

    def _build_lambda_field_reference(self, field_name: str) -> str:
        """Build a field reference within a lambda/array context.

        This method properly handles nested paths by quoting each component
        separately to avoid treating dotted paths as single field names.

        Args:
            field_name: The field name to reference (may be a simple name or dotted path)

        Returns:
            Properly quoted field reference (e.g., 'item.`parent`.`child`.`field`')
        """
        if self.parent_path:
            # Quote parent path components separately
            parent_parts = self.parent_path.split(".")
            quoted_parent = ".".join([f"`{part}`" for part in parent_parts])
            return f"{self.lambda_var}.{quoted_parent}.`{field_name}`"
        else:
            return f"{self.lambda_var}.`{field_name}`"

    def _quote_column_path(self, path: str) -> str:
        """Quote a column path with backticks for Databricks compatibility.

        Args:
            path: Column path with dots (e.g., 'parent.child.field')

        Returns:
            Backtick-quoted path (e.g., '`parent`.`child`.`field`')
        """
        parts = path.split(".")
        quoted_parts = [f"`{part}`" for part in parts]
        return ".".join(quoted_parts)

    def _generate_lambda_var(self, depth: int) -> str:
        """Generate a unique lambda variable name based on nesting depth.

        Args:
            depth: The nesting depth (0 for 'item', 1 for 'item2', etc.)

        Returns:
            Lambda variable name (e.g., 'item', 'item2', 'item3')
        """
        if depth == 0:
            return "item"
        else:
            return f"item{depth + 1}"


class SchemaTreeSQLGenerator:
    """Generates explicit SELECT statements from schema tree representation.

    This is the main interface for SQL generation using the schema tree approach.
    """

    def __init__(self, table_schema_node: TableSchemaNode):
        """Initialize the schema tree SQL generator.

        Args:
            table_schema_node: The schema tree representation of the table schema
        """
        self.schema_node = table_schema_node

    def generate_select(self) -> str:
        """Generate a complete SELECT statement with all fields explicitly listed.

        Returns:
            A complete SELECT statement string
        """
        column_expressions = self._expand_all_columns()

        select_clause = "SELECT " + ",\n       ".join(column_expressions)
        from_clause = f"FROM {self._get_full_table_name()}"

        return f"{select_clause}\n{from_clause}"

    def _get_full_table_name(self) -> str:
        """Get the fully qualified table name with backtick quoting.

        Returns:
            Backtick-quoted table name in format: `catalog`.`schema`.`table`
        """
        return f"`{self.schema_node.catalog}`.`{self.schema_node.schema_name}`.`{self.schema_node.table_name}`"

    def _expand_all_columns(self) -> List[str]:
        """Generate column expressions for all top-level columns.

        Returns:
            List of column expression strings
        """
        expanded_columns = []

        for column in self.schema_node.columns:
            expanded_columns.append(self._expand_column(column))

        return expanded_columns

    def _expand_column(self, column: SchemaTreeNode) -> str:
        """Generate SQL expression for a single top-level column.

        Args:
            column: The schema tree node representing the column

        Returns:
            SQL expression for the column
        """
        visitor = SQLGeneratorVisitor(parent_path="", lambda_var="", depth=0)
        expr = column.accept(visitor)

        # For complex types, add alias
        if isinstance(column, (StructNode, ArrayNode)):
            # Check if the element type requires reconstruction
            if isinstance(column, ArrayNode):
                element = column.element_type
                if isinstance(element, (StructNode, ArrayNode, MapNode)):
                    # TRANSFORM was used, add alias
                    return f"{expr} AS `{column.name}`"
                else:
                    # Simple array, just return reference
                    return expr
            else:
                # STRUCT reconstruction, add alias
                return f"{expr} AS `{column.name}`"
        else:
            # Simple column or map, return as-is
            return expr


def generate_select_from_schema_tree(table_schema_node: TableSchemaNode) -> str:
    """Convenience function to generate a SELECT statement from a schema tree.

    Args:
        table_schema_node: The schema tree representation of the table schema

    Returns:
        A complete SELECT statement string
    """
    generator = SchemaTreeSQLGenerator(table_schema_node)
    return generator.generate_select()
