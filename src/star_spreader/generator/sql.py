"""SQL generation module for expanding SELECT * into explicit column lists."""

from typing import List

from star_spreader.schema.base import ColumnInfo, TableSchema


class SQLGenerator:
    """Generates explicit SELECT statements from table schema information.

    This class takes a TableSchema and generates a complete SELECT statement
    that explicitly lists all fields (including nested struct members) and
    reconstructs complex types using STRUCT(), ARRAY(), etc. to produce output
    identical to SELECT *.
    """

    def __init__(self, table_schema: TableSchema):
        """Initialize the SQL generator with a table schema.

        Args:
            table_schema: The TableSchema object containing column information
        """
        self.table_schema = table_schema

    def generate_select(self) -> str:
        """Generate a complete SELECT statement with all fields explicitly listed.

        This method explicitly selects all fields (including nested struct members)
        and reconstructs complex types to produce output identical to SELECT *.

        Returns:
            A complete SELECT statement string with format:
            SELECT col1, col2, STRUCT(field1, field2) AS struct_col, ...
            FROM catalog.schema.table
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
        return f"`{self.table_schema.catalog}`.`{self.table_schema.schema_name}`.`{self.table_schema.table_name}`"

    def _expand_all_columns(self) -> List[str]:
        """Generate column expressions for all top-level columns.

        Returns:
            List of column expression strings for all top-level columns
        """
        expanded_columns = []

        for column in self.table_schema.columns:
            expanded_columns.extend(self._expand_column(column))

        return expanded_columns

    def _expand_column(self, column: ColumnInfo, parent_path: str = "") -> List[str]:
        """Generate column expression, reconstructing structs with STRUCT().

        Args:
            column: The ColumnInfo object to process
            parent_path: The parent column path (for nested structs)

        Returns:
            List containing a single column expression. For STRUCT columns, this will
            be a STRUCT() constructor with all fields explicitly listed. For simple
            columns, just the column reference.
        """
        current_path = f"{parent_path}.{column.name}" if parent_path else column.name

        # Only process top-level columns (no parent path)
        if parent_path:
            # Shouldn't reach here - this method is only called for top-level columns
            return []

        # Check if this is a complex type
        if column.is_complex and column.children:
            child_names = {child.name for child in column.children}

            # ARRAY - check if it contains structs
            if child_names == {"element"}:
                element_child = column.children[0]
                if element_child.is_complex and element_child.children:
                    # ARRAY<STRUCT<...>> - use TRANSFORM to reconstruct each element
                    array_expr = self._build_array_of_struct_expression(column, column.name)
                    return [f"{array_expr} AS {self._quote_column_path(column.name)}"]
                else:
                    # ARRAY<primitive> - reference as-is
                    return [self._quote_column_path(column.name)]

            # MAP - reference as-is (no reconstruction needed)
            elif child_names == {"key", "value"}:
                return [self._quote_column_path(column.name)]

            # STRUCT - reconstruct with STRUCT()
            else:
                struct_expr = self._build_struct_expression(column, column.name)
                return [f"{struct_expr} AS {self._quote_column_path(column.name)}"]
        else:
            # Simple column - just reference it
            return [self._quote_column_path(column.name)]

    def _build_array_of_struct_expression(self, column: ColumnInfo, base_path: str) -> str:
        """Build a TRANSFORM expression for ARRAY<STRUCT<...>> columns.

        Args:
            column: The ColumnInfo for the array column
            base_path: The path to this array (e.g., 'line_items')

        Returns:
            A TRANSFORM() expression like: TRANSFORM(array_col, x -> STRUCT(x.field1 AS field1, ...))
        """
        # Get the element child (the struct definition)
        if not column.children:
            return self._quote_column_path(base_path)

        element_child = next((c for c in column.children if c.name == "element"), None)
        if not element_child or not element_child.children:
            # Shouldn't happen, but fallback to simple reference
            return self._quote_column_path(base_path)

        # Build the STRUCT reconstruction for each array element
        # Use 'item' as the lambda variable name
        field_expressions = []
        for field in element_child.children:
            field_ref = f"item.`{field.name}`"

            if field.is_complex and field.children:
                child_names = {c.name for c in field.children}

                # Nested array or map within the struct
                if child_names == {"element"}:
                    # Check if it's an array of structs (nested)
                    nested_element = field.children[0]
                    if nested_element.is_complex and nested_element.children:
                        # Nested ARRAY<STRUCT> - we need nested TRANSFORM with different lambda var
                        nested_transform = self._build_nested_array_of_struct_in_array(
                            field, "item", field.name, depth=0
                        )
                        field_expressions.append(f"{nested_transform} AS `{field.name}`")
                    else:
                        # ARRAY<primitive>
                        field_expressions.append(f"{field_ref} AS `{field.name}`")
                elif child_names == {"key", "value"}:
                    # MAP
                    field_expressions.append(f"{field_ref} AS `{field.name}`")
                else:
                    # Nested STRUCT within the array element
                    nested_struct_expr = self._build_nested_struct_in_array(field, "item")
                    field_expressions.append(f"{nested_struct_expr} AS `{field.name}`")
            else:
                # Simple field
                field_expressions.append(f"{field_ref} AS `{field.name}`")

        struct_expr = f"STRUCT({', '.join(field_expressions)})"
        array_path = self._quote_column_path(base_path)

        return f"TRANSFORM({array_path}, item -> {struct_expr})"

    def _build_nested_array_of_struct_in_array(
        self, column: ColumnInfo, outer_lambda_var: str, field_name: str, depth: int = 0
    ) -> str:
        """Build a nested TRANSFORM expression for ARRAY<STRUCT> within an array element.

        Args:
            column: The ColumnInfo for the nested array column
            outer_lambda_var: The lambda variable from the outer TRANSFORM (e.g., 'item')
            field_name: The field name containing this nested array
            depth: Current nesting depth (used to generate unique lambda variable names)

        Returns:
            A nested TRANSFORM() expression
        """
        if not column.children:
            return f"{outer_lambda_var}.`{field_name}`"

        element_child = next((c for c in column.children if c.name == "element"), None)
        if not element_child or not element_child.children:
            return f"{outer_lambda_var}.`{field_name}`"

        # Generate unique lambda variable name based on depth
        # 'item' -> 'item2' -> 'item3' -> 'item4' etc.
        nested_lambda_var = self._generate_lambda_var(depth + 1)

        field_expressions = []
        for field in element_child.children:
            field_ref = f"{nested_lambda_var}.`{field.name}`"

            if field.is_complex and field.children:
                child_names = {c.name for c in field.children}

                if child_names == {"element"}:
                    # Even deeper nesting - check if it's an array of structs
                    deeper_element = field.children[0]
                    if deeper_element.is_complex and deeper_element.children:
                        # Recursively handle even deeper ARRAY<STRUCT>
                        deeper_transform = self._build_nested_array_of_struct_in_array(
                            field, nested_lambda_var, field.name, depth + 1
                        )
                        field_expressions.append(f"{deeper_transform} AS `{field.name}`")
                    else:
                        # ARRAY<primitive>
                        field_expressions.append(f"{field_ref} AS `{field.name}`")
                elif child_names == {"key", "value"}:
                    # MAP
                    field_expressions.append(f"{field_ref} AS `{field.name}`")
                else:
                    # Nested STRUCT within this nested array element
                    nested_struct_expr = self._build_nested_struct_in_array(
                        field, nested_lambda_var
                    )
                    field_expressions.append(f"{nested_struct_expr} AS `{field.name}`")
            else:
                # Simple field
                field_expressions.append(f"{field_ref} AS `{field.name}`")

        struct_expr = f"STRUCT({', '.join(field_expressions)})"
        array_ref = f"{outer_lambda_var}.`{field_name}`"

        return f"TRANSFORM({array_ref}, {nested_lambda_var} -> {struct_expr})"

    def _generate_lambda_var(self, depth: int) -> str:
        """Generate a unique lambda variable name based on nesting depth.

        Args:
            depth: The nesting depth (0 for 'item', 1 for 'item2', 2 for 'item3', etc.)

        Returns:
            Lambda variable name (e.g., 'item', 'item2', 'item3')
        """
        if depth == 0:
            return "item"
        else:
            return f"item{depth + 1}"

    def _build_nested_struct_in_array(self, column: ColumnInfo, lambda_var: str) -> str:
        """Build a STRUCT expression for a struct nested within an array element.

        Args:
            column: The ColumnInfo for the nested struct
            lambda_var: The lambda variable name (e.g., 'item')

        Returns:
            A STRUCT() expression using the lambda variable
        """
        if not column.children:
            return f"{lambda_var}.`{column.name}`"

        field_expressions = []
        for child in column.children:
            child_ref = f"{lambda_var}.`{column.name}`.`{child.name}`"

            if child.is_complex and child.children:
                child_names = {c.name for c in child.children}

                # Handle nested complex types
                if child_names == {"element"} or child_names == {"key", "value"}:
                    # Array or Map - reference as-is
                    field_expressions.append(f"{child_ref} AS `{child.name}`")
                else:
                    # Further nested STRUCT - could recurse but keeping simple for now
                    field_expressions.append(f"{child_ref} AS `{child.name}`")
            else:
                # Simple field
                field_expressions.append(f"{child_ref} AS `{child.name}`")

        return f"STRUCT({', '.join(field_expressions)})"

    def _build_struct_expression(self, column: ColumnInfo, base_path: str) -> str:
        """Recursively build a STRUCT() expression for a struct column.

        Args:
            column: The ColumnInfo for the struct
            base_path: The path to this struct (e.g., 'address' or 'person.contact')

        Returns:
            A STRUCT() expression like: STRUCT(field1, field2, STRUCT(...) AS nested)
        """
        if not column.children:
            return self._quote_column_path(base_path)

        field_expressions = []
        for child in column.children:
            child_path = f"{base_path}.{child.name}"

            if child.is_complex and child.children:
                child_names = {c.name for c in child.children}

                # ARRAY - check if it contains structs
                if child_names == {"element"}:
                    element_child = child.children[0]
                    if element_child.is_complex and element_child.children:
                        # ARRAY<STRUCT<...>> within a struct field
                        array_expr = self._build_array_of_struct_in_struct(child, child_path)
                        field_expressions.append(f"{array_expr} AS `{child.name}`")
                    else:
                        # ARRAY<primitive>
                        field_expr = self._quote_column_path(child_path)
                        field_expressions.append(f"{field_expr} AS `{child.name}`")
                elif child_names == {"key", "value"}:
                    # MAP - reference as-is
                    field_expr = self._quote_column_path(child_path)
                    field_expressions.append(f"{field_expr} AS `{child.name}`")
                else:
                    # Nested STRUCT - recurse
                    nested_struct = self._build_struct_expression(child, child_path)
                    field_expressions.append(f"{nested_struct} AS `{child.name}`")
            else:
                # Simple field
                field_expr = self._quote_column_path(child_path)
                field_expressions.append(f"{field_expr} AS `{child.name}`")

        return f"STRUCT({', '.join(field_expressions)})"

    def _build_array_of_struct_in_struct(self, column: ColumnInfo, base_path: str) -> str:
        """Build a TRANSFORM expression for ARRAY<STRUCT<...>> within a struct field.

        Args:
            column: The ColumnInfo for the array column
            base_path: The path to this array (e.g., 'parent.child_array')

        Returns:
            A TRANSFORM() expression
        """
        if not column.children:
            return self._quote_column_path(base_path)

        element_child = next((c for c in column.children if c.name == "element"), None)
        if not element_child or not element_child.children:
            return self._quote_column_path(base_path)

        field_expressions = []
        for field in element_child.children:
            field_ref = f"item.`{field.name}`"

            if field.is_complex and field.children:
                child_names = {c.name for c in field.children}
                if child_names == {"element"}:
                    # Check if it's an array of structs (nested)
                    nested_element = field.children[0]
                    if nested_element.is_complex and nested_element.children:
                        # Nested ARRAY<STRUCT> - use nested TRANSFORM
                        nested_transform = self._build_nested_array_of_struct_in_array(
                            field, "item", field.name, depth=0
                        )
                        field_expressions.append(f"{nested_transform} AS `{field.name}`")
                    else:
                        # ARRAY<primitive>
                        field_expressions.append(f"{field_ref} AS `{field.name}`")
                elif child_names == {"key", "value"}:
                    # MAP
                    field_expressions.append(f"{field_ref} AS `{field.name}`")
                else:
                    # Nested struct
                    nested_struct_expr = self._build_nested_struct_in_array(field, "item")
                    field_expressions.append(f"{nested_struct_expr} AS `{field.name}`")
            else:
                field_expressions.append(f"{field_ref} AS `{field.name}`")

        struct_expr = f"STRUCT({', '.join(field_expressions)})"
        array_path = self._quote_column_path(base_path)

        return f"TRANSFORM({array_path}, item -> {struct_expr})"

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

    def _create_alias(self, path: str) -> str:
        """Create an alias for a nested field path.

        Converts dotted notation to underscore-separated format.

        Args:
            path: Column path with dots (e.g., 'parent.child.field')

        Returns:
            Backtick-quoted alias (e.g., '`parent_child_field`')
        """
        alias = path.replace(".", "_")
        return f"`{alias}`"


def generate_select(table_schema: TableSchema) -> str:
    """Convenience function to generate a SELECT statement from a table schema.

    Args:
        table_schema: The TableSchema object containing column information

    Returns:
        A complete SELECT statement string
    """
    generator = SQLGenerator(table_schema)
    return generator.generate_select()
