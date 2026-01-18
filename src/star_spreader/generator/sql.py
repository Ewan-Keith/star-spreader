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

        # Check if this is a STRUCT that needs reconstruction
        if column.is_complex and column.children:
            child_names = {child.name for child in column.children}

            # ARRAY and MAP are referenced as-is (no reconstruction needed)
            if child_names == {"element"} or child_names == {"key", "value"}:
                return [self._quote_column_path(column.name)]

            # STRUCT - reconstruct with STRUCT()
            struct_expr = self._build_struct_expression(column, column.name)
            return [f"{struct_expr} AS {self._quote_column_path(column.name)}"]
        else:
            # Simple column - just reference it
            return [self._quote_column_path(column.name)]

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

                # ARRAY or MAP - reference as-is
                if child_names == {"element"} or child_names == {"key", "value"}:
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
