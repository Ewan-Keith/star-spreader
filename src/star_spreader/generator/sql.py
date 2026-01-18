"""SQL generation module for expanding SELECT * into explicit column lists."""

from typing import List

from star_spreader.schema.base import ColumnInfo, TableSchema


class SQLGenerator:
    """Generates explicit SELECT statements from table schema information.

    This class takes a TableSchema and generates a complete SELECT statement
    that explicitly lists all columns, including expanded struct fields with
    proper dotted notation and aliases.
    """

    def __init__(self, table_schema: TableSchema):
        """Initialize the SQL generator with a table schema.

        Args:
            table_schema: The TableSchema object containing column information
        """
        self.table_schema = table_schema

    def generate_select(self) -> str:
        """Generate a complete SELECT statement with all columns explicitly listed.

        This method expands struct fields into dotted notation (e.g., parent.child)
        and creates appropriate aliases. Array fields are included as-is.

        Returns:
            A complete SELECT statement string with format:
            SELECT col1, col2, struct1.field1 AS struct1_field1, ...
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
        """Expand all columns including nested struct fields.

        Returns:
            List of column expression strings, including expanded struct fields
        """
        expanded_columns = []

        for column in self.table_schema.columns:
            expanded_columns.extend(self._expand_column(column))

        return expanded_columns

    def _expand_column(self, column: ColumnInfo, parent_path: str = "") -> List[str]:
        """Recursively expand a column, handling struct fields with dotted notation.

        Args:
            column: The ColumnInfo object to expand
            parent_path: The parent column path (for nested structs)

        Returns:
            List of column expression strings. For simple columns, a single element.
            For struct columns, multiple elements for each nested field.
            For array/map columns, a single element (not expanded).
        """
        current_path = f"{parent_path}.{column.name}" if parent_path else column.name

        # Check if this is an ARRAY or MAP (should not be expanded)
        if column.is_complex and column.children:
            # ARRAY columns have a single child named 'element'
            # MAP columns have children named 'key' and 'value'
            # These should not be expanded - include them as-is
            child_names = {child.name for child in column.children}
            if child_names == {"element"} or child_names == {"key", "value"}:
                # Don't expand - treat as a simple column
                quoted_path = self._quote_column_path(current_path)
                if parent_path:
                    alias = self._create_alias(current_path)
                    return [f"{quoted_path} AS {alias}"]
                else:
                    return [quoted_path]

            # STRUCT columns - expand recursively
            expanded = []
            for child in column.children:
                expanded.extend(self._expand_column(child, current_path))
            return expanded
        else:
            quoted_path = self._quote_column_path(current_path)

            if parent_path:
                alias = self._create_alias(current_path)
                return [f"{quoted_path} AS {alias}"]
            else:
                return [quoted_path]

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
