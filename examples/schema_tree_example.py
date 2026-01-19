#!/usr/bin/env python3
"""Example demonstrating the new Schema Tree-based architecture.

This example shows how the Schema Tree provides modularity and extensibility
for working with database schemas.
"""

from star_spreader.schema_tree.nodes import (
    ArrayNode,
    SimpleColumnNode,
    StructNode,
    TableSchemaNode,
)
from star_spreader.schema_tree.visitor import SchemaTreeVisitor
from star_spreader.generator.sql_schema_tree import SchemaTreeSQLGenerator


def create_example_schema():
    """Create an example table schema as an Schema Tree."""
    return TableSchemaNode(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="users",
        columns=[
            SimpleColumnNode(name="id", data_type="BIGINT", nullable=False),
            SimpleColumnNode(name="username", data_type="STRING", nullable=False),
            StructNode(
                name="profile",
                data_type="STRUCT<name: STRING, email: STRING, age: INT>",
                nullable=True,
                fields=[
                    SimpleColumnNode(name="name", data_type="STRING", nullable=True),
                    SimpleColumnNode(name="email", data_type="STRING", nullable=True),
                    SimpleColumnNode(name="age", data_type="INT", nullable=True),
                ],
            ),
            ArrayNode(
                name="orders",
                data_type="ARRAY<STRUCT<order_id: INT, amount: DECIMAL>>",
                nullable=True,
                element_type=StructNode(
                    name="element",
                    data_type="STRUCT<order_id: INT, amount: DECIMAL>",
                    nullable=True,
                    fields=[
                        SimpleColumnNode(name="order_id", data_type="INT", nullable=True),
                        SimpleColumnNode(name="amount", data_type="DECIMAL", nullable=True),
                    ],
                ),
            ),
        ],
    )


class SchemaAnalyzerVisitor(SchemaTreeVisitor):
    """Custom visitor that analyzes schema complexity."""

    def __init__(self):
        self.simple_count = 0
        self.struct_count = 0
        self.array_count = 0
        self.map_count = 0

    def visit_simple_column(self, node):
        self.simple_count += 1
        return f"Simple: {node.name}"

    def visit_struct(self, node):
        self.struct_count += 1
        # Visit all fields
        for field in node.fields:
            field.accept(self)
        return f"Struct: {node.name} with {len(node.fields)} fields"

    def visit_array(self, node):
        self.array_count += 1
        # Visit element type
        node.element_type.accept(self)
        return f"Array: {node.name}"

    def visit_map(self, node):
        self.map_count += 1
        node.key_type.accept(self)
        node.value_type.accept(self)
        return f"Map: {node.name}"

    def get_summary(self):
        return {
            "simple_columns": self.simple_count,
            "struct_columns": self.struct_count,
            "array_columns": self.array_count,
            "map_columns": self.map_count,
            "total_complex": self.struct_count + self.array_count + self.map_count,
        }


def main():
    """Demonstrate Schema Tree usage."""
    print("=" * 70)
    print("Schema Tree-Based Architecture Example")
    print("=" * 70)
    print()

    # Create schema Schema Tree
    schema_ast = create_example_schema()

    print(f"Table: {schema_ast.get_full_table_name()}")
    print(f"Columns: {len(schema_ast.columns)}")
    print()

    # Use SQL generator to create SELECT statement
    print("1. Generate SQL using Schema Tree SQL Generator:")
    print("-" * 70)
    sql_generator = SchemaTreeSQLGenerator(schema_ast)
    sql = sql_generator.generate_select()
    print(sql)
    print()

    # Use custom visitor to analyze schema
    print("2. Analyze schema complexity using custom visitor:")
    print("-" * 70)
    analyzer = SchemaAnalyzerVisitor()

    for column in schema_ast.columns:
        result = column.accept(analyzer)
        print(f"  - {result}")

    print()
    print("Summary:")
    summary = analyzer.get_summary()
    for key, value in summary.items():
        print(f"  {key}: {value}")

    print()
    print("=" * 70)
    print("Key Benefits of Schema Tree Architecture:")
    print("=" * 70)
    print("✓ Modular: Schema fetching is decoupled from SQL generation")
    print("✓ Extensible: Easy to add new visitors for different operations")
    print("✓ Type-safe: Each node type is explicitly represented")
    print("✓ Testable: Each component can be tested independently")
    print("✓ Maintainable: Clean separation of concerns")
    print()


if __name__ == "__main__":
    main()
