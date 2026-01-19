"""Tests for SQL generation from schema trees.

These tests verify that SchemaTreeSQLGenerator correctly generates SQL
for various schema patterns including simple columns, structs, arrays, maps,
and complex nested combinations.
"""

from star_spreader.schema_tree.nodes import (
    ArrayNode,
    MapNode,
    SimpleColumnNode,
    StructNode,
    TableSchemaNode,
)
from star_spreader.generator.sql_schema_tree import (
    SchemaTreeSQLGenerator,
    generate_select_from_schema_tree,
)


def test_simple_columns():
    """Test generating SELECT for table with only simple columns."""
    schema_tree = TableSchemaNode(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            SimpleColumnNode(name="id", data_type="INT", nullable=False),
            SimpleColumnNode(name="name", data_type="STRING", nullable=True),
            SimpleColumnNode(name="age", data_type="INT", nullable=True),
        ],
    )

    generator = SchemaTreeSQLGenerator(schema_tree)
    result = generator.generate_select()

    expected = """SELECT `id`,
       `name`,
       `age`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_struct_column():
    """Test generating SELECT for table with struct column."""
    schema_tree = TableSchemaNode(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            SimpleColumnNode(name="id", data_type="INT", nullable=False),
            StructNode(
                name="address",
                data_type="STRUCT<street:STRING,city:STRING,zip:INT>",
                nullable=True,
                fields=[
                    SimpleColumnNode(name="street", data_type="STRING", nullable=True),
                    SimpleColumnNode(name="city", data_type="STRING", nullable=True),
                    SimpleColumnNode(name="zip", data_type="INT", nullable=True),
                ],
            ),
        ],
    )

    generator = SchemaTreeSQLGenerator(schema_tree)
    result = generator.generate_select()

    expected = """SELECT `id`,
       STRUCT(`address`.`street` AS `street`, `address`.`city` AS `city`, `address`.`zip` AS `zip`) AS `address`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_nested_struct():
    """Test generating SELECT for table with nested struct columns."""
    schema_tree = TableSchemaNode(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            SimpleColumnNode(name="id", data_type="INT", nullable=False),
            StructNode(
                name="person",
                data_type="STRUCT<name:STRING,contact:STRUCT<email:STRING,phone:STRING>>",
                nullable=True,
                fields=[
                    SimpleColumnNode(name="name", data_type="STRING", nullable=True),
                    StructNode(
                        name="contact",
                        data_type="STRUCT<email:STRING,phone:STRING>",
                        nullable=True,
                        fields=[
                            SimpleColumnNode(name="email", data_type="STRING", nullable=True),
                            SimpleColumnNode(name="phone", data_type="STRING", nullable=True),
                        ],
                    ),
                ],
            ),
        ],
    )

    generator = SchemaTreeSQLGenerator(schema_tree)
    result = generator.generate_select()

    expected = """SELECT `id`,
       STRUCT(`person`.`name` AS `name`, STRUCT(`person`.`contact`.`email` AS `email`, `person`.`contact`.`phone` AS `phone`) AS `contact`) AS `person`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_array_column():
    """Test generating SELECT for table with array column."""
    schema_tree = TableSchemaNode(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            SimpleColumnNode(name="id", data_type="INT", nullable=False),
            ArrayNode(
                name="tags",
                data_type="ARRAY<STRING>",
                nullable=True,
                element_type=SimpleColumnNode(name="element", data_type="STRING", nullable=True),
            ),
        ],
    )

    generator = SchemaTreeSQLGenerator(schema_tree)
    result = generator.generate_select()

    expected = """SELECT `id`,
       `tags`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_array_of_struct_column():
    """Test generating SELECT for table with array of struct column."""
    schema_tree = TableSchemaNode(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            SimpleColumnNode(name="id", data_type="INT", nullable=False),
            ArrayNode(
                name="line_items",
                data_type="ARRAY<STRUCT<product_id:INT,quantity:INT,price:DECIMAL>>",
                nullable=True,
                element_type=StructNode(
                    name="element",
                    data_type="STRUCT<product_id:INT,quantity:INT,price:DECIMAL>",
                    nullable=True,
                    fields=[
                        SimpleColumnNode(name="product_id", data_type="INT", nullable=True),
                        SimpleColumnNode(name="quantity", data_type="INT", nullable=True),
                        SimpleColumnNode(name="price", data_type="DECIMAL", nullable=True),
                    ],
                ),
            ),
        ],
    )

    generator = SchemaTreeSQLGenerator(schema_tree)
    result = generator.generate_select()

    expected = """SELECT `id`,
       TRANSFORM(`line_items`, item -> STRUCT(item.`product_id` AS `product_id`, item.`quantity` AS `quantity`, item.`price` AS `price`)) AS `line_items`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_mixed_columns():
    """Test generating SELECT for table with mixed column types."""
    schema_tree = TableSchemaNode(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            SimpleColumnNode(name="id", data_type="INT", nullable=False),
            SimpleColumnNode(name="name", data_type="STRING", nullable=True),
            StructNode(
                name="metadata",
                data_type="STRUCT<created_at:TIMESTAMP,updated_at:TIMESTAMP>",
                nullable=True,
                fields=[
                    SimpleColumnNode(name="created_at", data_type="TIMESTAMP", nullable=True),
                    SimpleColumnNode(name="updated_at", data_type="TIMESTAMP", nullable=True),
                ],
            ),
            ArrayNode(
                name="tags",
                data_type="ARRAY<STRING>",
                nullable=True,
                element_type=SimpleColumnNode(name="element", data_type="STRING", nullable=True),
            ),
        ],
    )

    generator = SchemaTreeSQLGenerator(schema_tree)
    result = generator.generate_select()

    expected = """SELECT `id`,
       `name`,
       STRUCT(`metadata`.`created_at` AS `created_at`, `metadata`.`updated_at` AS `updated_at`) AS `metadata`,
       `tags`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_struct_with_array_of_struct():
    """Test generating SELECT for table with struct containing array of structs."""
    schema_tree = TableSchemaNode(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            SimpleColumnNode(name="id", data_type="INT", nullable=False),
            StructNode(
                name="order",
                data_type="STRUCT<order_id:INT,items:ARRAY<STRUCT<product:STRING,quantity:INT>>>",
                nullable=True,
                fields=[
                    SimpleColumnNode(name="order_id", data_type="INT", nullable=True),
                    ArrayNode(
                        name="items",
                        data_type="ARRAY<STRUCT<product:STRING,quantity:INT>>",
                        nullable=True,
                        element_type=StructNode(
                            name="element",
                            data_type="STRUCT<product:STRING,quantity:INT>",
                            nullable=True,
                            fields=[
                                SimpleColumnNode(name="product", data_type="STRING", nullable=True),
                                SimpleColumnNode(name="quantity", data_type="INT", nullable=True),
                            ],
                        ),
                    ),
                ],
            ),
        ],
    )

    generator = SchemaTreeSQLGenerator(schema_tree)
    result = generator.generate_select()

    expected = """SELECT `id`,
       STRUCT(`order`.`order_id` AS `order_id`, TRANSFORM(`order`.`items`, item -> STRUCT(item.`product` AS `product`, item.`quantity` AS `quantity`)) AS `items`) AS `order`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_array_of_struct_with_nested_struct():
    """Test ARRAY<STRUCT> where the struct contains another nested STRUCT."""
    schema_tree = TableSchemaNode(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            SimpleColumnNode(name="id", data_type="INT", nullable=False),
            ArrayNode(
                name="orders",
                data_type="ARRAY<STRUCT<order_id:INT,customer:STRUCT<name:STRING,email:STRING>>>",
                nullable=True,
                element_type=StructNode(
                    name="element",
                    data_type="STRUCT<order_id:INT,customer:STRUCT<name:STRING,email:STRING>>",
                    nullable=True,
                    fields=[
                        SimpleColumnNode(name="order_id", data_type="INT", nullable=True),
                        StructNode(
                            name="customer",
                            data_type="STRUCT<name:STRING,email:STRING>",
                            nullable=True,
                            fields=[
                                SimpleColumnNode(name="name", data_type="STRING", nullable=True),
                                SimpleColumnNode(name="email", data_type="STRING", nullable=True),
                            ],
                        ),
                    ],
                ),
            ),
        ],
    )

    generator = SchemaTreeSQLGenerator(schema_tree)
    result = generator.generate_select()

    expected = """SELECT `id`,
       TRANSFORM(`orders`, item -> STRUCT(item.`order_id` AS `order_id`, STRUCT(item.`customer`.`name` AS `name`, item.`customer`.`email` AS `email`) AS `customer`)) AS `orders`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_deeply_nested_array_struct_array_struct():
    """Test ARRAY<STRUCT<ARRAY<STRUCT>>> - arrays containing structs containing arrays containing structs."""
    schema_tree = TableSchemaNode(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            SimpleColumnNode(name="id", data_type="INT", nullable=False),
            ArrayNode(
                name="departments",
                data_type="ARRAY<STRUCT<dept_name:STRING,employees:ARRAY<STRUCT<emp_id:INT,emp_name:STRING>>>>",
                nullable=True,
                element_type=StructNode(
                    name="element",
                    data_type="STRUCT<dept_name:STRING,employees:ARRAY<STRUCT<emp_id:INT,emp_name:STRING>>>",
                    nullable=True,
                    fields=[
                        SimpleColumnNode(name="dept_name", data_type="STRING", nullable=True),
                        ArrayNode(
                            name="employees",
                            data_type="ARRAY<STRUCT<emp_id:INT,emp_name:STRING>>",
                            nullable=True,
                            element_type=StructNode(
                                name="element",
                                data_type="STRUCT<emp_id:INT,emp_name:STRING>",
                                nullable=True,
                                fields=[
                                    SimpleColumnNode(name="emp_id", data_type="INT", nullable=True),
                                    SimpleColumnNode(
                                        name="emp_name", data_type="STRING", nullable=True
                                    ),
                                ],
                            ),
                        ),
                    ],
                ),
            ),
        ],
    )

    generator = SchemaTreeSQLGenerator(schema_tree)
    result = generator.generate_select()

    # Note: Nested arrays should use different lambda variable names
    assert "TRANSFORM(`departments`, item ->" in result
    assert "TRANSFORM(item.`employees`, item2 ->" in result
    assert "item2.`emp_id`" in result
    assert "item2.`emp_name`" in result


def test_struct_with_multiple_array_fields():
    """Test STRUCT containing multiple ARRAY<STRUCT> fields."""
    schema_tree = TableSchemaNode(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            SimpleColumnNode(name="id", data_type="INT", nullable=False),
            StructNode(
                name="data",
                data_type="STRUCT<...>",
                nullable=True,
                fields=[
                    ArrayNode(
                        name="orders",
                        data_type="ARRAY<STRUCT<order_id:INT>>",
                        nullable=True,
                        element_type=StructNode(
                            name="element",
                            data_type="STRUCT<order_id:INT>",
                            nullable=True,
                            fields=[
                                SimpleColumnNode(name="order_id", data_type="INT", nullable=True),
                            ],
                        ),
                    ),
                    ArrayNode(
                        name="shipments",
                        data_type="ARRAY<STRUCT<shipment_id:INT>>",
                        nullable=True,
                        element_type=StructNode(
                            name="element",
                            data_type="STRUCT<shipment_id:INT>",
                            nullable=True,
                            fields=[
                                SimpleColumnNode(
                                    name="shipment_id", data_type="INT", nullable=True
                                ),
                            ],
                        ),
                    ),
                ],
            ),
        ],
    )

    generator = SchemaTreeSQLGenerator(schema_tree)
    result = generator.generate_select()

    # Both arrays should have independent lambda variables
    assert "TRANSFORM(`data`.`orders`, item ->" in result
    assert "TRANSFORM(`data`.`shipments`, item ->" in result


def test_triple_nested_struct():
    """Test deeply nested STRUCT (3 levels deep)."""
    schema_tree = TableSchemaNode(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            SimpleColumnNode(name="id", data_type="INT", nullable=False),
            StructNode(
                name="level1",
                data_type="STRUCT<...>",
                nullable=True,
                fields=[
                    StructNode(
                        name="level2",
                        data_type="STRUCT<...>",
                        nullable=True,
                        fields=[
                            StructNode(
                                name="level3",
                                data_type="STRUCT<value:INT>",
                                nullable=True,
                                fields=[
                                    SimpleColumnNode(name="value", data_type="INT", nullable=True),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )

    generator = SchemaTreeSQLGenerator(schema_tree)
    result = generator.generate_select()

    # Verify nested STRUCT generation
    assert "STRUCT(STRUCT(STRUCT(" in result
    assert "`level1`.`level2`.`level3`.`value`" in result


def test_multiple_independent_nested_arrays_no_lambda_conflict():
    """Test two top-level ARRAY<STRUCT<ARRAY<STRUCT>>> fields to ensure lambda vars don't conflict."""
    schema_tree = TableSchemaNode(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            SimpleColumnNode(name="id", data_type="INT", nullable=False),
            ArrayNode(
                name="field1",
                data_type="ARRAY<STRUCT<nested:ARRAY<STRUCT<val:INT>>>>",
                nullable=True,
                element_type=StructNode(
                    name="element",
                    data_type="STRUCT<nested:ARRAY<STRUCT<val:INT>>>",
                    nullable=True,
                    fields=[
                        ArrayNode(
                            name="nested",
                            data_type="ARRAY<STRUCT<val:INT>>",
                            nullable=True,
                            element_type=StructNode(
                                name="element",
                                data_type="STRUCT<val:INT>",
                                nullable=True,
                                fields=[
                                    SimpleColumnNode(name="val", data_type="INT", nullable=True),
                                ],
                            ),
                        ),
                    ],
                ),
            ),
            ArrayNode(
                name="field2",
                data_type="ARRAY<STRUCT<nested:ARRAY<STRUCT<val:INT>>>>",
                nullable=True,
                element_type=StructNode(
                    name="element",
                    data_type="STRUCT<nested:ARRAY<STRUCT<val:INT>>>",
                    nullable=True,
                    fields=[
                        ArrayNode(
                            name="nested",
                            data_type="ARRAY<STRUCT<val:INT>>",
                            nullable=True,
                            element_type=StructNode(
                                name="element",
                                data_type="STRUCT<val:INT>",
                                nullable=True,
                                fields=[
                                    SimpleColumnNode(name="val", data_type="INT", nullable=True),
                                ],
                            ),
                        ),
                    ],
                ),
            ),
        ],
    )

    generator = SchemaTreeSQLGenerator(schema_tree)
    result = generator.generate_select()

    # Verify both fields generate correctly without lambda conflicts
    assert "`field1`" in result
    assert "`field2`" in result
    # Each nested structure should have its own lambda scoping
    assert result.count("item ->") >= 2
    assert result.count("item2 ->") >= 2


def test_extreme_nesting_three_level_deep_arrays():
    """Test STRUCT<ARRAY<STRUCT<ARRAY<STRUCT<ARRAY<STRUCT>>>>>> - 3 levels of nested arrays."""
    schema_tree = TableSchemaNode(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            SimpleColumnNode(name="id", data_type="INT", nullable=False),
            StructNode(
                name="complex",
                data_type="STRUCT<...>",
                nullable=True,
                fields=[
                    ArrayNode(
                        name="level1",
                        data_type="ARRAY<STRUCT<...>>",
                        nullable=True,
                        element_type=StructNode(
                            name="element",
                            data_type="STRUCT<...>",
                            nullable=True,
                            fields=[
                                ArrayNode(
                                    name="level2",
                                    data_type="ARRAY<STRUCT<...>>",
                                    nullable=True,
                                    element_type=StructNode(
                                        name="element",
                                        data_type="STRUCT<...>",
                                        nullable=True,
                                        fields=[
                                            ArrayNode(
                                                name="level3",
                                                data_type="ARRAY<STRUCT<value:INT>>",
                                                nullable=True,
                                                element_type=StructNode(
                                                    name="element",
                                                    data_type="STRUCT<value:INT>",
                                                    nullable=True,
                                                    fields=[
                                                        SimpleColumnNode(
                                                            name="value",
                                                            data_type="INT",
                                                            nullable=True,
                                                        ),
                                                    ],
                                                ),
                                            ),
                                        ],
                                    ),
                                ),
                            ],
                        ),
                    ),
                ],
            ),
        ],
    )

    generator = SchemaTreeSQLGenerator(schema_tree)
    result = generator.generate_select()

    # Verify 3 levels of TRANSFORM with different lambda variables
    assert "TRANSFORM(`complex`.`level1`, item ->" in result
    assert "TRANSFORM(item.`level2`, item2 ->" in result
    assert "TRANSFORM(item2.`level3`, item3 ->" in result
    assert "item3.`value`" in result


def test_complex_real_world_scenario():
    """Test a complex real-world-like schema with multiple nesting patterns."""
    schema_tree = TableSchemaNode(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            SimpleColumnNode(name="id", data_type="BIGINT", nullable=False),
            SimpleColumnNode(name="created_at", data_type="TIMESTAMP", nullable=False),
            StructNode(
                name="user_info",
                data_type="STRUCT<...>",
                nullable=True,
                fields=[
                    SimpleColumnNode(name="name", data_type="STRING", nullable=True),
                    SimpleColumnNode(name="email", data_type="STRING", nullable=True),
                    StructNode(
                        name="address",
                        data_type="STRUCT<...>",
                        nullable=True,
                        fields=[
                            SimpleColumnNode(name="street", data_type="STRING", nullable=True),
                            SimpleColumnNode(name="city", data_type="STRING", nullable=True),
                        ],
                    ),
                ],
            ),
            ArrayNode(
                name="orders",
                data_type="ARRAY<STRUCT<...>>",
                nullable=True,
                element_type=StructNode(
                    name="element",
                    data_type="STRUCT<...>",
                    nullable=True,
                    fields=[
                        SimpleColumnNode(name="order_id", data_type="INT", nullable=True),
                        ArrayNode(
                            name="items",
                            data_type="ARRAY<STRUCT<...>>",
                            nullable=True,
                            element_type=StructNode(
                                name="element",
                                data_type="STRUCT<...>",
                                nullable=True,
                                fields=[
                                    SimpleColumnNode(
                                        name="product_id", data_type="INT", nullable=True
                                    ),
                                    SimpleColumnNode(
                                        name="quantity", data_type="INT", nullable=True
                                    ),
                                ],
                            ),
                        ),
                    ],
                ),
            ),
            MapNode(
                name="tags",
                data_type="MAP<STRING,STRING>",
                nullable=True,
                key_type=SimpleColumnNode(name="key", data_type="STRING", nullable=False),
                value_type=SimpleColumnNode(name="value", data_type="STRING", nullable=True),
            ),
        ],
    )

    generator = SchemaTreeSQLGenerator(schema_tree)
    result = generator.generate_select()

    # Verify all components are present
    assert "`id`" in result
    assert "`created_at`" in result
    assert "STRUCT(" in result  # user_info
    assert "`user_info`.`name`" in result
    assert "`user_info`.`address`.`city`" in result
    assert "TRANSFORM(`orders`, item ->" in result
    assert "TRANSFORM(item.`items`, item2 ->" in result
    assert "`tags`" in result  # MAP is referenced directly


def test_convenience_function():
    """Test the convenience function generate_select_from_schema_tree."""
    schema_tree = TableSchemaNode(
        catalog="test",
        schema_name="test",
        table_name="test",
        columns=[
            SimpleColumnNode(name="id", data_type="INT", nullable=False),
            SimpleColumnNode(name="name", data_type="STRING", nullable=True),
        ],
    )

    result = generate_select_from_schema_tree(schema_tree)

    assert "SELECT `id`" in result
    assert "`name`" in result
    assert "FROM `test`.`test`.`test`" in result
