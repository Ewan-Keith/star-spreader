"""Tests for SQL generator module."""

import pytest

from star_spreader.schema.base import ColumnInfo, TableSchema
from star_spreader.generator.sql import SQLGenerator, generate_select


def test_simple_columns():
    """Test generating SELECT for table with only simple columns."""
    schema = TableSchema(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            ColumnInfo(name="id", data_type="INT", is_complex=False, nullable=False),
            ColumnInfo(name="name", data_type="STRING", is_complex=False, nullable=True),
            ColumnInfo(name="age", data_type="INT", is_complex=False, nullable=True),
        ],
    )

    generator = SQLGenerator(schema)
    result = generator.generate_select()

    expected = """SELECT `id`,
       `name`,
       `age`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_struct_column():
    """Test generating SELECT for table with struct column."""
    schema = TableSchema(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            ColumnInfo(name="id", data_type="INT", is_complex=False, nullable=False),
            ColumnInfo(
                name="address",
                data_type="STRUCT<street:STRING,city:STRING,zip:INT>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(name="street", data_type="STRING", is_complex=False, nullable=True),
                    ColumnInfo(name="city", data_type="STRING", is_complex=False, nullable=True),
                    ColumnInfo(name="zip", data_type="INT", is_complex=False, nullable=True),
                ],
            ),
        ],
    )

    generator = SQLGenerator(schema)
    result = generator.generate_select()

    expected = """SELECT `id`,
       STRUCT(`address`.`street` AS `street`, `address`.`city` AS `city`, `address`.`zip` AS `zip`) AS `address`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_nested_struct():
    """Test generating SELECT for table with nested struct columns."""
    schema = TableSchema(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            ColumnInfo(name="id", data_type="INT", is_complex=False, nullable=False),
            ColumnInfo(
                name="person",
                data_type="STRUCT<name:STRING,contact:STRUCT<email:STRING,phone:STRING>>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(name="name", data_type="STRING", is_complex=False, nullable=True),
                    ColumnInfo(
                        name="contact",
                        data_type="STRUCT<email:STRING,phone:STRING>",
                        is_complex=True,
                        nullable=True,
                        children=[
                            ColumnInfo(
                                name="email", data_type="STRING", is_complex=False, nullable=True
                            ),
                            ColumnInfo(
                                name="phone", data_type="STRING", is_complex=False, nullable=True
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )

    generator = SQLGenerator(schema)
    result = generator.generate_select()

    expected = """SELECT `id`,
       STRUCT(`person`.`name` AS `name`, STRUCT(`person`.`contact`.`email` AS `email`, `person`.`contact`.`phone` AS `phone`) AS `contact`) AS `person`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_array_column():
    """Test generating SELECT for table with array column."""
    schema = TableSchema(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            ColumnInfo(name="id", data_type="INT", is_complex=False, nullable=False),
            ColumnInfo(
                name="tags",
                data_type="ARRAY<STRING>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(name="element", data_type="STRING", is_complex=False, nullable=True),
                ],
            ),
        ],
    )

    generator = SQLGenerator(schema)
    result = generator.generate_select()

    expected = """SELECT `id`,
       `tags`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_array_of_struct_column():
    """Test generating SELECT for table with array of struct column."""
    schema = TableSchema(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            ColumnInfo(name="id", data_type="INT", is_complex=False, nullable=False),
            ColumnInfo(
                name="line_items",
                data_type="ARRAY<STRUCT<product_id:INT,quantity:INT,price:DECIMAL>>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(
                        name="element",
                        data_type="STRUCT<product_id:INT,quantity:INT,price:DECIMAL>",
                        is_complex=True,
                        nullable=True,
                        children=[
                            ColumnInfo(
                                name="product_id", data_type="INT", is_complex=False, nullable=True
                            ),
                            ColumnInfo(
                                name="quantity", data_type="INT", is_complex=False, nullable=True
                            ),
                            ColumnInfo(
                                name="price", data_type="DECIMAL", is_complex=False, nullable=True
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )

    generator = SQLGenerator(schema)
    result = generator.generate_select()

    expected = """SELECT `id`,
       TRANSFORM(`line_items`, item -> STRUCT(item.`product_id` AS `product_id`, item.`quantity` AS `quantity`, item.`price` AS `price`)) AS `line_items`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_mixed_columns():
    """Test generating SELECT for table with mixed column types."""
    schema = TableSchema(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            ColumnInfo(name="id", data_type="INT", is_complex=False, nullable=False),
            ColumnInfo(name="name", data_type="STRING", is_complex=False, nullable=True),
            ColumnInfo(
                name="metadata",
                data_type="STRUCT<created_at:TIMESTAMP,updated_at:TIMESTAMP>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(
                        name="created_at", data_type="TIMESTAMP", is_complex=False, nullable=True
                    ),
                    ColumnInfo(
                        name="updated_at", data_type="TIMESTAMP", is_complex=False, nullable=True
                    ),
                ],
            ),
            ColumnInfo(
                name="tags",
                data_type="ARRAY<STRING>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(name="element", data_type="STRING", is_complex=False, nullable=True),
                ],
            ),
        ],
    )

    generator = SQLGenerator(schema)
    result = generator.generate_select()

    expected = """SELECT `id`,
       `name`,
       STRUCT(`metadata`.`created_at` AS `created_at`, `metadata`.`updated_at` AS `updated_at`) AS `metadata`,
       `tags`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_struct_with_array_of_struct():
    """Test generating SELECT for table with struct containing array of structs."""
    schema = TableSchema(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            ColumnInfo(name="id", data_type="INT", is_complex=False, nullable=False),
            ColumnInfo(
                name="order",
                data_type="STRUCT<order_id:INT,items:ARRAY<STRUCT<sku:STRING,qty:INT>>>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(name="order_id", data_type="INT", is_complex=False, nullable=True),
                    ColumnInfo(
                        name="items",
                        data_type="ARRAY<STRUCT<sku:STRING,qty:INT>>",
                        is_complex=True,
                        nullable=True,
                        children=[
                            ColumnInfo(
                                name="element",
                                data_type="STRUCT<sku:STRING,qty:INT>",
                                is_complex=True,
                                nullable=True,
                                children=[
                                    ColumnInfo(
                                        name="sku",
                                        data_type="STRING",
                                        is_complex=False,
                                        nullable=True,
                                    ),
                                    ColumnInfo(
                                        name="qty", data_type="INT", is_complex=False, nullable=True
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )

    generator = SQLGenerator(schema)
    result = generator.generate_select()

    expected = """SELECT `id`,
       STRUCT(`order`.`order_id` AS `order_id`, TRANSFORM(`order`.`items`, item -> STRUCT(item.`sku` AS `sku`, item.`qty` AS `qty`)) AS `items`) AS `order`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_convenience_function():
    """Test the convenience function generate_select()."""
    schema = TableSchema(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            ColumnInfo(name="id", data_type="INT", is_complex=False, nullable=False),
            ColumnInfo(name="name", data_type="STRING", is_complex=False, nullable=True),
        ],
    )

    result = generate_select(schema)

    expected = """SELECT `id`,
       `name`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected
