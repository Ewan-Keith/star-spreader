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
       `address`.`street` AS `address_street`,
       `address`.`city` AS `address_city`,
       `address`.`zip` AS `address_zip`
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
       `person`.`name` AS `person_name`,
       `person`.`contact`.`email` AS `person_contact_email`,
       `person`.`contact`.`phone` AS `person_contact_phone`
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
                children=None,
            ),
        ],
    )

    generator = SQLGenerator(schema)
    result = generator.generate_select()

    expected = """SELECT `id`,
       `tags`
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
                children=None,
            ),
        ],
    )

    generator = SQLGenerator(schema)
    result = generator.generate_select()

    expected = """SELECT `id`,
       `name`,
       `metadata`.`created_at` AS `metadata_created_at`,
       `metadata`.`updated_at` AS `metadata_updated_at`,
       `tags`
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
