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


def test_array_of_struct_with_nested_struct():
    """Test ARRAY<STRUCT> where the struct contains another nested STRUCT."""
    schema = TableSchema(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            ColumnInfo(name="id", data_type="INT", is_complex=False, nullable=False),
            ColumnInfo(
                name="employees",
                data_type="ARRAY<STRUCT<name:STRING,address:STRUCT<city:STRING,zip:INT>>>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(
                        name="element",
                        data_type="STRUCT<name:STRING,address:STRUCT<city:STRING,zip:INT>>",
                        is_complex=True,
                        nullable=True,
                        children=[
                            ColumnInfo(
                                name="name", data_type="STRING", is_complex=False, nullable=True
                            ),
                            ColumnInfo(
                                name="address",
                                data_type="STRUCT<city:STRING,zip:INT>",
                                is_complex=True,
                                nullable=True,
                                children=[
                                    ColumnInfo(
                                        name="city",
                                        data_type="STRING",
                                        is_complex=False,
                                        nullable=True,
                                    ),
                                    ColumnInfo(
                                        name="zip", data_type="INT", is_complex=False, nullable=True
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
       TRANSFORM(`employees`, item -> STRUCT(item.`name` AS `name`, STRUCT(item.`address`.`city` AS `city`, item.`address`.`zip` AS `zip`) AS `address`)) AS `employees`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_deeply_nested_array_struct_array_struct():
    """Test ARRAY<STRUCT<ARRAY<STRUCT>>> - arrays containing structs containing arrays containing structs."""
    schema = TableSchema(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            ColumnInfo(name="id", data_type="INT", is_complex=False, nullable=False),
            ColumnInfo(
                name="departments",
                data_type="ARRAY<STRUCT<dept_name:STRING,teams:ARRAY<STRUCT<team_name:STRING,size:INT>>>>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(
                        name="element",
                        data_type="STRUCT<dept_name:STRING,teams:ARRAY<STRUCT<team_name:STRING,size:INT>>>",
                        is_complex=True,
                        nullable=True,
                        children=[
                            ColumnInfo(
                                name="dept_name",
                                data_type="STRING",
                                is_complex=False,
                                nullable=True,
                            ),
                            ColumnInfo(
                                name="teams",
                                data_type="ARRAY<STRUCT<team_name:STRING,size:INT>>",
                                is_complex=True,
                                nullable=True,
                                children=[
                                    ColumnInfo(
                                        name="element",
                                        data_type="STRUCT<team_name:STRING,size:INT>",
                                        is_complex=True,
                                        nullable=True,
                                        children=[
                                            ColumnInfo(
                                                name="team_name",
                                                data_type="STRING",
                                                is_complex=False,
                                                nullable=True,
                                            ),
                                            ColumnInfo(
                                                name="size",
                                                data_type="INT",
                                                is_complex=False,
                                                nullable=True,
                                            ),
                                        ],
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

    # Now handles nested ARRAY<STRUCT> with nested TRANSFORM
    expected = """SELECT `id`,
       TRANSFORM(`departments`, item -> STRUCT(item.`dept_name` AS `dept_name`, TRANSFORM(item.`teams`, item2 -> STRUCT(item2.`team_name` AS `team_name`, item2.`size` AS `size`)) AS `teams`)) AS `departments`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_struct_with_multiple_array_fields():
    """Test STRUCT containing multiple ARRAY<STRUCT> fields."""
    schema = TableSchema(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            ColumnInfo(name="id", data_type="INT", is_complex=False, nullable=False),
            ColumnInfo(
                name="company",
                data_type="STRUCT<name:STRING,employees:ARRAY<STRUCT<emp_id:INT>>,offices:ARRAY<STRUCT<location:STRING>>>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(name="name", data_type="STRING", is_complex=False, nullable=True),
                    ColumnInfo(
                        name="employees",
                        data_type="ARRAY<STRUCT<emp_id:INT>>",
                        is_complex=True,
                        nullable=True,
                        children=[
                            ColumnInfo(
                                name="element",
                                data_type="STRUCT<emp_id:INT>",
                                is_complex=True,
                                nullable=True,
                                children=[
                                    ColumnInfo(
                                        name="emp_id",
                                        data_type="INT",
                                        is_complex=False,
                                        nullable=True,
                                    ),
                                ],
                            ),
                        ],
                    ),
                    ColumnInfo(
                        name="offices",
                        data_type="ARRAY<STRUCT<location:STRING>>",
                        is_complex=True,
                        nullable=True,
                        children=[
                            ColumnInfo(
                                name="element",
                                data_type="STRUCT<location:STRING>",
                                is_complex=True,
                                nullable=True,
                                children=[
                                    ColumnInfo(
                                        name="location",
                                        data_type="STRING",
                                        is_complex=False,
                                        nullable=True,
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
       STRUCT(`company`.`name` AS `name`, TRANSFORM(`company`.`employees`, item -> STRUCT(item.`emp_id` AS `emp_id`)) AS `employees`, TRANSFORM(`company`.`offices`, item -> STRUCT(item.`location` AS `location`)) AS `offices`) AS `company`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_array_with_mixed_complex_types():
    """Test ARRAY<STRUCT> where struct contains ARRAY, MAP, and nested STRUCT."""
    schema = TableSchema(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            ColumnInfo(name="id", data_type="INT", is_complex=False, nullable=False),
            ColumnInfo(
                name="records",
                data_type="ARRAY<STRUCT<tags:ARRAY<STRING>,metadata:MAP<STRING,STRING>,details:STRUCT<key:STRING,value:INT>>>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(
                        name="element",
                        data_type="STRUCT<tags:ARRAY<STRING>,metadata:MAP<STRING,STRING>,details:STRUCT<key:STRING,value:INT>>",
                        is_complex=True,
                        nullable=True,
                        children=[
                            ColumnInfo(
                                name="tags",
                                data_type="ARRAY<STRING>",
                                is_complex=True,
                                nullable=True,
                                children=[
                                    ColumnInfo(
                                        name="element",
                                        data_type="STRING",
                                        is_complex=False,
                                        nullable=True,
                                    ),
                                ],
                            ),
                            ColumnInfo(
                                name="metadata",
                                data_type="MAP<STRING,STRING>",
                                is_complex=True,
                                nullable=True,
                                children=[
                                    ColumnInfo(
                                        name="key",
                                        data_type="STRING",
                                        is_complex=False,
                                        nullable=True,
                                    ),
                                    ColumnInfo(
                                        name="value",
                                        data_type="STRING",
                                        is_complex=False,
                                        nullable=True,
                                    ),
                                ],
                            ),
                            ColumnInfo(
                                name="details",
                                data_type="STRUCT<key:STRING,value:INT>",
                                is_complex=True,
                                nullable=True,
                                children=[
                                    ColumnInfo(
                                        name="key",
                                        data_type="STRING",
                                        is_complex=False,
                                        nullable=True,
                                    ),
                                    ColumnInfo(
                                        name="value",
                                        data_type="INT",
                                        is_complex=False,
                                        nullable=True,
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
       TRANSFORM(`records`, item -> STRUCT(item.`tags` AS `tags`, item.`metadata` AS `metadata`, STRUCT(item.`details`.`key` AS `key`, item.`details`.`value` AS `value`) AS `details`)) AS `records`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_triple_nested_struct():
    """Test deeply nested STRUCT (3 levels deep)."""
    schema = TableSchema(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            ColumnInfo(name="id", data_type="INT", is_complex=False, nullable=False),
            ColumnInfo(
                name="root",
                data_type="STRUCT<level1:STRUCT<level2:STRUCT<value:STRING>>>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(
                        name="level1",
                        data_type="STRUCT<level2:STRUCT<value:STRING>>",
                        is_complex=True,
                        nullable=True,
                        children=[
                            ColumnInfo(
                                name="level2",
                                data_type="STRUCT<value:STRING>",
                                is_complex=True,
                                nullable=True,
                                children=[
                                    ColumnInfo(
                                        name="value",
                                        data_type="STRING",
                                        is_complex=False,
                                        nullable=True,
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
       STRUCT(STRUCT(STRUCT(`root`.`level1`.`level2`.`value` AS `value`) AS `level2`) AS `level1`) AS `root`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_extreme_nesting_three_level_deep_arrays():
    """Test STRUCT<ARRAY<STRUCT<ARRAY<STRUCT<ARRAY<STRUCT>>>>>> - 3 levels of nested arrays."""
    schema = TableSchema(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            ColumnInfo(name="id", data_type="INT", is_complex=False, nullable=False),
            ColumnInfo(
                name="root",
                data_type="STRUCT<regions:ARRAY<STRUCT<region_name:STRING,countries:ARRAY<STRUCT<country_name:STRING,cities:ARRAY<STRUCT<city_name:STRING,population:INT>>>>>>>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(
                        name="regions",
                        data_type="ARRAY<STRUCT<region_name:STRING,countries:ARRAY<STRUCT<country_name:STRING,cities:ARRAY<STRUCT<city_name:STRING,population:INT>>>>>>",
                        is_complex=True,
                        nullable=True,
                        children=[
                            ColumnInfo(
                                name="element",
                                data_type="STRUCT<region_name:STRING,countries:ARRAY<STRUCT<country_name:STRING,cities:ARRAY<STRUCT<city_name:STRING,population:INT>>>>>",
                                is_complex=True,
                                nullable=True,
                                children=[
                                    ColumnInfo(
                                        name="region_name",
                                        data_type="STRING",
                                        is_complex=False,
                                        nullable=True,
                                    ),
                                    ColumnInfo(
                                        name="countries",
                                        data_type="ARRAY<STRUCT<country_name:STRING,cities:ARRAY<STRUCT<city_name:STRING,population:INT>>>>",
                                        is_complex=True,
                                        nullable=True,
                                        children=[
                                            ColumnInfo(
                                                name="element",
                                                data_type="STRUCT<country_name:STRING,cities:ARRAY<STRUCT<city_name:STRING,population:INT>>>",
                                                is_complex=True,
                                                nullable=True,
                                                children=[
                                                    ColumnInfo(
                                                        name="country_name",
                                                        data_type="STRING",
                                                        is_complex=False,
                                                        nullable=True,
                                                    ),
                                                    ColumnInfo(
                                                        name="cities",
                                                        data_type="ARRAY<STRUCT<city_name:STRING,population:INT>>",
                                                        is_complex=True,
                                                        nullable=True,
                                                        children=[
                                                            ColumnInfo(
                                                                name="element",
                                                                data_type="STRUCT<city_name:STRING,population:INT>",
                                                                is_complex=True,
                                                                nullable=True,
                                                                children=[
                                                                    ColumnInfo(
                                                                        name="city_name",
                                                                        data_type="STRING",
                                                                        is_complex=False,
                                                                        nullable=True,
                                                                    ),
                                                                    ColumnInfo(
                                                                        name="population",
                                                                        data_type="INT",
                                                                        is_complex=False,
                                                                        nullable=True,
                                                                    ),
                                                                ],
                                                            ),
                                                        ],
                                                    ),
                                                ],
                                            ),
                                        ],
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

    # Should handle 3 levels of nested TRANSFORM with unique lambda vars (item, item2, item3)
    expected = """SELECT `id`,
       STRUCT(TRANSFORM(`root`.`regions`, item -> STRUCT(item.`region_name` AS `region_name`, TRANSFORM(item.`countries`, item2 -> STRUCT(item2.`country_name` AS `country_name`, TRANSFORM(item2.`cities`, item3 -> STRUCT(item3.`city_name` AS `city_name`, item3.`population` AS `population`)) AS `cities`)) AS `countries`)) AS `regions`) AS `root`
FROM `my_catalog`.`my_schema`.`my_table`"""

    assert result == expected


def test_complex_real_world_scenario():
    """Test a complex real-world-like schema with multiple nesting patterns."""
    schema = TableSchema(
        catalog="my_catalog",
        schema_name="my_schema",
        table_name="my_table",
        columns=[
            ColumnInfo(name="id", data_type="BIGINT", is_complex=False, nullable=False),
            ColumnInfo(name="created_at", data_type="TIMESTAMP", is_complex=False, nullable=False),
            # Simple array
            ColumnInfo(
                name="tags",
                data_type="ARRAY<STRING>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(name="element", data_type="STRING", is_complex=False, nullable=True),
                ],
            ),
            # Struct with simple fields and nested struct
            ColumnInfo(
                name="user",
                data_type="STRUCT<name:STRING,email:STRING,address:STRUCT<city:STRING,country:STRING>>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(name="name", data_type="STRING", is_complex=False, nullable=True),
                    ColumnInfo(name="email", data_type="STRING", is_complex=False, nullable=True),
                    ColumnInfo(
                        name="address",
                        data_type="STRUCT<city:STRING,country:STRING>",
                        is_complex=True,
                        nullable=True,
                        children=[
                            ColumnInfo(
                                name="city", data_type="STRING", is_complex=False, nullable=True
                            ),
                            ColumnInfo(
                                name="country", data_type="STRING", is_complex=False, nullable=True
                            ),
                        ],
                    ),
                ],
            ),
            # Array of structs with nested struct
            ColumnInfo(
                name="orders",
                data_type="ARRAY<STRUCT<order_id:INT,shipping:STRUCT<carrier:STRING,tracking:STRING>>>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(
                        name="element",
                        data_type="STRUCT<order_id:INT,shipping:STRUCT<carrier:STRING,tracking:STRING>>",
                        is_complex=True,
                        nullable=True,
                        children=[
                            ColumnInfo(
                                name="order_id", data_type="INT", is_complex=False, nullable=True
                            ),
                            ColumnInfo(
                                name="shipping",
                                data_type="STRUCT<carrier:STRING,tracking:STRING>",
                                is_complex=True,
                                nullable=True,
                                children=[
                                    ColumnInfo(
                                        name="carrier",
                                        data_type="STRING",
                                        is_complex=False,
                                        nullable=True,
                                    ),
                                    ColumnInfo(
                                        name="tracking",
                                        data_type="STRING",
                                        is_complex=False,
                                        nullable=True,
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
            # Map
            ColumnInfo(
                name="metadata",
                data_type="MAP<STRING,STRING>",
                is_complex=True,
                nullable=True,
                children=[
                    ColumnInfo(name="key", data_type="STRING", is_complex=False, nullable=True),
                    ColumnInfo(name="value", data_type="STRING", is_complex=False, nullable=True),
                ],
            ),
        ],
    )

    generator = SQLGenerator(schema)
    result = generator.generate_select()

    expected = """SELECT `id`,
       `created_at`,
       `tags`,
       STRUCT(`user`.`name` AS `name`, `user`.`email` AS `email`, STRUCT(`user`.`address`.`city` AS `city`, `user`.`address`.`country` AS `country`) AS `address`) AS `user`,
       TRANSFORM(`orders`, item -> STRUCT(item.`order_id` AS `order_id`, STRUCT(item.`shipping`.`carrier` AS `carrier`, item.`shipping`.`tracking` AS `tracking`) AS `shipping`)) AS `orders`,
       `metadata`
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
