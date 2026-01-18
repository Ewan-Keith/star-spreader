"""Star Spreader - Convert SELECT * to explicit column lists."""

from star_spreader.config import Config, load_config
from star_spreader.generator.sql import SQLGenerator, generate_select
from star_spreader.schema.base import ColumnInfo, TableSchema
from star_spreader.schema.databricks import DatabricksSchemaFetcher

__version__ = "0.1.0"

__all__ = [
    "Config",
    "load_config",
    "SQLGenerator",
    "generate_select",
    "ColumnInfo",
    "TableSchema",
    "DatabricksSchemaFetcher",
]
