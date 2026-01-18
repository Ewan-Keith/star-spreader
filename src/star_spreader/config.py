"""Configuration management for star-spreader.

This module provides a pydantic-based configuration system that loads settings
from environment variables and provides convenient access to Databricks clients.
"""

from typing import Optional

from databricks.sdk import WorkspaceClient
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    """Configuration settings for star-spreader.

    This class uses pydantic-settings to automatically load configuration
    from environment variables. All settings can be overridden by setting
    the corresponding environment variable.

    Environment Variables:
        DATABRICKS_HOST: Databricks workspace URL (e.g., 'https://company.cloud.databricks.com')
        DATABRICKS_TOKEN: Personal access token for authentication
        DATABRICKS_WAREHOUSE_ID: SQL warehouse ID for query execution and validation
        DATABRICKS_CATALOG: Default catalog name (e.g., 'main', 'hive_metastore')
        DATABRICKS_SCHEMA: Default schema/database name

    Example:
        >>> import os
        >>> os.environ["DATABRICKS_HOST"] = "https://company.cloud.databricks.com"
        >>> os.environ["DATABRICKS_TOKEN"] = "dapi..."
        >>> config = Config()
        >>> client = config.get_workspace_client()
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    databricks_host: Optional[str] = Field(
        default=None,
        description="Databricks workspace host URL",
    )

    databricks_token: Optional[str] = Field(
        default=None,
        description="Databricks personal access token",
    )

    databricks_warehouse_id: Optional[str] = Field(
        default=None,
        description="Databricks SQL warehouse ID or HTTP path (e.g., '1b046cf442ff1288' or '/sql/1.0/warehouses/1b046cf442ff1288')",
    )

    databricks_catalog: str = Field(
        default="main",
        description="Default Databricks catalog name",
    )

    databricks_schema: str = Field(
        default="default",
        description="Default Databricks schema/database name",
    )

    def get_workspace_client(self) -> WorkspaceClient:
        """Create and return a configured Databricks WorkspaceClient.

        This method creates a WorkspaceClient using the configured host and token.
        If host and token are not configured, it falls back to the Databricks SDK's
        default authentication chain (environment variables, config file, Azure CLI, etc.).

        Returns:
            A configured WorkspaceClient instance ready for use.

        Raises:
            Exception: If authentication fails or credentials are invalid.

        Example:
            >>> config = Config()
            >>> workspace = config.get_workspace_client()
            >>> tables = workspace.tables.list(catalog_name="main", schema_name="default")
        """
        if self.databricks_host and self.databricks_token:
            return WorkspaceClient(
                host=self.databricks_host,
                token=self.databricks_token,
            )
        else:
            return WorkspaceClient()

    def validate_config(self) -> dict[str, bool]:
        """Validate the current configuration and report status.

        Checks which required settings are configured and returns a status
        dictionary indicating what is available.

        Returns:
            Dictionary with validation status for each setting:
            {
                "workspace_configured": bool,
                "warehouse_configured": bool,
                "catalog_configured": bool,
                "schema_configured": bool,
            }

        Example:
            >>> config = Config()
            >>> status = config.validate_config()
            >>> if not status["workspace_configured"]:
            ...     print("Warning: Databricks workspace not configured")
        """
        return {
            "workspace_configured": bool(self.databricks_host and self.databricks_token),
            "warehouse_configured": bool(self.databricks_warehouse_id),
            "catalog_configured": bool(self.databricks_catalog),
            "schema_configured": bool(self.databricks_schema),
        }

    def __repr__(self) -> str:
        """Return a string representation with masked sensitive data.

        Returns:
            String representation with token masked for security.
        """
        token_display = "***" if self.databricks_token else None
        return (
            f"Config("
            f"host={self.databricks_host!r}, "
            f"token={token_display!r}, "
            f"warehouse_id={self.databricks_warehouse_id!r}, "
            f"catalog={self.databricks_catalog!r}, "
            f"schema={self.databricks_schema!r}"
            f")"
        )


def load_config() -> Config:
    """Load configuration from environment variables.

    This is a convenience function that creates and returns a Config instance.

    Returns:
        A Config instance with settings loaded from environment.

    Example:
        >>> config = load_config()
        >>> workspace = config.get_workspace_client()
    """
    return Config()
