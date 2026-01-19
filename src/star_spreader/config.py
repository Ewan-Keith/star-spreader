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

    Authentication:
        This uses Databricks Unified Authentication which automatically
        discovers credentials from your local environment:
        - Databricks CLI authentication (`databricks auth login`)
        - Azure CLI authentication
        - Environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN)
        - Configuration file (~/.databrickscfg)
        - Other cloud provider auth methods

    Environment Variables:
        DATABRICKS_HOST: (Optional) Databricks workspace URL to connect to
        DATABRICKS_WAREHOUSE_ID: SQL warehouse HTTP path (e.g., '/sql/1.0/warehouses/abc123') or ID
        DATABRICKS_CATALOG: Default catalog name (e.g., 'main', 'hive_metastore')
        DATABRICKS_SCHEMA: Default schema/database name

    Example:
        >>> # Authenticate once with databricks CLI
        >>> # $ databricks auth login
        >>> config = Config()
        >>> client = config.get_workspace_client()  # Uses local databricks auth
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    databricks_host: Optional[str] = Field(
        default=None,
        description="Databricks workspace host URL (optional, uses unified auth discovery if not set)",
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

        This method uses Databricks Unified Authentication which automatically
        discovers credentials from your local environment:
        - Databricks CLI authentication (`databricks auth login`)
        - Azure CLI authentication
        - Environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN)
        - Configuration file (~/.databrickscfg)
        - Other cloud provider auth methods

        Returns:
            A configured WorkspaceClient instance ready for use.

        Raises:
            Exception: If authentication fails or credentials are invalid.

        Example:
            >>> # Authenticate with databricks CLI first
            >>> # $ databricks auth login
            >>> config = Config()
            >>> workspace = config.get_workspace_client()
            >>> tables = workspace.tables.list(catalog_name="main", schema_name="default")
        """
        if self.databricks_host:
            # Host specified - use unified auth with specific host
            return WorkspaceClient(host=self.databricks_host)
        else:
            # No host specified - use unified auth discovery
            return WorkspaceClient()

    def validate_config(self) -> dict[str, bool]:
        """Validate the current configuration and report status.

        Checks which settings are explicitly configured. Note that workspace
        authentication works via unified auth even if workspace_configured is False.

        Returns:
            Dictionary with validation status for each setting:
            {
                "workspace_configured": bool,  # True if explicit host provided
                "warehouse_configured": bool,
                "catalog_configured": bool,
                "schema_configured": bool,
            }

        Example:
            >>> config = Config()
            >>> status = config.validate_config()
            >>> if not status["workspace_configured"]:
            ...     print("Using unified authentication with auto-discovery")
        """
        return {
            "workspace_configured": bool(self.databricks_host),
            "warehouse_configured": bool(self.databricks_warehouse_id),
            "catalog_configured": bool(self.databricks_catalog),
            "schema_configured": bool(self.databricks_schema),
        }

    def __repr__(self) -> str:
        """Return a string representation of the configuration.

        Returns:
            String representation of the config.
        """
        return (
            f"Config("
            f"host={self.databricks_host!r}, "
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
