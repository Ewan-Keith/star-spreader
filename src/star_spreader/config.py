"""Configuration management for star-spreader.

This module provides configuration for Databricks workspace connections using
unified authentication profiles.
"""

from typing import Optional

from databricks.sdk import WorkspaceClient
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    """Configuration settings for star-spreader.

    Uses Databricks Unified Authentication with profile support. Authentication is
    handled automatically by the Databricks SDK using profiles from ~/.databrickscfg.

    Authentication:
        Authenticate using the Databricks CLI:
        - `databricks auth login` - Creates a DEFAULT profile
        - `databricks auth login --profile <name>` - Creates a named profile

    Environment Variables:
        DATABRICKS_WAREHOUSE_ID: (Optional) SQL warehouse ID for query execution
                                 Only needed for functional tests

    Example:
        >>> # Authenticate once with databricks CLI
        >>> # $ databricks auth login
        >>> config = Config()
        >>> client = config.get_workspace_client()  # Uses DEFAULT profile

        >>> # Or use a specific profile
        >>> config = Config()
        >>> client = config.get_workspace_client(profile="production")
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    databricks_warehouse_id: Optional[str] = Field(
        default=None,
        description="Databricks SQL warehouse ID (optional, only needed for query execution)",
    )

    def get_workspace_client(self, profile: str = "DEFAULT") -> WorkspaceClient:
        """Create and return a configured Databricks WorkspaceClient.

        Uses Databricks Unified Authentication with the specified profile from
        ~/.databrickscfg. The profile contains workspace URL and authentication
        credentials.

        Args:
            profile: The profile name to use from ~/.databrickscfg (default: "DEFAULT")

        Returns:
            A configured WorkspaceClient instance ready for use.

        Raises:
            Exception: If authentication fails or the profile doesn't exist.

        Example:
            >>> # Use default profile
            >>> config = Config()
            >>> workspace = config.get_workspace_client()

            >>> # Use named profile
            >>> workspace = config.get_workspace_client(profile="production")
        """
        return WorkspaceClient(profile=profile)

    def __repr__(self) -> str:
        """Return a string representation of the configuration.

        Returns:
            String representation of the config.
        """
        return f"Config(warehouse_id={self.databricks_warehouse_id!r})"


def get_workspace_client(profile: str = "DEFAULT") -> WorkspaceClient:
    """Create a Databricks WorkspaceClient using the specified profile.

    This is a convenience function for quickly creating a client.

    Args:
        profile: The profile name to use from ~/.databrickscfg (default: "DEFAULT")

    Returns:
        A configured WorkspaceClient instance.

    Example:
        >>> # Use default profile
        >>> client = get_workspace_client()

        >>> # Use named profile
        >>> client = get_workspace_client(profile="production")
    """
    config = Config()
    return config.get_workspace_client(profile=profile)
