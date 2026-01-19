"""Configuration management for star-spreader.

This module provides utilities for creating Databricks workspace connections using
unified authentication profiles.
"""

from databricks.sdk import WorkspaceClient


def get_workspace_client(profile: str = "DEFAULT") -> WorkspaceClient:
    """Create a Databricks WorkspaceClient using the specified profile.

    Uses Databricks Unified Authentication with profiles from ~/.databrickscfg.
    Profiles are created via `databricks auth login`.

    Args:
        profile: The profile name to use from ~/.databrickscfg (default: "DEFAULT")

    Returns:
        A configured WorkspaceClient instance ready for use.

    Raises:
        Exception: If authentication fails or the profile doesn't exist.

    Example:
        >>> # Use default profile
        >>> client = get_workspace_client()

        >>> # Use named profile
        >>> client = get_workspace_client(profile="production")
    """
    return WorkspaceClient(profile=profile)
