"""EXPLAIN plan validator for comparing query equivalence.

This module provides functionality to validate whether two SQL queries are equivalent
by comparing their EXPLAIN plans in Databricks. This is particularly useful for
verifying that a SELECT * query produces the same execution plan as an explicit
column list query.
"""

import re
from typing import Any, Dict, Optional, Tuple

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


class ExplainValidator:
    """Validates query equivalence by comparing EXPLAIN plans in Databricks.

    This validator executes EXPLAIN queries against Databricks and compares the
    resulting logical plans to determine if two queries are equivalent. It's designed
    to verify that SELECT * expansions match explicit column lists.

    Attributes:
        workspace: The Databricks WorkspaceClient instance for API calls.
        warehouse_id: Optional warehouse ID to use for query execution.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        token: Optional[str] = None,
        workspace_client: Optional[WorkspaceClient] = None,
        warehouse_id: Optional[str] = None,
    ) -> None:
        """Initialize the EXPLAIN plan validator.

        Args:
            host: Databricks workspace host URL (e.g., 'https://company.cloud.databricks.com').
                  Required if workspace_client is not provided.
            token: Databricks personal access token. Required if workspace_client is not provided.
            workspace_client: Pre-configured WorkspaceClient instance. If provided, host and
                            token are ignored.
            warehouse_id: Optional Databricks SQL warehouse ID or HTTP path for query execution.
                         Can be either just the ID (e.g., '1b046cf442ff1288') or the full HTTP path
                         (e.g., '/sql/1.0/warehouses/1b046cf442ff1288').
                         If not provided, the default warehouse will be used.

        Raises:
            ValueError: If neither workspace_client nor (host and token) are provided.
        """
        if workspace_client is not None:
            self.workspace = workspace_client
        elif host is not None and token is not None:
            self.workspace = WorkspaceClient(host=host, token=token)
        else:
            raise ValueError("Either workspace_client or both host and token must be provided")

        # Extract warehouse ID from HTTP path if provided in that format
        self.warehouse_id = self._extract_warehouse_id(warehouse_id) if warehouse_id else None

    def validate_equivalence(
        self, select_star_query: str, explicit_query: str, catalog: str, schema: str
    ) -> Dict[str, Any]:
        """Validate that two queries produce equivalent execution plans.

        Executes EXPLAIN on both queries and compares their logical plans to determine
        if they will produce the same results.

        Args:
            select_star_query: The original query with SELECT *.
            explicit_query: The expanded query with explicit column list.
            catalog: Catalog name to set for query execution context.
            schema: Schema name to set for query execution context.

        Returns:
            Dictionary containing:
                - equivalent (bool): Whether the plans are equivalent.
                - select_star_plan (str): The EXPLAIN output for the SELECT * query.
                - explicit_plan (str): The EXPLAIN output for the explicit query.
                - differences (Optional[str]): Description of differences if not equivalent.

        Raises:
            Exception: If query execution fails or plans cannot be retrieved.
        """
        # Execute EXPLAIN on both queries
        select_star_plan = self._execute_explain(select_star_query, catalog, schema)
        explicit_plan = self._execute_explain(explicit_query, catalog, schema)

        # Extract logical plans from the EXPLAIN output
        select_star_logical = self._extract_logical_plan(select_star_plan)
        explicit_logical = self._extract_logical_plan(explicit_plan)

        # Compare the logical plans
        are_equivalent, differences = self._compare_plans(select_star_logical, explicit_logical)

        return {
            "equivalent": are_equivalent,
            "select_star_plan": select_star_plan,
            "explicit_plan": explicit_plan,
            "differences": differences,
        }

    @staticmethod
    def _extract_warehouse_id(warehouse_input: str) -> str:
        """Extract warehouse ID from either a plain ID or HTTP path.

        Args:
            warehouse_input: Either a warehouse ID (e.g., '1b046cf442ff1288') or
                           HTTP path (e.g., '/sql/1.0/warehouses/1b046cf442ff1288').

        Returns:
            The extracted warehouse ID.
        """
        # Check if input is an HTTP path format
        if warehouse_input.startswith("/sql/") or warehouse_input.startswith("sql/"):
            # Extract ID from path like: /sql/1.0/warehouses/{warehouse_id}
            parts = warehouse_input.split("/")
            if "warehouses" in parts:
                idx = parts.index("warehouses")
                if idx + 1 < len(parts):
                    return parts[idx + 1]

        # If not a path or extraction failed, return as-is (assume it's already just the ID)
        return warehouse_input

    def _execute_explain(self, query: str, catalog: str, schema: str) -> str:
        """Execute an EXPLAIN query and return the plan output.

        Args:
            query: The SQL query to explain.
            catalog: Catalog name for execution context.
            schema: Schema name for execution context.

        Returns:
            The EXPLAIN plan output as a string.

        Raises:
            Exception: If the query execution fails or times out.
        """
        # Prepare the EXPLAIN query
        explain_query = f"EXPLAIN {query}"

        # Set up execution parameters
        execution_params: Dict[str, Any] = {
            "statement": explain_query,
            "catalog": catalog,
            "schema": schema,
            "wait_timeout": "50s",  # Max allowed is 50 seconds
        }

        # Add warehouse_id if provided
        if self.warehouse_id:
            execution_params["warehouse_id"] = self.warehouse_id

        # Execute the statement
        response = self.workspace.statement_execution.execute_statement(**execution_params)

        # Check execution status
        if response.status and response.status.state == StatementState.FAILED:
            error_message = (
                response.status.error.message if response.status.error else "Unknown error"
            )
            raise Exception(f"EXPLAIN query failed: {error_message}")

        # Extract the result
        if not response.result or not response.result.data_array:
            raise Exception("No EXPLAIN output received")

        # The EXPLAIN output is typically in the first row, first column
        # data_array is a list of rows, where each row is a list of column values
        explain_output_rows = []
        for row in response.result.data_array:
            if row and len(row) > 0:
                # Each row typically contains the plan text
                explain_output_rows.append(str(row[0]))

        return "\n".join(explain_output_rows)

    def _extract_logical_plan(self, explain_output: str) -> str:
        """Extract the logical plan section from EXPLAIN output.

        Databricks EXPLAIN output typically contains multiple sections. This method
        extracts just the logical plan portion for comparison.

        Args:
            explain_output: The full EXPLAIN output text.

        Returns:
            The extracted logical plan text, normalized for comparison.
        """
        # Normalize whitespace and line endings
        normalized = explain_output.strip()

        # Look for common logical plan markers in Databricks EXPLAIN output
        # The exact format may vary, but typically includes sections like:
        # - "== Parsed Logical Plan =="
        # - "== Analyzed Logical Plan =="
        # - "== Optimized Logical Plan =="
        # - "== Physical Plan =="

        # Try to extract the Analyzed or Optimized Logical Plan section
        # as these are most stable for comparison
        logical_plan_match = re.search(
            r"== (?:Analyzed|Optimized) Logical Plan ==\s*\n(.*?)(?:\n== |$)",
            normalized,
            re.DOTALL | re.IGNORECASE,
        )

        if logical_plan_match:
            plan_text = logical_plan_match.group(1).strip()
        else:
            # If specific section not found, try to extract the main plan body
            # Skip header lines and extract the plan tree
            lines = normalized.split("\n")
            plan_lines = []
            in_plan = False

            for line in lines:
                # Skip empty lines and section headers
                if line.strip().startswith("==") or not line.strip():
                    if plan_lines:
                        # If we've started collecting plan lines and hit a new section, stop
                        break
                    continue

                in_plan = True
                plan_lines.append(line)

            plan_text = "\n".join(plan_lines) if plan_lines else normalized

        # Normalize the plan text for comparison
        # Remove extra whitespace and normalize line endings
        plan_text = re.sub(r"\s+", " ", plan_text).strip()

        return plan_text

    def _compare_plans(self, plan1: str, plan2: str) -> Tuple[bool, Optional[str]]:
        """Compare two logical plans for equivalence.

        This method performs a normalized comparison of query plans, accounting for
        minor formatting differences while detecting substantial logical differences.

        Args:
            plan1: First logical plan to compare.
            plan2: Second logical plan to compare.

        Returns:
            Tuple of (are_equivalent, differences_description):
                - are_equivalent: True if plans are logically equivalent.
                - differences_description: Description of differences if not equivalent,
                                          None if equivalent.
        """
        # Normalize both plans
        normalized_plan1 = self._normalize_plan(plan1)
        normalized_plan2 = self._normalize_plan(plan2)

        # Direct string comparison after normalization
        if normalized_plan1 == normalized_plan2:
            return (True, None)

        # If not exactly equal, try to identify the differences
        differences = self._identify_plan_differences(plan1, plan2)

        # For now, consider any difference as non-equivalent
        # Future enhancements could add semantic equivalence checking
        return (False, differences)

    def _normalize_plan(self, plan: str) -> str:
        """Normalize a plan string for comparison.

        Removes irrelevant differences like extra whitespace, aliases, and
        formatting variations.

        Args:
            plan: The plan text to normalize.

        Returns:
            Normalized plan string.
        """
        # Convert to lowercase for case-insensitive comparison
        normalized = plan.lower()

        # Normalize whitespace (collapse multiple spaces, normalize line endings)
        normalized = re.sub(r"\s+", " ", normalized)

        # Remove common aliases and temporary identifiers that might differ
        # but don't affect logical equivalence
        # Example: _tmp_123 or _gen_456
        normalized = re.sub(r"_(?:tmp|gen)_\d+", "_temp", normalized)

        # Remove specific object IDs or hashes that might differ
        normalized = re.sub(r"#\d+", "#id", normalized)

        # Strip leading/trailing whitespace
        normalized = normalized.strip()

        return normalized

    def _identify_plan_differences(self, plan1: str, plan2: str) -> str:
        """Identify and describe differences between two plans.

        Args:
            plan1: First plan text.
            plan2: Second plan text.

        Returns:
            A human-readable description of the differences found.
        """
        # Split into lines for line-by-line comparison
        lines1 = plan1.split("\n")
        lines2 = plan2.split("\n")

        differences = []

        # Check for length difference
        if len(lines1) != len(lines2):
            differences.append(f"Plan length differs: {len(lines1)} lines vs {len(lines2)} lines")

        # Find differing lines (simple approach)
        max_len = max(len(lines1), len(lines2))
        for i in range(max_len):
            line1 = lines1[i] if i < len(lines1) else ""
            line2 = lines2[i] if i < len(lines2) else ""

            if line1.strip() != line2.strip():
                differences.append(f"Line {i + 1} differs:")
                if line1:
                    differences.append(f"  SELECT *: {line1[:100]}")
                if line2:
                    differences.append(f"  Explicit: {line2[:100]}")

                # Limit to first 5 differences to avoid overwhelming output
                if len(differences) > 10:
                    differences.append("... (additional differences omitted)")
                    break

        if not differences:
            differences.append("Plans differ in normalization or structure")

        return "\n".join(differences)
