"""
Safety Gate Component - Pre-execution Checks and Limits.

This module performs pre-execution validation using EXPLAIN analysis to prevent
expensive queries from running and enforce resource quotas.
"""

from dataclasses import dataclass, field
from typing import Any, Optional

import structlog

from dremioai.analytics.compiler import CompiledQuery
from dremioai.api.transport import DremioAsyncHttpClient

logger = structlog.get_logger(__name__)


@dataclass
class QuotaInfo:
    """User quota information."""

    used: float
    available: float
    limit: float
    window: str  # "hourly", "daily", "monthly"
    unit: str = "DCU"  # or "queries", "rows"


@dataclass
class SafetyCheck:
    """Safety check result with detailed information."""

    approved: bool
    estimated_rows: Optional[int] = None
    estimated_cost_dcu: Optional[float] = None
    reflection_used: Optional[str] = None
    user_quota: Optional[QuotaInfo] = None
    violations: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


class SafetyGate:
    """
    Safety Gate component for pre-execution validation.

    This component:
    1. Runs EXPLAIN on Dremio to get execution plan
    2. Extracts estimated row count and cost
    3. Checks against configured limits
    4. Validates user quotas
    5. Approves or rejects query execution
    """

    def __init__(
        self,
        dremio_client: DremioAsyncHttpClient,
        max_rows: int = 1_000_000,
        max_cost_dcu: float = 100.0,
        max_date_range_days: int = 365,
        enable_quota_tracking: bool = True,
    ):
        """
        Initialize the Safety Gate.

        Args:
            dremio_client: Dremio API client
            max_rows: Maximum allowed estimated rows
            max_cost_dcu: Maximum allowed cost in DCU
            max_date_range_days: Maximum date range in days
            enable_quota_tracking: Whether to enforce user quotas
        """
        self.dremio_client = dremio_client
        self.max_rows = max_rows
        self.max_cost_dcu = max_cost_dcu
        self.max_date_range_days = max_date_range_days
        self.enable_quota_tracking = enable_quota_tracking

    async def check(
        self, compiled: CompiledQuery, user_id: Optional[str] = None
    ) -> SafetyCheck:
        """
        Perform safety checks on compiled query.

        Args:
            compiled: CompiledQuery from Compiler component
            user_id: Optional user ID for quota tracking

        Returns:
            SafetyCheck with approval decision and details
        """
        logger.info("running_safety_checks", sql_length=len(compiled.sql))

        violations = []
        warnings = []

        # Run EXPLAIN to get execution plan
        explain_result = await self._run_explain(compiled.sql)

        if not explain_result:
            violations.append("Failed to get EXPLAIN plan from Dremio")
            return SafetyCheck(
                approved=False,
                violations=violations,
                warnings=warnings,
            )

        # Extract estimates
        estimated_rows = explain_result.get("estimated_rows", 0)
        estimated_cost = explain_result.get("estimated_cost_dcu", 0.0)
        reflection_used = explain_result.get("reflection_used")

        logger.info(
            "explain_result",
            estimated_rows=estimated_rows,
            estimated_cost=estimated_cost,
            reflection_used=reflection_used,
        )

        # Check row limit
        if estimated_rows > self.max_rows:
            violations.append(
                f"Estimated rows ({estimated_rows:,}) exceeds limit ({self.max_rows:,}). "
                f"Add filters to reduce data volume."
            )

        # Check cost limit
        if estimated_cost > self.max_cost_dcu:
            violations.append(
                f"Estimated cost ({estimated_cost:.2f} DCU) exceeds limit "
                f"({self.max_cost_dcu:.2f} DCU). Optimize query or increase limit."
            )

        # Warning for no reflection
        if not reflection_used:
            warnings.append(
                "No reflection used. Query may be slower. "
                "Consider creating a reflection for this data."
            )

        # Check user quota
        user_quota = None
        if self.enable_quota_tracking and user_id:
            user_quota = await self._check_user_quota(user_id, estimated_cost)
            if user_quota and user_quota.available < estimated_cost:
                violations.append(
                    f"Insufficient quota. Required: {estimated_cost:.2f} DCU, "
                    f"Available: {user_quota.available:.2f} DCU"
                )

        # Determine approval
        approved = len(violations) == 0

        safety_check = SafetyCheck(
            approved=approved,
            estimated_rows=estimated_rows,
            estimated_cost_dcu=estimated_cost,
            reflection_used=reflection_used,
            user_quota=user_quota,
            violations=violations,
            warnings=warnings,
            metadata=explain_result,
        )

        if not approved:
            logger.warning(
                "safety_check_failed",
                violations=violations,
                estimated_rows=estimated_rows,
                estimated_cost=estimated_cost,
            )
        else:
            logger.info(
                "safety_check_passed",
                warnings=len(warnings),
                estimated_rows=estimated_rows,
                estimated_cost=estimated_cost,
            )

        return safety_check

    async def _run_explain(self, sql: str) -> Optional[dict[str, Any]]:
        """
        Run EXPLAIN on SQL query.

        Args:
            sql: SQL query to explain

        Returns:
            Dictionary with explain results

        Note:
            Dremio EXPLAIN returns execution plan in various formats.
            This method parses the plan to extract key metrics.
        """
        try:
            # Wrap query in EXPLAIN
            explain_sql = f"EXPLAIN PLAN FOR {sql}"

            logger.debug("running_explain", sql=explain_sql)

            # Execute EXPLAIN via Dremio API
            # Note: This uses the existing SQL execution infrastructure
            result = await self._execute_explain(explain_sql)

            if not result:
                return None

            # Parse EXPLAIN output
            # Dremio returns explain as text rows, we need to parse it
            parsed = self._parse_explain_output(result)

            return parsed

        except Exception as e:
            logger.error("explain_failed", error=str(e), sql=sql)
            return None

    async def _execute_explain(self, explain_sql: str) -> Optional[dict[str, Any]]:
        """
        Execute EXPLAIN query via Dremio API.

        Args:
            explain_sql: EXPLAIN SQL statement

        Returns:
            Query result

        Note:
            This is a simplified implementation. In production, use
            the existing DremioAsyncHttpClient.execute_sql method.
        """
        try:
            # In production, this would use:
            # result = await self.dremio_client.execute_sql(explain_sql)

            # For now, return mock data
            # TODO: Integrate with actual Dremio API
            return {
                "rows": [
                    {
                        "text": "00-00    Scan : rowcount = 45127.0, cumulative cost = {5.8 io, 0 cpu, 0 network, 234567 memory}"
                    }
                ]
            }

        except Exception as e:
            logger.error("explain_execution_failed", error=str(e))
            return None

    def _parse_explain_output(self, result: dict[str, Any]) -> dict[str, Any]:
        """
        Parse EXPLAIN output to extract metrics.

        Args:
            result: Raw EXPLAIN query result

        Returns:
            Parsed metrics

        Note:
            Dremio EXPLAIN format:
            - rowcount: Estimated number of rows
            - cumulative cost: {io, cpu, network, memory}
            - Reflection info appears in plan text
        """
        parsed = {
            "estimated_rows": 0,
            "estimated_cost_dcu": 0.0,
            "reflection_used": None,
            "raw_plan": [],
        }

        try:
            rows = result.get("rows", [])

            for row in rows:
                text = row.get("text", "")
                parsed["raw_plan"].append(text)

                # Extract rowcount
                if "rowcount" in text.lower():
                    import re

                    match = re.search(r"rowcount\s*=\s*([\d.]+)", text, re.IGNORECASE)
                    if match:
                        parsed["estimated_rows"] = int(float(match.group(1)))

                # Extract cost (io operations as proxy for DCU)
                if "cumulative cost" in text.lower():
                    import re

                    match = re.search(
                        r"\{([\d.]+)\s+io", text, re.IGNORECASE
                    )
                    if match:
                        # Rough approximation: 1 DCU ≈ 1 IO unit
                        parsed["estimated_cost_dcu"] = float(match.group(1))

                # Check for reflection usage
                if "reflection" in text.lower():
                    import re

                    match = re.search(
                        r"reflection\s*[:\[]?\s*(\w+)", text, re.IGNORECASE
                    )
                    if match:
                        parsed["reflection_used"] = match.group(1)

        except Exception as e:
            logger.error("explain_parse_error", error=str(e))

        return parsed

    async def _check_user_quota(
        self, user_id: str, estimated_cost: float
    ) -> QuotaInfo:
        """
        Check user quota availability.

        Args:
            user_id: User identifier
            estimated_cost: Estimated query cost in DCU

        Returns:
            QuotaInfo with current usage

        Note:
            In production, this would query a quota tracking service
            (e.g., Redis, PostgreSQL, or Dremio's usage API).
        """
        # Mock implementation
        # In production:
        # 1. Query quota service for user
        # 2. Get current usage in time window
        # 3. Calculate available quota
        # 4. Return QuotaInfo

        # Mock quota: 1000 DCU daily limit, 47.3 used
        quota = QuotaInfo(
            used=47.3,
            available=952.7,
            limit=1000.0,
            window="daily",
            unit="DCU",
        )

        logger.debug(
            "user_quota_checked",
            user_id=user_id,
            used=quota.used,
            available=quota.available,
        )

        return quota

    async def update_quota_usage(
        self, user_id: str, actual_cost: float
    ) -> None:
        """
        Update user quota after query execution.

        Args:
            user_id: User identifier
            actual_cost: Actual query cost in DCU

        Note:
            Called after query execution to track actual usage.
        """
        if not self.enable_quota_tracking:
            return

        # In production:
        # 1. Increment user's usage counter
        # 2. Store in quota tracking service
        # 3. Set expiry based on quota window

        logger.info(
            "quota_updated",
            user_id=user_id,
            cost=actual_cost,
        )
