"""
Diagnostics Agent - Root Cause Analysis for "Why" Queries.

This module performs automated diagnostics for questions like:
- "Why did revenue drop last month?"
- "Why are orders down in APAC?"
- "What caused the profit increase?"
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

import structlog

from dremioai.analytics.planner import GroundedPlan
from dremioai.api.transport import DremioAsyncHttpClient

logger = structlog.get_logger(__name__)


class DiagnosticStatus(str, Enum):
    """Diagnostic analysis status."""

    DIAGNOSED = "diagnosed"  # Successfully identified drivers
    UNCLEAR = "unclear"  # Could not determine clear drivers
    FAILED = "failed"  # Analysis failed
    PARTIAL = "partial"  # Partial diagnosis with low confidence


@dataclass
class Driver:
    """Root cause driver with impact quantification."""

    factor: str  # e.g., "promo_reduction", "stockouts"
    dimension: Optional[str] = None  # e.g., "product_category", "region"
    impact: float = 0.0  # Impact amount (e.g., -70000)
    impact_pct: float = 0.0  # Impact percentage of total variance
    evidence_sql: str = ""  # SQL query used for evidence
    evidence_data: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DiagnosticResult:
    """Result of diagnostic analysis."""

    status: DiagnosticStatus
    confidence: float  # 0.0 to 1.0
    baseline_value: float = 0.0
    current_value: float = 0.0
    delta: float = 0.0
    delta_pct: float = 0.0
    drivers: list[Driver] = field(default_factory=list)
    narrative: str = ""
    queries_executed: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


class DiagnosticsAgent:
    """
    Diagnostics Agent for automated root cause analysis.

    This agent implements diagnostic recipes:
    1. compare_periods - Compare metric across time periods
    2. decompose_variance - Break down changes by dimension
    3. rank_drivers - Identify top contributing factors
    4. check_events - Look for correlated events
    """

    # Hardcoded dimensions for decomposition (configurable per metric)
    DECOMPOSITION_DIMENSIONS = {
        "revenue": ["product_category", "region", "channel"],
        "profit": ["product_category", "region", "channel"],
        "orders": ["product_category", "region", "channel", "status"],
        "customers": ["segment", "region", "channel"],
    }

    # Event tables to check for correlations
    EVENT_TABLES = {
        "promotions": "marketing.promotions",
        "stockouts": "inventory.stockouts",
        "pricing": "commerce.price_changes",
        "campaigns": "marketing.campaigns",
    }

    # Confidence threshold for returning diagnosis
    CONFIDENCE_THRESHOLD = 0.8

    def __init__(self, dremio_client: DremioAsyncHttpClient):
        """
        Initialize the Diagnostics Agent.

        Args:
            dremio_client: Dremio API client for executing queries
        """
        self.dremio_client = dremio_client

    async def diagnose(
        self, plan: GroundedPlan, baseline_period: str, current_period: str
    ) -> DiagnosticResult:
        """
        Perform diagnostic analysis on metric change.

        Args:
            plan: GroundedPlan with metric to diagnose
            baseline_period: Baseline time period (e.g., "previous_month")
            current_period: Current time period (e.g., "last_month")

        Returns:
            DiagnosticResult with identified drivers and narrative
        """
        logger.info(
            "starting_diagnostics",
            metric=plan.metrics[0].canonical if plan.metrics else None,
            baseline=baseline_period,
            current=current_period,
        )

        if not plan.metrics:
            return DiagnosticResult(
                status=DiagnosticStatus.FAILED,
                confidence=0.0,
                narrative="No metric specified for diagnostics",
            )

        metric = plan.metrics[0]  # Diagnose first metric
        queries_executed = 0

        # Step 1: Compare periods
        logger.info("step_1_compare_periods")
        baseline_value, current_value = await self._compare_periods(
            metric, baseline_period, current_period
        )
        queries_executed += 2

        delta = current_value - baseline_value
        delta_pct = (delta / baseline_value * 100) if baseline_value != 0 else 0

        logger.info(
            "period_comparison",
            baseline=baseline_value,
            current=current_value,
            delta=delta,
            delta_pct=delta_pct,
        )

        # If no significant change, return early
        if abs(delta_pct) < 5.0:
            return DiagnosticResult(
                status=DiagnosticStatus.UNCLEAR,
                confidence=0.5,
                baseline_value=baseline_value,
                current_value=current_value,
                delta=delta,
                delta_pct=delta_pct,
                narrative=f"No significant change detected ({delta_pct:.1f}%)",
                queries_executed=queries_executed,
            )

        # Step 2: Decompose variance by dimensions
        logger.info("step_2_decompose_variance")
        dimension_drivers = await self._decompose_variance(
            metric, baseline_period, current_period, delta
        )
        queries_executed += len(
            self.DECOMPOSITION_DIMENSIONS.get(metric.canonical, [])
        )

        # Step 3: Check event tables for correlations
        logger.info("step_3_check_events")
        event_drivers = await self._check_events(
            metric, baseline_period, current_period, delta
        )
        queries_executed += len(self.EVENT_TABLES)

        # Step 4: Rank all drivers
        logger.info("step_4_rank_drivers")
        all_drivers = dimension_drivers + event_drivers
        ranked_drivers = self._rank_drivers(all_drivers, delta)

        # Step 5: Calculate confidence
        total_explained = sum(abs(d.impact) for d in ranked_drivers)
        explained_pct = (
            (total_explained / abs(delta) * 100) if delta != 0 else 0
        )
        confidence = min(explained_pct / 100.0, 1.0)

        logger.info(
            "diagnostics_complete",
            drivers_found=len(ranked_drivers),
            total_explained=total_explained,
            explained_pct=explained_pct,
            confidence=confidence,
        )

        # Generate narrative
        narrative = self._generate_narrative(
            metric.canonical,
            delta,
            delta_pct,
            ranked_drivers,
            baseline_period,
            current_period,
        )

        # Determine status
        if confidence >= self.CONFIDENCE_THRESHOLD:
            status = DiagnosticStatus.DIAGNOSED
        elif confidence >= 0.5:
            status = DiagnosticStatus.PARTIAL
        else:
            status = DiagnosticStatus.UNCLEAR

        return DiagnosticResult(
            status=status,
            confidence=confidence,
            baseline_value=baseline_value,
            current_value=current_value,
            delta=delta,
            delta_pct=delta_pct,
            drivers=ranked_drivers,
            narrative=narrative,
            queries_executed=queries_executed,
            metadata={
                "total_explained": total_explained,
                "explained_pct": explained_pct,
            },
        )

    async def _compare_periods(
        self, metric, baseline_period: str, current_period: str
    ) -> tuple[float, float]:
        """
        Compare metric values between two periods.

        Args:
            metric: Metric definition
            baseline_period: Baseline period
            current_period: Current period

        Returns:
            Tuple of (baseline_value, current_value)
        """
        # In production, these would be actual SQL queries
        # For now, return mock data

        # Mock baseline query:
        # SELECT {metric.definition} FROM {metric.table}
        # WHERE period = {baseline_period}

        # Mock current query:
        # SELECT {metric.definition} FROM {metric.table}
        # WHERE period = {current_period}

        logger.debug(
            "comparing_periods",
            metric=metric.canonical,
            baseline=baseline_period,
            current=current_period,
        )

        # Mock values
        baseline_value = 1_000_000.0
        current_value = 880_000.0

        return baseline_value, current_value

    async def _decompose_variance(
        self, metric, baseline_period: str, current_period: str, total_delta: float
    ) -> list[Driver]:
        """
        Decompose variance by dimensions.

        Args:
            metric: Metric definition
            baseline_period: Baseline period
            current_period: Current period
            total_delta: Total variance to explain

        Returns:
            List of Driver objects for each dimension
        """
        drivers = []
        dimensions = self.DECOMPOSITION_DIMENSIONS.get(metric.canonical, [])

        for dimension in dimensions:
            # In production, execute SQL:
            # SELECT {dimension}, {metric.definition}
            # FROM {metric.table}
            # WHERE period IN ({baseline_period}, {current_period})
            # GROUP BY {dimension}, period

            logger.debug(
                "decomposing_by_dimension",
                metric=metric.canonical,
                dimension=dimension,
            )

            # Mock dimension analysis
            # Example: Electronics dropped -$80K, Home -$30K, etc.
            if dimension == "product_category":
                drivers.append(
                    Driver(
                        factor=f"{dimension}_change",
                        dimension="product_category",
                        impact=-80_000,
                        evidence_sql=f"-- Mock SQL for {dimension}",
                        evidence_data=[
                            {"category": "Electronics", "delta": -80_000},
                            {"category": "Home", "delta": -30_000},
                            {"category": "Apparel", "delta": -10_000},
                        ],
                        metadata={"dimension_values_analyzed": 3},
                    )
                )

        return drivers

    async def _check_events(
        self, metric, baseline_period: str, current_period: str, total_delta: float
    ) -> list[Driver]:
        """
        Check event tables for correlated events.

        Args:
            metric: Metric definition
            baseline_period: Baseline period
            current_period: Current period
            total_delta: Total variance to explain

        Returns:
            List of Driver objects for events
        """
        drivers = []

        for event_type, event_table in self.EVENT_TABLES.items():
            # In production, query event table:
            # SELECT * FROM {event_table}
            # WHERE period IN ({baseline_period}, {current_period})

            logger.debug(
                "checking_events",
                event_type=event_type,
                table=event_table,
            )

            # Mock event analysis
            if event_type == "promotions":
                drivers.append(
                    Driver(
                        factor="promo_reduction",
                        impact=-70_000,
                        evidence_sql=f"SELECT * FROM {event_table}",
                        evidence_data=[
                            {"period": baseline_period, "promo_days": 9},
                            {"period": current_period, "promo_days": 2},
                        ],
                        metadata={
                            "baseline_promo_days": 9,
                            "current_promo_days": 2,
                        },
                    )
                )
            elif event_type == "stockouts":
                drivers.append(
                    Driver(
                        factor="stockouts",
                        impact=-50_000,
                        evidence_sql=f"SELECT * FROM {event_table}",
                        evidence_data=[
                            {"warehouse": "DC1", "stockout_days": 3},
                            {"warehouse": "DC2", "stockout_days": 5},
                            {"warehouse": "DC3", "stockout_days": 2},
                            {"warehouse": "DC4", "stockout_days": 4},
                        ],
                        metadata={"warehouses_affected": 4},
                    )
                )

        return drivers

    def _rank_drivers(self, drivers: list[Driver], total_delta: float) -> list[Driver]:
        """
        Rank drivers by impact and calculate percentages.

        Args:
            drivers: List of Driver objects
            total_delta: Total variance

        Returns:
            Sorted list of drivers with impact percentages
        """
        # Calculate impact percentages
        for driver in drivers:
            driver.impact_pct = (
                (abs(driver.impact) / abs(total_delta) * 100)
                if total_delta != 0
                else 0
            )

        # Sort by absolute impact (descending)
        ranked = sorted(drivers, key=lambda d: abs(d.impact), reverse=True)

        return ranked

    def _generate_narrative(
        self,
        metric: str,
        delta: float,
        delta_pct: float,
        drivers: list[Driver],
        baseline_period: str,
        current_period: str,
    ) -> str:
        """
        Generate natural language narrative of diagnostic findings.

        Args:
            metric: Metric name
            delta: Absolute change
            delta_pct: Percentage change
            drivers: Ranked list of drivers
            baseline_period: Baseline period
            current_period: Current period

        Returns:
            Natural language narrative
        """
        direction = "dropped" if delta < 0 else "increased"
        abs_delta = abs(delta)

        narrative_parts = [
            f"{metric.capitalize()} {direction} ${abs_delta:,.0f} "
            f"({abs(delta_pct):.1f}%) from {baseline_period} to {current_period}"
        ]

        if drivers:
            narrative_parts.append(" due to:")

            for i, driver in enumerate(drivers[:3], 1):  # Top 3 drivers
                factor_desc = driver.factor.replace("_", " ")
                narrative_parts.append(
                    f"\n{i}. {factor_desc.capitalize()}: "
                    f"${abs(driver.impact):,.0f} ({driver.impact_pct:.1f}%)"
                )

                # Add context from evidence
                if driver.metadata:
                    if "baseline_promo_days" in driver.metadata:
                        narrative_parts.append(
                            f" (promo days: {driver.metadata['baseline_promo_days']} → "
                            f"{driver.metadata['current_promo_days']})"
                        )
                    elif "warehouses_affected" in driver.metadata:
                        narrative_parts.append(
                            f" ({driver.metadata['warehouses_affected']} warehouses affected)"
                        )

        else:
            narrative_parts.append(". Unable to identify clear drivers.")

        return "".join(narrative_parts)
