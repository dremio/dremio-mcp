"""
Planner & Grounder Component - Fuzzy Matching and Semantic Layer Integration.

This module maps user terms to canonical terms using fuzzy matching and
validates queries against the Dremio semantic layer.
"""

from dataclasses import dataclass, field
from typing import Any, Optional

import structlog
from rapidfuzz import fuzz, process

from dremioai.analytics.resolver import ResolvedQuery
from dremioai.api.transport import DremioAsyncHttpClient

logger = structlog.get_logger(__name__)


@dataclass
class MetricDefinition:
    """Canonical metric definition."""

    canonical: str
    definition: str
    table: str
    user_term: str
    match_score: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DimensionDefinition:
    """Canonical dimension definition."""

    canonical: str
    table: str
    column: str
    user_term: str
    match_score: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class JoinPath:
    """Join path between tables."""

    from_table: str
    to_table: str
    condition: str
    join_type: str = "INNER"


@dataclass
class GroundedPlan:
    """Grounded query plan with canonical terms and validation."""

    metrics: list[MetricDefinition]
    dimensions: list[DimensionDefinition]
    join_paths: list[JoinPath]
    filters: list[dict[str, Any]] = field(default_factory=list)
    policy_checks: dict[str, bool] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


class Planner:
    """
    Planner & Grounder component for fuzzy matching and semantic validation.

    This component:
    1. Fuzzy matches user terms to canonical terms
    2. Queries Dremio semantic layer for definitions
    3. Validates join paths
    4. Checks access policies
    """

    # Value dictionary: User terms → Canonical terms
    # In production, this would be loaded from configuration or semantic layer
    VALUE_DICTIONARY = {
        # Metrics
        "revenue": ["revenue", "sales", "total_sales", "turnover", "income"],
        "profit": ["profit", "net_profit", "earnings"],
        "cost": ["cost", "expense", "expenditure", "spending"],
        "orders": ["orders", "transactions", "purchases"],
        "customers": ["customers", "users", "accounts", "clients"],
        # Dimensions
        "product_category": [
            "category",
            "product_category",
            "product_type",
            "product_group",
        ],
        "region": ["region", "area", "territory", "zone"],
        "country": ["country", "nation"],
        "channel": ["channel", "sales_channel", "distribution_channel"],
        "segment": ["segment", "customer_segment", "market_segment"],
    }

    # Join graph: Defines allowed domain combinations and join conditions
    JOIN_GRAPH = {
        ("sales", "commerce"): {
            "from_table": "sales.orders",
            "to_table": "commerce.products",
            "condition": "sales.orders.product_id = commerce.products.product_id",
            "join_type": "INNER",
        },
        ("sales", "customer"): {
            "from_table": "sales.orders",
            "to_table": "customer.customers",
            "condition": "sales.orders.customer_id = customer.customers.customer_id",
            "join_type": "INNER",
        },
        ("commerce", "sales"): {
            "from_table": "commerce.products",
            "to_table": "sales.orders",
            "condition": "commerce.products.product_id = sales.orders.product_id",
            "join_type": "INNER",
        },
    }

    # Metric definitions (would come from Dremio semantic layer in production)
    METRIC_DEFINITIONS = {
        "revenue": {
            "definition": "SUM(order_amount)",
            "table": "sales.orders",
            "column": "order_amount",
            "aggregation": "SUM",
        },
        "profit": {
            "definition": "SUM(order_amount - order_cost)",
            "table": "sales.orders",
            "calculation": "order_amount - order_cost",
            "aggregation": "SUM",
        },
        "orders": {
            "definition": "COUNT(DISTINCT order_id)",
            "table": "sales.orders",
            "column": "order_id",
            "aggregation": "COUNT",
        },
        "customers": {
            "definition": "COUNT(DISTINCT customer_id)",
            "table": "sales.orders",
            "column": "customer_id",
            "aggregation": "COUNT",
        },
    }

    # Dimension definitions (would come from Dremio catalog in production)
    DIMENSION_DEFINITIONS = {
        "product_category": {
            "table": "commerce.products",
            "column": "category",
            "type": "string",
        },
        "region": {"table": "sales.orders", "column": "region", "type": "string"},
        "country": {"table": "sales.orders", "column": "country", "type": "string"},
        "channel": {
            "table": "sales.orders",
            "column": "sales_channel",
            "type": "string",
        },
    }

    def __init__(
        self,
        dremio_client: Optional[DremioAsyncHttpClient] = None,
        fuzzy_threshold: float = 0.8,
    ):
        """
        Initialize the Planner.

        Args:
            dremio_client: Optional Dremio client for semantic layer queries
            fuzzy_threshold: Minimum similarity score for fuzzy matching (0-1)
        """
        self.dremio_client = dremio_client
        self.fuzzy_threshold = fuzzy_threshold

    async def ground(self, resolved: ResolvedQuery) -> GroundedPlan:
        """
        Ground a resolved query to canonical terms and validate.

        Args:
            resolved: ResolvedQuery from Resolver component

        Returns:
            GroundedPlan with canonical terms and validation results
        """
        logger.info("grounding_query", intent=resolved.query_type.value)

        # Fuzzy match metrics
        grounded_metrics = []
        for user_term in resolved.entities.metrics:
            metric_def = self._fuzzy_match_metric(user_term)
            if metric_def:
                grounded_metrics.append(metric_def)
                logger.info(
                    "metric_grounded",
                    user_term=user_term,
                    canonical=metric_def.canonical,
                    score=metric_def.match_score,
                )

        # Fuzzy match dimensions
        grounded_dimensions = []
        for user_term in resolved.entities.dimensions:
            dim_def = self._fuzzy_match_dimension(user_term)
            if dim_def:
                grounded_dimensions.append(dim_def)
                logger.info(
                    "dimension_grounded",
                    user_term=user_term,
                    canonical=dim_def.canonical,
                    score=dim_def.match_score,
                )

        # Validate join paths
        join_paths = self._validate_join_paths(resolved.domains)

        # Check policies
        policy_checks = await self._check_policies(
            resolved.domains, grounded_metrics, grounded_dimensions
        )

        plan = GroundedPlan(
            metrics=grounded_metrics,
            dimensions=grounded_dimensions,
            join_paths=join_paths,
            policy_checks=policy_checks,
            metadata={
                "query_type": resolved.query_type.value,
                "domains": resolved.domains,
                "fuzzy_threshold": self.fuzzy_threshold,
            },
        )

        logger.info(
            "query_grounded",
            metrics_count=len(grounded_metrics),
            dimensions_count=len(grounded_dimensions),
            joins_count=len(join_paths),
            policy_passed=all(policy_checks.values()),
        )

        return plan

    def _fuzzy_match_metric(self, user_term: str) -> Optional[MetricDefinition]:
        """
        Fuzzy match user term to canonical metric.

        Args:
            user_term: User's metric term

        Returns:
            MetricDefinition if match found, None otherwise
        """
        best_match = None
        best_score = 0.0
        best_canonical = None

        for canonical, synonyms in self.VALUE_DICTIONARY.items():
            if canonical not in self.METRIC_DEFINITIONS:
                continue

            # Check exact match first
            if user_term.lower() in [s.lower() for s in synonyms]:
                best_match = canonical
                best_score = 1.0
                break

            # Fuzzy match against all synonyms
            for synonym in synonyms:
                score = fuzz.ratio(user_term.lower(), synonym.lower()) / 100.0
                if score > best_score:
                    best_score = score
                    best_match = canonical

        if best_match and best_score >= self.fuzzy_threshold:
            metric_info = self.METRIC_DEFINITIONS[best_match]
            return MetricDefinition(
                canonical=best_match,
                definition=metric_info["definition"],
                table=metric_info["table"],
                user_term=user_term,
                match_score=best_score,
                metadata=metric_info,
            )

        logger.warning(
            "metric_fuzzy_match_failed",
            user_term=user_term,
            best_match=best_match,
            best_score=best_score,
            threshold=self.fuzzy_threshold,
        )
        return None

    def _fuzzy_match_dimension(self, user_term: str) -> Optional[DimensionDefinition]:
        """
        Fuzzy match user term to canonical dimension.

        Args:
            user_term: User's dimension term

        Returns:
            DimensionDefinition if match found, None otherwise
        """
        best_match = None
        best_score = 0.0

        for canonical, synonyms in self.VALUE_DICTIONARY.items():
            if canonical not in self.DIMENSION_DEFINITIONS:
                continue

            # Check exact match first
            if user_term.lower() in [s.lower() for s in synonyms]:
                best_match = canonical
                best_score = 1.0
                break

            # Fuzzy match against all synonyms
            for synonym in synonyms:
                score = fuzz.ratio(user_term.lower(), synonym.lower()) / 100.0
                if score > best_score:
                    best_score = score
                    best_match = canonical

        if best_match and best_score >= self.fuzzy_threshold:
            dim_info = self.DIMENSION_DEFINITIONS[best_match]
            return DimensionDefinition(
                canonical=best_match,
                table=dim_info["table"],
                column=dim_info["column"],
                user_term=user_term,
                match_score=best_score,
                metadata=dim_info,
            )

        logger.warning(
            "dimension_fuzzy_match_failed",
            user_term=user_term,
            best_match=best_match,
            best_score=best_score,
            threshold=self.fuzzy_threshold,
        )
        return None

    def _validate_join_paths(self, domains: list[str]) -> list[JoinPath]:
        """
        Validate and construct join paths between domains.

        Args:
            domains: List of domains involved in query

        Returns:
            List of JoinPath objects
        """
        join_paths = []

        if len(domains) == 1:
            # Single domain, no joins needed
            return join_paths

        # Check all domain pairs
        for i, domain1 in enumerate(domains):
            for domain2 in domains[i + 1 :]:
                # Check both directions
                key1 = (domain1, domain2)
                key2 = (domain2, domain1)

                if key1 in self.JOIN_GRAPH:
                    join_info = self.JOIN_GRAPH[key1]
                    join_paths.append(
                        JoinPath(
                            from_table=join_info["from_table"],
                            to_table=join_info["to_table"],
                            condition=join_info["condition"],
                            join_type=join_info["join_type"],
                        )
                    )
                elif key2 in self.JOIN_GRAPH:
                    join_info = self.JOIN_GRAPH[key2]
                    join_paths.append(
                        JoinPath(
                            from_table=join_info["from_table"],
                            to_table=join_info["to_table"],
                            condition=join_info["condition"],
                            join_type=join_info["join_type"],
                        )
                    )
                else:
                    logger.warning(
                        "join_path_not_found",
                        domain1=domain1,
                        domain2=domain2,
                    )

        return join_paths

    async def _check_policies(
        self,
        domains: list[str],
        metrics: list[MetricDefinition],
        dimensions: list[DimensionDefinition],
    ) -> dict[str, bool]:
        """
        Check access policies for domains, metrics, and dimensions.

        Args:
            domains: List of domains
            metrics: List of grounded metrics
            dimensions: List of grounded dimensions

        Returns:
            Dictionary of policy check results

        Note:
            In production, this would query a policy service or check
            user permissions against Dremio's RBAC system.
        """
        # Placeholder implementation - always returns True
        # In production, implement actual policy checks
        return {
            "domain_access": True,
            "metric_access": True,
            "dimension_access": True,
            "row_level_security": True,
        }

    async def query_semantic_layer(
        self, metric: str
    ) -> Optional[dict[str, Any]]:
        """
        Query Dremio semantic layer for metric definition.

        Args:
            metric: Canonical metric name

        Returns:
            Metric definition from semantic layer

        Note:
            This would use Dremio's semantic layer API in production.
        """
        if not self.dremio_client:
            logger.warning("dremio_client_not_configured")
            return None

        # TODO: Implement actual semantic layer API call
        # This would involve:
        # 1. GET /api/v3/semantic/metrics/{metric}
        # 2. Parse response
        # 3. Return structured definition

        logger.info("querying_semantic_layer", metric=metric)
        return self.METRIC_DEFINITIONS.get(metric)
