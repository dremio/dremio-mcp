"""
Resolver Component - Intent Classification and Entity Extraction.

This module classifies user queries into intent types (what/why/compare) and
extracts entities such as metrics, dimensions, filters, and time periods.
"""

import re
from enum import Enum
from typing import Any, Optional
from dataclasses import dataclass, field

import structlog

logger = structlog.get_logger(__name__)


class QueryIntent(str, Enum):
    """Query intent types."""

    WHAT = "what"  # Descriptive: "Show me X by Y"
    WHY = "why"  # Diagnostic: "Why did X happen?"
    COMPARE = "compare"  # Comparison: "Compare X to Y"
    HOW = "how"  # Procedural: "How to X?"
    UNKNOWN = "unknown"


@dataclass
class ExtractedEntities:
    """Entities extracted from user query."""

    metrics: list[str] = field(default_factory=list)
    dimensions: list[str] = field(default_factory=list)
    filters: list[dict[str, Any]] = field(default_factory=list)
    time_period: Optional[str] = None
    baseline_period: Optional[str] = None
    aggregations: list[str] = field(default_factory=list)


@dataclass
class ResolvedQuery:
    """Resolved query with intent and extracted entities."""

    query_type: QueryIntent
    entities: ExtractedEntities
    domains: list[str]
    raw_query: str
    confidence: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


class Resolver:
    """
    Resolver component for intent classification and entity extraction.

    Uses pattern matching and optional LLM-based classification for complex queries.
    """

    # Pattern definitions for intent classification
    WHAT_PATTERNS = [
        r"^(show|display|list|get|give|find)\s+(me\s+)?",
        r"^what\s+(is|are|were|was)\s+",
        r"^(how\s+much|how\s+many)",
    ]

    WHY_PATTERNS = [
        r"^why\s+(did|is|are|was|were|has|have|do|does)",
        r"^what\s+caused",
        r"^what\s+is\s+the\s+reason",
        r"^explain\s+why",
        r"(dropped|increased|decreased|changed)\s+(last|this|in)",
    ]

    COMPARE_PATTERNS = [
        r"^compare",
        r"^(difference|delta)\s+between",
        r"vs\.?\s+",
        r"versus\s+",
        r"compared\s+to",
    ]

    # Common metric terms
    METRIC_TERMS = {
        "revenue",
        "sales",
        "profit",
        "cost",
        "margin",
        "orders",
        "customers",
        "users",
        "count",
        "sum",
        "average",
        "mean",
        "total",
        "amount",
    }

    # Common dimension terms
    DIMENSION_TERMS = {
        "category",
        "product",
        "region",
        "country",
        "state",
        "city",
        "channel",
        "segment",
        "type",
        "status",
        "date",
        "month",
        "year",
        "quarter",
        "week",
    }

    # Time period patterns
    TIME_PATTERNS = {
        "last_month": r"last\s+month",
        "this_month": r"this\s+month",
        "last_year": r"last\s+year",
        "this_year": r"this\s+year",
        "last_quarter": r"last\s+quarter",
        "this_quarter": r"this\s+quarter",
        "yesterday": r"yesterday",
        "today": r"today",
        "last_week": r"last\s+week",
        "this_week": r"this\s+week",
    }

    # Domain mapping (can be configured)
    DOMAIN_KEYWORDS = {
        "sales": ["revenue", "sales", "orders", "transactions"],
        "commerce": ["product", "category", "catalog", "sku"],
        "customer": ["customer", "user", "account", "subscription"],
        "finance": ["cost", "profit", "margin", "expense", "budget"],
        "marketing": ["campaign", "promotion", "channel", "conversion"],
        "inventory": ["stock", "inventory", "warehouse", "supply"],
    }

    def __init__(self, use_llm: bool = False, llm_provider: Optional[str] = None):
        """
        Initialize the Resolver.

        Args:
            use_llm: Whether to use LLM for complex query classification
            llm_provider: LLM provider to use (anthropic, openai, bedrock)
        """
        self.use_llm = use_llm
        self.llm_provider = llm_provider

    async def resolve(self, query: str) -> ResolvedQuery:
        """
        Resolve a user query into intent and entities.

        Args:
            query: Natural language query from user

        Returns:
            ResolvedQuery with classified intent and extracted entities
        """
        logger.info("resolving_query", query=query)

        # Normalize query
        normalized_query = query.lower().strip()

        # Classify intent
        intent, confidence = self._classify_intent(normalized_query)

        # Extract entities
        entities = self._extract_entities(normalized_query)

        # Identify domains
        domains = self._identify_domains(normalized_query, entities)

        resolved = ResolvedQuery(
            query_type=intent,
            entities=entities,
            domains=domains,
            raw_query=query,
            confidence=confidence,
            metadata={
                "normalized_query": normalized_query,
                "pattern_matched": confidence > 0.8,
            },
        )

        logger.info(
            "query_resolved",
            intent=intent.value,
            confidence=confidence,
            domains=domains,
            metrics=entities.metrics,
            dimensions=entities.dimensions,
        )

        return resolved

    def _classify_intent(self, query: str) -> tuple[QueryIntent, float]:
        """
        Classify query intent using pattern matching.

        Args:
            query: Normalized query string

        Returns:
            Tuple of (QueryIntent, confidence score)
        """
        # Check WHY patterns (highest priority for diagnostics)
        for pattern in self.WHY_PATTERNS:
            if re.search(pattern, query, re.IGNORECASE):
                return QueryIntent.WHY, 0.95

        # Check COMPARE patterns
        for pattern in self.COMPARE_PATTERNS:
            if re.search(pattern, query, re.IGNORECASE):
                return QueryIntent.COMPARE, 0.90

        # Check WHAT patterns
        for pattern in self.WHAT_PATTERNS:
            if re.search(pattern, query, re.IGNORECASE):
                return QueryIntent.WHAT, 0.85

        # Default to WHAT with lower confidence
        return QueryIntent.WHAT, 0.5

    def _extract_entities(self, query: str) -> ExtractedEntities:
        """
        Extract entities from query using pattern matching and keyword detection.

        Args:
            query: Normalized query string

        Returns:
            ExtractedEntities with metrics, dimensions, filters, and time periods
        """
        entities = ExtractedEntities()

        # Extract metrics
        for metric_term in self.METRIC_TERMS:
            if metric_term in query:
                entities.metrics.append(metric_term)

        # Extract dimensions
        for dim_term in self.DIMENSION_TERMS:
            if dim_term in query:
                entities.dimensions.append(dim_term)

        # Extract time periods
        for period_name, pattern in self.TIME_PATTERNS.items():
            if re.search(pattern, query, re.IGNORECASE):
                entities.time_period = period_name

                # For "why" queries, infer baseline period
                if "last_month" in period_name:
                    entities.baseline_period = "previous_month"
                elif "last_year" in period_name:
                    entities.baseline_period = "previous_year"
                elif "last_quarter" in period_name:
                    entities.baseline_period = "previous_quarter"

        # Extract aggregation functions
        agg_patterns = {
            "sum": r"\b(sum|total)\b",
            "average": r"\b(average|avg|mean)\b",
            "count": r"\b(count|number of)\b",
            "max": r"\b(max|maximum|highest)\b",
            "min": r"\b(min|minimum|lowest)\b",
        }

        for agg_name, pattern in agg_patterns.items():
            if re.search(pattern, query, re.IGNORECASE):
                entities.aggregations.append(agg_name)

        # If metrics found but no aggregations, default to sum
        if entities.metrics and not entities.aggregations:
            entities.aggregations.append("sum")

        return entities

    def _identify_domains(
        self, query: str, entities: ExtractedEntities
    ) -> list[str]:
        """
        Identify relevant data domains based on query and entities.

        Args:
            query: Normalized query string
            entities: Extracted entities

        Returns:
            List of domain names
        """
        domains = set()

        # Check domain keywords
        for domain, keywords in self.DOMAIN_KEYWORDS.items():
            for keyword in keywords:
                if keyword in query:
                    domains.add(domain)

        # Check metrics against domains
        for metric in entities.metrics:
            for domain, keywords in self.DOMAIN_KEYWORDS.items():
                if metric in keywords:
                    domains.add(domain)

        # Check dimensions against domains
        for dimension in entities.dimensions:
            for domain, keywords in self.DOMAIN_KEYWORDS.items():
                if dimension in keywords:
                    domains.add(domain)

        # Default to "sales" if no domain identified (fallback)
        if not domains:
            domains.add("sales")

        return sorted(list(domains))

    async def resolve_with_llm(self, query: str) -> ResolvedQuery:
        """
        Resolve query using LLM for complex queries.

        This method uses an LLM to classify intent and extract entities
        for queries that don't match simple patterns.

        Args:
            query: Natural language query

        Returns:
            ResolvedQuery with LLM-classified intent and entities

        Note:
            This is a placeholder for LLM integration.
            Implement based on your LLM provider (Claude, GPT-4, Bedrock).
        """
        # TODO: Implement LLM-based resolution
        # This would involve:
        # 1. Creating a prompt with query and examples
        # 2. Calling LLM API
        # 3. Parsing structured output
        # 4. Returning ResolvedQuery

        logger.warning("llm_resolution_not_implemented", query=query)
        return await self.resolve(query)
