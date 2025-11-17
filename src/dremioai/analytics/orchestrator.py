"""
Analytics Orchestrator - Main Pipeline Coordinator.

This module orchestrates the complete analytics pipeline from user query
through all components to final formatted response.
"""

import uuid
from typing import Any, Optional

import structlog

from dremioai.analytics.resolver import Resolver, QueryIntent
from dremioai.analytics.planner import Planner
from dremioai.analytics.compiler import Compiler
from dremioai.analytics.safety import SafetyGate
from dremioai.analytics.diagnostics import DiagnosticsAgent
from dremioai.analytics.results import ResultsProcessor, ResponseFormat
from dremioai.api.transport import DremioAsyncHttpClient

logger = structlog.get_logger(__name__)


class AnalyticsOrchestrator:
    """
    Main orchestrator for the analytics pipeline.

    Flow:
    1. Resolver: Intent classification
    2. Planner: Fuzzy matching and grounding
    3. [If "why"] Diagnostics Agent
    4. Compiler: SQL generation and validation
    5. Safety Gate: Pre-execution checks
    6. Execute: Run query on Dremio
    7. Results Processor: Format response
    """

    def __init__(
        self,
        dremio_client: DremioAsyncHttpClient,
        use_llm: bool = False,
        llm_provider: Optional[str] = None,
        fuzzy_threshold: float = 0.8,
        max_rows: int = 1_000_000,
        max_cost_dcu: float = 100.0,
        schema_allowlist: Optional[list[str]] = None,
    ):
        """
        Initialize the Analytics Orchestrator.

        Args:
            dremio_client: Dremio API client
            use_llm: Whether to use LLM for SQL generation
            llm_provider: LLM provider (anthropic, openai, bedrock)
            fuzzy_threshold: Fuzzy matching threshold
            max_rows: Maximum allowed rows per query
            max_cost_dcu: Maximum allowed cost per query
            schema_allowlist: List of allowed schemas
        """
        self.dremio_client = dremio_client

        # Initialize components
        self.resolver = Resolver(use_llm=use_llm, llm_provider=llm_provider)
        self.planner = Planner(
            dremio_client=dremio_client, fuzzy_threshold=fuzzy_threshold
        )
        self.compiler = Compiler(
            schema_allowlist=schema_allowlist,
            use_llm=use_llm,
            llm_provider=llm_provider,
        )
        self.safety_gate = SafetyGate(
            dremio_client=dremio_client,
            max_rows=max_rows,
            max_cost_dcu=max_cost_dcu,
        )
        self.diagnostics = DiagnosticsAgent(dremio_client=dremio_client)
        self.results_processor = ResultsProcessor()

    async def process_query(
        self,
        query: str,
        user_id: Optional[str] = None,
        response_format: ResponseFormat = ResponseFormat.MCP_STANDARD,
    ) -> dict[str, Any]:
        """
        Process a natural language query through the complete pipeline.

        Args:
            query: Natural language query from user
            user_id: Optional user ID for quota tracking
            response_format: Desired response format

        Returns:
            Formatted response dictionary

        Raises:
            ValueError: If query validation fails
            RuntimeError: If query execution fails
        """
        # Generate trace ID for observability
        trace_id = str(uuid.uuid4())

        logger.info(
            "analytics_query_start",
            query=query,
            user_id=user_id,
            trace_id=trace_id,
        )

        try:
            # Step 1: Resolve intent
            logger.info("step_1_resolve", trace_id=trace_id)
            resolved = await self.resolver.resolve(query)

            logger.info(
                "query_resolved",
                intent=resolved.query_type.value,
                confidence=resolved.confidence,
                trace_id=trace_id,
            )

            # Step 2: Ground to canonical terms
            logger.info("step_2_ground", trace_id=trace_id)
            grounded = await self.planner.ground(resolved)

            if not grounded.metrics and not grounded.dimensions:
                raise ValueError(
                    "Could not identify any metrics or dimensions in query. "
                    "Please rephrase with clearer metric names."
                )

            logger.info(
                "query_grounded",
                metrics=len(grounded.metrics),
                dimensions=len(grounded.dimensions),
                trace_id=trace_id,
            )

            # Step 3: Route based on intent
            if resolved.query_type == QueryIntent.WHY:
                # Diagnostics flow for "why" queries
                return await self._process_diagnostic_query(
                    resolved, grounded, user_id, response_format, trace_id
                )
            else:
                # Standard flow for "what" and "compare" queries
                return await self._process_standard_query(
                    resolved, grounded, user_id, response_format, trace_id
                )

        except Exception as e:
            logger.error(
                "analytics_query_failed",
                error=str(e),
                query=query,
                trace_id=trace_id,
            )
            raise

    async def _process_standard_query(
        self, resolved, grounded, user_id, response_format, trace_id
    ) -> dict[str, Any]:
        """
        Process standard "what" or "compare" query.

        Args:
            resolved: ResolvedQuery
            grounded: GroundedPlan
            user_id: User ID
            response_format: Response format
            trace_id: Trace ID

        Returns:
            Formatted response
        """
        # Step 4: Compile to SQL
        logger.info("step_4_compile", trace_id=trace_id)
        compiled = await self.compiler.compile(grounded)

        if not compiled.validated:
            errors = ", ".join(compiled.ast_checks.errors)
            raise ValueError(f"SQL validation failed: {errors}")

        logger.info("query_compiled", sql_length=len(compiled.sql), trace_id=trace_id)

        # Step 5: Safety checks
        logger.info("step_5_safety_check", trace_id=trace_id)
        safety_check = await self.safety_gate.check(compiled, user_id=user_id)

        if not safety_check.approved:
            violations = "; ".join(safety_check.violations)
            raise RuntimeError(f"Query rejected by safety gate: {violations}")

        logger.info(
            "safety_check_passed",
            estimated_rows=safety_check.estimated_rows,
            estimated_cost=safety_check.estimated_cost_dcu,
            trace_id=trace_id,
        )

        # Step 6: Execute query
        logger.info("step_6_execute", trace_id=trace_id)
        # TODO: Integrate with actual Dremio execution
        # For now, return mock data
        data, job_id, runtime_ms = await self._execute_query(compiled.sql)

        logger.info(
            "query_executed",
            job_id=job_id,
            rows=len(data),
            runtime_ms=runtime_ms,
            trace_id=trace_id,
        )

        # Update quota (if tracking enabled)
        if user_id and safety_check.estimated_cost_dcu:
            await self.safety_gate.update_quota_usage(
                user_id, safety_check.estimated_cost_dcu
            )

        # Step 7: Process results
        logger.info("step_7_process_results", trace_id=trace_id)
        result = self.results_processor.process(
            data=data,
            sql=compiled.sql,
            job_id=job_id,
            runtime_ms=runtime_ms,
            cost_dcu=safety_check.estimated_cost_dcu,
            trace_id=trace_id,
            response_format=response_format,
        )

        # Format for target client
        if response_format == ResponseFormat.CHATGPT_ENTERPRISE:
            formatted = self.results_processor.format_for_chatgpt(result)
        elif response_format == ResponseFormat.AWS_BEDROCK:
            formatted = self.results_processor.format_for_bedrock(result)
        else:
            formatted = self.results_processor.format_for_mcp(result)

        logger.info(
            "analytics_query_complete",
            trace_id=trace_id,
            rows=len(data),
            runtime_ms=runtime_ms,
        )

        return formatted

    async def _process_diagnostic_query(
        self, resolved, grounded, user_id, response_format, trace_id
    ) -> dict[str, Any]:
        """
        Process "why" diagnostic query.

        Args:
            resolved: ResolvedQuery
            grounded: GroundedPlan
            user_id: User ID
            response_format: Response format
            trace_id: Trace ID

        Returns:
            Formatted response with diagnostic findings
        """
        logger.info("diagnostic_query_detected", trace_id=trace_id)

        # Extract time periods from resolved query
        current_period = resolved.entities.time_period or "last_month"
        baseline_period = resolved.entities.baseline_period or "previous_month"

        # Step 4: Run diagnostics
        logger.info("step_4_diagnostics", trace_id=trace_id)
        diagnostic_result = await self.diagnostics.diagnose(
            grounded, baseline_period, current_period
        )

        logger.info(
            "diagnostics_complete",
            status=diagnostic_result.status.value,
            confidence=diagnostic_result.confidence,
            drivers=len(diagnostic_result.drivers),
            trace_id=trace_id,
        )

        # Format diagnostic data for visualization
        # Create waterfall chart data for drivers
        waterfall_data = []

        waterfall_data.append(
            {
                "factor": "Baseline",
                "value": diagnostic_result.baseline_value,
                "type": "baseline",
            }
        )

        for driver in diagnostic_result.drivers:
            waterfall_data.append(
                {
                    "factor": driver.factor.replace("_", " ").title(),
                    "value": driver.impact,
                    "type": "driver",
                }
            )

        waterfall_data.append(
            {
                "factor": "Current",
                "value": diagnostic_result.current_value,
                "type": "current",
            }
        )

        # Step 7: Process results (diagnostics format)
        result = self.results_processor.process(
            data=waterfall_data,
            sql="-- Diagnostic queries executed",
            runtime_ms=None,
            cost_dcu=None,
            trace_id=trace_id,
            narrative=diagnostic_result.narrative,
            response_format=response_format,
        )

        # Override visualization to waterfall for diagnostics
        from dremioai.analytics.results import VisualizationConfig, ChartType

        result.visualization = VisualizationConfig(
            type=ChartType.WATERFALL,
            x="factor",
            y="value",
            title=f"Variance Analysis - {grounded.metrics[0].canonical if grounded.metrics else 'Metric'}",
            x_label="Factor",
            y_label="Impact",
        )

        # Format for target client
        if response_format == ResponseFormat.CHATGPT_ENTERPRISE:
            formatted = self.results_processor.format_for_chatgpt(result)
        elif response_format == ResponseFormat.AWS_BEDROCK:
            formatted = self.results_processor.format_for_bedrock(result)
        else:
            formatted = self.results_processor.format_for_mcp(result)

        # Add diagnostic details
        formatted["diagnostic_details"] = {
            "status": diagnostic_result.status.value,
            "confidence": diagnostic_result.confidence,
            "baseline_value": diagnostic_result.baseline_value,
            "current_value": diagnostic_result.current_value,
            "delta": diagnostic_result.delta,
            "delta_pct": diagnostic_result.delta_pct,
            "queries_executed": diagnostic_result.queries_executed,
            "drivers": [
                {
                    "factor": d.factor,
                    "impact": d.impact,
                    "impact_pct": d.impact_pct,
                    "dimension": d.dimension,
                }
                for d in diagnostic_result.drivers
            ],
        }

        logger.info(
            "diagnostic_query_complete",
            trace_id=trace_id,
            status=diagnostic_result.status.value,
        )

        return formatted

    async def _execute_query(
        self, sql: str
    ) -> tuple[list[dict[str, Any]], str, int]:
        """
        Execute SQL query via Dremio.

        Args:
            sql: SQL query to execute

        Returns:
            Tuple of (data rows, job_id, runtime_ms)

        Note:
            This is a simplified implementation. In production, use
            the existing DremioAsyncHttpClient.execute_sql method.
        """
        # TODO: Integrate with actual Dremio execution
        # In production:
        # df = await self.dremio_client.execute_sql(sql)
        # data = df.to_dict('records')
        # job_id = response.job_id
        # runtime_ms = response.runtime_ms

        # Mock data for now
        mock_data = [
            {"product_category": "Electronics", "revenue": 1247893.21},
            {"product_category": "Home", "revenue": 892341.12},
            {"product_category": "Apparel", "revenue": 654123.45},
            {"product_category": "Sports", "revenue": 543210.98},
            {"product_category": "Books", "revenue": 321098.76},
        ]

        job_id = f"job_{uuid.uuid4().hex[:8]}"
        runtime_ms = 847

        return mock_data, job_id, runtime_ms
