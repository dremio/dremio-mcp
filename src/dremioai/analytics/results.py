"""
Results Processor - Response Formatting and Visualization Selection.

This module formats query results for different client types (ChatGPT, Bedrock, MCP)
and automatically selects appropriate visualizations based on data shape.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

import structlog

logger = structlog.get_logger(__name__)


class ChartType(str, Enum):
    """Supported visualization types."""

    BAR_CHART = "bar_chart"
    LINE_CHART = "line_chart"
    PIE_CHART = "pie_chart"
    SCATTER_PLOT = "scatter_plot"
    TABLE = "table"
    WATERFALL = "waterfall"
    HEATMAP = "heatmap"
    MULTI_SERIES = "multi_series"


class ResponseFormat(str, Enum):
    """Client response format types."""

    MCP_STANDARD = "mcp_standard"
    CHATGPT_ENTERPRISE = "chatgpt_enterprise"
    AWS_BEDROCK = "aws_bedrock"


@dataclass
class VisualizationConfig:
    """Visualization configuration."""

    type: ChartType
    x: Optional[str] = None
    y: Optional[str | list[str]] = None
    title: str = ""
    x_label: str = ""
    y_label: str = ""
    color_by: Optional[str] = None
    sort_by: Optional[str] = None
    limit: Optional[int] = None
    config: dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryMetadata:
    """Query execution metadata."""

    sql: str
    job_id: Optional[str] = None
    runtime_ms: Optional[int] = None
    cost_dcu: Optional[float] = None
    rows_returned: int = 0
    bytes_scanned: Optional[int] = None
    trace_id: Optional[str] = None
    reflection_used: Optional[str] = None


@dataclass
class FormattedResult:
    """Formatted query result with visualization."""

    data: list[dict[str, Any]]
    visualization: Optional[VisualizationConfig] = None
    metadata: Optional[QueryMetadata] = None
    narrative: Optional[str] = None
    response_format: ResponseFormat = ResponseFormat.MCP_STANDARD


class ResultsProcessor:
    """
    Results Processor for formatting and visualization selection.

    This component:
    1. Formats query results for different clients
    2. Selects appropriate visualizations based on data shape
    3. Assembles metadata for tracing and auditing
    """

    # Cardinality threshold for table vs chart
    MAX_CARDINALITY_FOR_CHART = 50

    # Row limit for visualization
    MAX_ROWS_FOR_VISUALIZATION = 1000

    def __init__(self, default_format: ResponseFormat = ResponseFormat.MCP_STANDARD):
        """
        Initialize the Results Processor.

        Args:
            default_format: Default response format
        """
        self.default_format = default_format

    def process(
        self,
        data: list[dict[str, Any]],
        sql: str,
        job_id: Optional[str] = None,
        runtime_ms: Optional[int] = None,
        cost_dcu: Optional[float] = None,
        trace_id: Optional[str] = None,
        narrative: Optional[str] = None,
        response_format: Optional[ResponseFormat] = None,
    ) -> FormattedResult:
        """
        Process query results into formatted response.

        Args:
            data: Query result rows
            sql: SQL query executed
            job_id: Dremio job ID
            runtime_ms: Query runtime in milliseconds
            cost_dcu: Query cost in DCU
            trace_id: Trace ID for observability
            narrative: Optional narrative description
            response_format: Target response format

        Returns:
            FormattedResult with data, visualization, and metadata
        """
        logger.info("processing_results", rows=len(data))

        format_type = response_format or self.default_format

        # Select visualization
        viz = self._select_visualization(data)

        # Assemble metadata
        metadata = QueryMetadata(
            sql=sql,
            job_id=job_id,
            runtime_ms=runtime_ms,
            cost_dcu=cost_dcu,
            rows_returned=len(data),
            trace_id=trace_id,
        )

        result = FormattedResult(
            data=data,
            visualization=viz,
            metadata=metadata,
            narrative=narrative,
            response_format=format_type,
        )

        logger.info(
            "results_processed",
            viz_type=viz.type.value if viz else None,
            rows=len(data),
            format=format_type.value,
        )

        return result

    def _select_visualization(
        self, data: list[dict[str, Any]]
    ) -> Optional[VisualizationConfig]:
        """
        Automatically select appropriate visualization based on data shape.

        Args:
            data: Query result rows

        Returns:
            VisualizationConfig or None if table is more appropriate
        """
        if not data:
            return None

        # Too many rows - use table
        if len(data) > self.MAX_ROWS_FOR_VISUALIZATION:
            logger.info(
                "using_table_too_many_rows",
                rows=len(data),
                limit=self.MAX_ROWS_FOR_VISUALIZATION,
            )
            return VisualizationConfig(type=ChartType.TABLE)

        # Analyze columns
        columns = list(data[0].keys())
        num_columns = len(columns)

        # Infer column types
        numeric_cols = []
        categorical_cols = []
        date_cols = []

        for col in columns:
            sample_values = [row.get(col) for row in data[:10] if row.get(col)]

            if not sample_values:
                continue

            # Check if numeric
            if all(isinstance(v, (int, float)) for v in sample_values):
                numeric_cols.append(col)
            # Check if date/time
            elif self._is_date_column(col, sample_values):
                date_cols.append(col)
            else:
                categorical_cols.append(col)

        logger.debug(
            "column_analysis",
            numeric=numeric_cols,
            categorical=categorical_cols,
            date=date_cols,
        )

        # Pattern 1: 1 categorical + 1 numeric = Bar chart
        if len(categorical_cols) == 1 and len(numeric_cols) == 1:
            cardinality = len(set(row[categorical_cols[0]] for row in data))

            if cardinality <= self.MAX_CARDINALITY_FOR_CHART:
                return VisualizationConfig(
                    type=ChartType.BAR_CHART,
                    x=categorical_cols[0],
                    y=numeric_cols[0],
                    title=f"{numeric_cols[0]} by {categorical_cols[0]}",
                    x_label=categorical_cols[0].replace("_", " ").title(),
                    y_label=numeric_cols[0].replace("_", " ").title(),
                    sort_by=numeric_cols[0],
                )

        # Pattern 2: 1 date + 1+ numeric = Line chart
        if len(date_cols) >= 1 and len(numeric_cols) >= 1:
            if len(numeric_cols) == 1:
                return VisualizationConfig(
                    type=ChartType.LINE_CHART,
                    x=date_cols[0],
                    y=numeric_cols[0],
                    title=f"{numeric_cols[0]} over time",
                    x_label="Date",
                    y_label=numeric_cols[0].replace("_", " ").title(),
                )
            else:
                # Multiple metrics - multi-series line chart
                return VisualizationConfig(
                    type=ChartType.MULTI_SERIES,
                    x=date_cols[0],
                    y=numeric_cols,
                    title="Metrics over time",
                    x_label="Date",
                    y_label="Value",
                )

        # Pattern 3: 1 categorical + 2+ numeric = Multi-series bar chart
        if len(categorical_cols) == 1 and len(numeric_cols) >= 2:
            return VisualizationConfig(
                type=ChartType.MULTI_SERIES,
                x=categorical_cols[0],
                y=numeric_cols,
                title=f"Metrics by {categorical_cols[0]}",
                x_label=categorical_cols[0].replace("_", " ").title(),
                y_label="Value",
            )

        # Pattern 4: 1 categorical + 1 numeric (low cardinality) = Pie chart
        if len(categorical_cols) == 1 and len(numeric_cols) == 1:
            cardinality = len(set(row[categorical_cols[0]] for row in data))

            if cardinality <= 10:  # Pie chart for small categories
                return VisualizationConfig(
                    type=ChartType.PIE_CHART,
                    x=categorical_cols[0],
                    y=numeric_cols[0],
                    title=f"{numeric_cols[0]} distribution",
                )

        # Pattern 5: 2+ categorical + 1 numeric = Heatmap
        if len(categorical_cols) >= 2 and len(numeric_cols) == 1:
            return VisualizationConfig(
                type=ChartType.HEATMAP,
                x=categorical_cols[0],
                y=categorical_cols[1],
                config={
                    "value_column": numeric_cols[0],
                },
                title=f"{numeric_cols[0]} by {categorical_cols[0]} and {categorical_cols[1]}",
            )

        # Default: Table for complex data
        logger.info("using_table_default", num_columns=num_columns)
        return VisualizationConfig(type=ChartType.TABLE)

    def _is_date_column(self, col_name: str, sample_values: list[Any]) -> bool:
        """
        Check if column appears to be a date/time column.

        Args:
            col_name: Column name
            sample_values: Sample values from column

        Returns:
            True if likely a date column
        """
        # Check column name
        date_keywords = ["date", "time", "month", "year", "day", "quarter", "week"]
        if any(keyword in col_name.lower() for keyword in date_keywords):
            return True

        # Check value format (basic check)
        # In production, use more sophisticated date detection
        if sample_values and isinstance(sample_values[0], str):
            import re

            # Simple date pattern check (YYYY-MM-DD, etc.)
            date_pattern = r"\d{4}-\d{2}-\d{2}"
            if re.match(date_pattern, str(sample_values[0])):
                return True

        return False

    def format_for_chatgpt(self, result: FormattedResult) -> dict[str, Any]:
        """
        Format result for ChatGPT Enterprise.

        Args:
            result: FormattedResult

        Returns:
            ChatGPT-formatted response
        """
        response = {
            "content": {
                "type": "analytics_result",
                "data": result.data,
            },
            "metadata": {},
        }

        # Add narrative if present
        if result.narrative:
            response["content"]["narrative"] = result.narrative

        # Add visualization if present
        if result.visualization:
            response["content"]["visualization"] = {
                "type": result.visualization.type.value,
                "config": {
                    "x": result.visualization.x,
                    "y": result.visualization.y,
                    "title": result.visualization.title,
                    "x_label": result.visualization.x_label,
                    "y_label": result.visualization.y_label,
                },
            }

        # Add metadata
        if result.metadata:
            response["metadata"] = {
                "sql": result.metadata.sql,
                "runtime_ms": result.metadata.runtime_ms,
                "rows": result.metadata.rows_returned,
                "trace_id": result.metadata.trace_id,
            }

        return response

    def format_for_bedrock(self, result: FormattedResult) -> dict[str, Any]:
        """
        Format result for AWS Bedrock.

        Args:
            result: FormattedResult

        Returns:
            Bedrock-formatted response
        """
        # Bedrock agent response format
        response = {
            "messageVersion": "1.0",
            "response": {
                "actionGroup": "dremio_analytics",
                "function": "query_data",
                "functionResponse": {
                    "responseBody": {
                        "TEXT": {
                            "body": result.narrative
                            or "Query executed successfully"
                        }
                    }
                },
            },
        }

        # Add structured data
        response["response"]["functionResponse"]["responseBody"]["data"] = result.data

        # Add visualization hint
        if result.visualization:
            response["response"]["functionResponse"]["responseBody"][
                "visualization"
            ] = {
                "type": result.visualization.type.value,
                "x_axis": result.visualization.x,
                "y_axis": result.visualization.y,
            }

        return response

    def format_for_mcp(self, result: FormattedResult) -> dict[str, Any]:
        """
        Format result for standard MCP protocol.

        Args:
            result: FormattedResult

        Returns:
            MCP-formatted response
        """
        response = {"data": result.data}

        if result.visualization:
            response["visualization"] = {
                "type": result.visualization.type.value,
                "x": result.visualization.x,
                "y": result.visualization.y,
                "title": result.visualization.title,
            }

        if result.metadata:
            response["metadata"] = {
                "sql": result.metadata.sql,
                "job_id": result.metadata.job_id,
                "runtime_ms": result.metadata.runtime_ms,
                "cost_dcu": result.metadata.cost_dcu,
                "rows_returned": result.metadata.rows_returned,
                "trace_id": result.metadata.trace_id,
            }

        if result.narrative:
            response["narrative"] = result.narrative

        return response
