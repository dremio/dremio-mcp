"""
Analytics REST API Endpoints.

This module provides REST API endpoints for:
- ChatGPT Enterprise integration
- AWS Bedrock integration
- Standard analytics API
"""

from typing import Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Header
from pydantic import BaseModel, Field

from dremioai.analytics.orchestrator import AnalyticsOrchestrator
from dremioai.analytics.results import ResponseFormat
from dremioai.api.transport import DremioAsyncHttpClient
from dremioai.config.settings import Settings, get_settings

logger = structlog.get_logger(__name__)

# Create router
router = APIRouter(prefix="/analytics", tags=["analytics"])


# Request/Response models
class AnalyticsQueryRequest(BaseModel):
    """Analytics query request."""

    query: str = Field(..., description="Natural language query")
    user_id: Optional[str] = Field(None, description="User identifier for quota tracking")
    session_id: Optional[str] = Field(None, description="Session identifier")
    response_format: Optional[str] = Field(
        "mcp_standard",
        description="Response format: mcp_standard, chatgpt_enterprise, aws_bedrock",
    )


class AnalyticsQueryResponse(BaseModel):
    """Analytics query response."""

    status: str = Field(..., description="Response status: success, error")
    data: Optional[dict] = Field(None, description="Query results and visualization")
    error: Optional[str] = Field(None, description="Error message if status=error")
    trace_id: Optional[str] = Field(None, description="Trace ID for debugging")


class HealthCheckResponse(BaseModel):
    """Health check response."""

    status: str = "healthy"
    version: str = "1.0.0"
    components: dict = Field(default_factory=dict)


# Dependency for creating orchestrator
async def get_orchestrator(
    settings: Settings = Depends(get_settings),
) -> AnalyticsOrchestrator:
    """
    Create and return Analytics Orchestrator.

    Args:
        settings: Application settings

    Returns:
        Configured AnalyticsOrchestrator
    """
    # Create Dremio client
    dremio_client = DremioAsyncHttpClient(
        uri=settings.dremio.uri,
        pat=settings.dremio.pat,
    )

    # Create orchestrator
    orchestrator = AnalyticsOrchestrator(
        dremio_client=dremio_client,
        use_llm=False,  # TODO: Configure from settings
        fuzzy_threshold=0.8,  # TODO: Configure from settings
        max_rows=1_000_000,  # TODO: Configure from settings
        max_cost_dcu=100.0,  # TODO: Configure from settings
    )

    return orchestrator


@router.post("/query", response_model=AnalyticsQueryResponse)
async def query_analytics(
    request: AnalyticsQueryRequest,
    authorization: Optional[str] = Header(None),
    orchestrator: AnalyticsOrchestrator = Depends(get_orchestrator),
) -> AnalyticsQueryResponse:
    """
    Execute analytics query.

    This endpoint accepts natural language queries and returns formatted results
    with automatic visualization selection.

    Supports multiple response formats:
    - mcp_standard: Standard MCP protocol response
    - chatgpt_enterprise: ChatGPT Enterprise formatted response
    - aws_bedrock: AWS Bedrock agent formatted response

    Args:
        request: Query request
        authorization: Bearer token from Authorization header
        orchestrator: Analytics orchestrator dependency

    Returns:
        AnalyticsQueryResponse with results or error

    Raises:
        HTTPException: If query processing fails
    """
    logger.info(
        "analytics_query_received",
        query=request.query,
        user_id=request.user_id,
        format=request.response_format,
    )

    try:
        # Parse response format
        response_format = ResponseFormat.MCP_STANDARD
        if request.response_format == "chatgpt_enterprise":
            response_format = ResponseFormat.CHATGPT_ENTERPRISE
        elif request.response_format == "aws_bedrock":
            response_format = ResponseFormat.AWS_BEDROCK

        # Process query
        result = await orchestrator.process_query(
            query=request.query,
            user_id=request.user_id,
            response_format=response_format,
        )

        # Extract trace_id if present
        trace_id = None
        if isinstance(result, dict):
            metadata = result.get("metadata", {})
            trace_id = metadata.get("trace_id")

        logger.info(
            "analytics_query_success",
            query=request.query,
            trace_id=trace_id,
        )

        return AnalyticsQueryResponse(
            status="success",
            data=result,
            trace_id=trace_id,
        )

    except ValueError as e:
        # Validation errors (400)
        logger.warning(
            "analytics_query_validation_error",
            query=request.query,
            error=str(e),
        )
        raise HTTPException(status_code=400, detail=str(e))

    except RuntimeError as e:
        # Runtime errors like safety gate rejection (403)
        logger.warning(
            "analytics_query_rejected",
            query=request.query,
            error=str(e),
        )
        raise HTTPException(status_code=403, detail=str(e))

    except Exception as e:
        # Internal errors (500)
        logger.error(
            "analytics_query_error",
            query=request.query,
            error=str(e),
        )
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@router.get("/health", response_model=HealthCheckResponse)
async def health_check() -> HealthCheckResponse:
    """
    Analytics API health check.

    Returns:
        HealthCheckResponse with component status
    """
    return HealthCheckResponse(
        status="healthy",
        version="1.0.0",
        components={
            "resolver": "ok",
            "planner": "ok",
            "compiler": "ok",
            "safety_gate": "ok",
            "diagnostics": "ok",
            "results_processor": "ok",
        },
    )


# ChatGPT Enterprise specific endpoint
@router.post("/chatgpt/query")
async def chatgpt_query(
    request: AnalyticsQueryRequest,
    x_openai_user_id: Optional[str] = Header(None, alias="X-OpenAI-User-ID"),
    authorization: Optional[str] = Header(None),
    orchestrator: AnalyticsOrchestrator = Depends(get_orchestrator),
) -> dict:
    """
    ChatGPT Enterprise specific analytics query endpoint.

    This endpoint is optimized for ChatGPT Enterprise integration and
    automatically uses the chatgpt_enterprise response format.

    Args:
        request: Query request
        x_openai_user_id: OpenAI user ID from header
        authorization: Bearer token (Azure AD)
        orchestrator: Analytics orchestrator

    Returns:
        ChatGPT-formatted response
    """
    logger.info(
        "chatgpt_query_received",
        query=request.query,
        openai_user_id=x_openai_user_id,
    )

    # Use OpenAI user ID if provided
    user_id = x_openai_user_id or request.user_id

    try:
        result = await orchestrator.process_query(
            query=request.query,
            user_id=user_id,
            response_format=ResponseFormat.CHATGPT_ENTERPRISE,
        )

        return result

    except Exception as e:
        logger.error("chatgpt_query_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# AWS Bedrock specific endpoint
@router.post("/bedrock/action")
async def bedrock_action(
    request: dict,
    orchestrator: AnalyticsOrchestrator = Depends(get_orchestrator),
) -> dict:
    """
    AWS Bedrock agent action endpoint.

    This endpoint follows the Bedrock agent action format and
    automatically uses the aws_bedrock response format.

    Expected request format:
    {
        "messageVersion": "1.0",
        "agent": {...},
        "actionGroup": "dremio_analytics",
        "function": "query_data",
        "parameters": [
            {"name": "query", "value": "Show me revenue by category"}
        ]
    }

    Args:
        request: Bedrock agent request
        orchestrator: Analytics orchestrator

    Returns:
        Bedrock-formatted response
    """
    logger.info("bedrock_action_received", request=request)

    try:
        # Extract parameters from Bedrock format
        parameters = {
            param["name"]: param["value"]
            for param in request.get("parameters", [])
        }

        query = parameters.get("query")
        if not query:
            raise ValueError("Missing required parameter: query")

        user_id = request.get("agent", {}).get("userId")

        result = await orchestrator.process_query(
            query=query,
            user_id=user_id,
            response_format=ResponseFormat.AWS_BEDROCK,
        )

        return result

    except Exception as e:
        logger.error("bedrock_action_error", error=str(e))

        # Return Bedrock error format
        return {
            "messageVersion": "1.0",
            "response": {
                "actionGroup": request.get("actionGroup"),
                "function": request.get("function"),
                "functionResponse": {
                    "responseState": "FAILURE",
                    "responseBody": {
                        "TEXT": {
                            "body": f"Error processing query: {str(e)}"
                        }
                    },
                },
            },
        }


# Export router for FastAPI app
__all__ = ["router"]
