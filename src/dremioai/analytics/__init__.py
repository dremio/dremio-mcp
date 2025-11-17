"""
Enterprise Analytics Components for Dremio MCP Server.

This package contains the core components for the analytics pipeline:
- Resolver: Intent classification and entity extraction
- Planner: Semantic grounding and fuzzy matching
- Compiler: SQL generation and validation
- Safety: Pre-execution checks and limits
- Diagnostics: Root cause analysis for "why" queries
- Results: Response formatting and visualization
"""

from .resolver import Resolver, QueryIntent, ResolvedQuery
from .planner import Planner, GroundedPlan
from .compiler import Compiler, CompiledQuery
from .safety import SafetyGate, SafetyCheck
from .diagnostics import DiagnosticsAgent, DiagnosticResult
from .results import ResultsProcessor, FormattedResult

__all__ = [
    "Resolver",
    "QueryIntent",
    "ResolvedQuery",
    "Planner",
    "GroundedPlan",
    "Compiler",
    "CompiledQuery",
    "SafetyGate",
    "SafetyCheck",
    "DiagnosticsAgent",
    "DiagnosticResult",
    "ResultsProcessor",
    "FormattedResult",
]
