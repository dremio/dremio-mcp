"""
Compiler & Validator Component - SQL Generation and Validation.

This module generates SQL from grounded plans and validates it using AST parsing
to prevent SQL injection and enforce security policies.
"""

from dataclasses import dataclass, field
from typing import Any, Optional

import structlog
import sqlglot
from sqlglot import exp, parse_one

from dremioai.analytics.planner import GroundedPlan

logger = structlog.get_logger(__name__)


@dataclass
class ASTValidationResult:
    """Results of AST validation checks."""

    valid: bool
    no_ddl: bool = False
    no_dml: bool = False
    no_export: bool = False
    schema_allowed: bool = False
    no_select_star: bool = False
    allowed_operations: bool = False
    errors: list[str] = field(default_factory=list)


@dataclass
class CompiledQuery:
    """Compiled SQL query with validation results."""

    sql: str
    validated: bool
    ast_checks: ASTValidationResult
    metadata: dict[str, Any] = field(default_factory=dict)


class Compiler:
    """
    Compiler & Validator component for SQL generation and validation.

    This component:
    1. Generates SQL from grounded plan (optionally using LLM)
    2. Parses SQL to AST
    3. Validates security constraints
    4. Enforces schema allowlist
    """

    # Allowed top-level statement types
    ALLOWED_STATEMENTS = {exp.Select, exp.With}

    # Blocked statement types (DDL/DML/EXPORT)
    BLOCKED_STATEMENTS = {
        exp.Create,
        exp.Drop,
        exp.Alter,
        exp.Insert,
        exp.Update,
        exp.Delete,
        exp.Merge,
        exp.Truncate,
        exp.Export,
        exp.Copy,
    }

    # Allowed operations within SELECT
    ALLOWED_OPERATIONS = {
        exp.Join,
        exp.Where,
        exp.Group,
        exp.Order,
        exp.Limit,
        exp.Having,
        exp.Union,
        exp.Subquery,
    }

    # Schema allowlist (configurable per deployment)
    DEFAULT_SCHEMA_ALLOWLIST = [
        "sales",
        "commerce",
        "customer",
        "finance",
        "marketing",
        "inventory",
    ]

    def __init__(
        self,
        schema_allowlist: Optional[list[str]] = None,
        use_llm: bool = False,
        llm_provider: Optional[str] = None,
    ):
        """
        Initialize the Compiler.

        Args:
            schema_allowlist: List of allowed schema names
            use_llm: Whether to use LLM for SQL generation
            llm_provider: LLM provider (anthropic, openai, bedrock)
        """
        self.schema_allowlist = schema_allowlist or self.DEFAULT_SCHEMA_ALLOWLIST
        self.use_llm = use_llm
        self.llm_provider = llm_provider

    async def compile(self, plan: GroundedPlan) -> CompiledQuery:
        """
        Compile a grounded plan into validated SQL.

        Args:
            plan: GroundedPlan from Planner component

        Returns:
            CompiledQuery with SQL and validation results
        """
        logger.info("compiling_query", metrics=len(plan.metrics))

        # Generate SQL (rule-based or LLM)
        if self.use_llm:
            sql = await self._generate_sql_with_llm(plan)
        else:
            sql = self._generate_sql_rule_based(plan)

        logger.info("sql_generated", sql=sql)

        # Validate SQL
        validation = self._validate_sql(sql)

        compiled = CompiledQuery(
            sql=sql,
            validated=validation.valid,
            ast_checks=validation,
            metadata={
                "metrics": [m.canonical for m in plan.metrics],
                "dimensions": [d.canonical for d in plan.dimensions],
                "joins": len(plan.join_paths),
            },
        )

        if not validation.valid:
            logger.error(
                "sql_validation_failed",
                errors=validation.errors,
                sql=sql,
            )
        else:
            logger.info("sql_validated")

        return compiled

    def _generate_sql_rule_based(self, plan: GroundedPlan) -> str:
        """
        Generate SQL using rule-based approach.

        Args:
            plan: GroundedPlan

        Returns:
            Generated SQL string
        """
        # Build SELECT clause
        select_parts = []

        # Add dimensions
        for dim in plan.dimensions:
            select_parts.append(f"{dim.table}.{dim.column} as {dim.canonical}")

        # Add metrics
        for metric in plan.metrics:
            select_parts.append(f"{metric.definition} as {metric.canonical}")

        select_clause = "SELECT " + ",\n  ".join(select_parts)

        # Build FROM clause
        # Use first table from metrics or dimensions
        if plan.metrics:
            from_table = plan.metrics[0].table
        elif plan.dimensions:
            from_table = plan.dimensions[0].table
        else:
            raise ValueError("No metrics or dimensions in plan")

        from_clause = f"FROM {from_table}"

        # Build JOIN clauses
        join_clauses = []
        for join in plan.join_paths:
            join_clauses.append(
                f"{join.join_type} JOIN {join.to_table} ON {join.condition}"
            )

        # Build WHERE clause (filters)
        where_clauses = []
        for filter_def in plan.filters:
            # Example filter format: {"column": "status", "operator": "=", "value": "completed"}
            column = filter_def.get("column")
            operator = filter_def.get("operator", "=")
            value = filter_def.get("value")
            if column and value:
                if isinstance(value, str):
                    where_clauses.append(f"{column} {operator} '{value}'")
                else:
                    where_clauses.append(f"{column} {operator} {value}")

        # Add default filter for completed orders (example)
        # In production, this would come from plan metadata
        where_clause = ""
        if where_clauses:
            where_clause = "WHERE " + " AND ".join(where_clauses)

        # Build GROUP BY clause (if dimensions present)
        group_by_clause = ""
        if plan.dimensions:
            group_by_parts = [
                f"{dim.table}.{dim.column}" for dim in plan.dimensions
            ]
            group_by_clause = "GROUP BY " + ", ".join(group_by_parts)

        # Build ORDER BY clause (order by first metric desc)
        order_by_clause = ""
        if plan.metrics:
            order_by_clause = f"ORDER BY {plan.metrics[0].canonical} DESC"

        # Assemble SQL
        sql_parts = [select_clause, from_clause]

        if join_clauses:
            sql_parts.extend(join_clauses)

        if where_clause:
            sql_parts.append(where_clause)

        if group_by_clause:
            sql_parts.append(group_by_clause)

        if order_by_clause:
            sql_parts.append(order_by_clause)

        sql = "\n".join(sql_parts)

        return sql

    async def _generate_sql_with_llm(self, plan: GroundedPlan) -> str:
        """
        Generate SQL using LLM.

        Args:
            plan: GroundedPlan

        Returns:
            Generated SQL string

        Note:
            This is a placeholder for LLM integration.
        """
        # TODO: Implement LLM-based SQL generation
        # This would involve:
        # 1. Create prompt with plan details
        # 2. Include metric definitions, join paths
        # 3. Call LLM API
        # 4. Extract SQL from response
        # 5. Return SQL

        logger.warning("llm_sql_generation_not_implemented")
        return self._generate_sql_rule_based(plan)

    def _validate_sql(self, sql: str) -> ASTValidationResult:
        """
        Validate SQL using AST parsing.

        Args:
            sql: SQL string to validate

        Returns:
            ASTValidationResult with detailed validation results
        """
        result = ASTValidationResult(valid=True)
        errors = []

        try:
            # Parse SQL to AST
            ast = parse_one(sql, read="dremio")

            # Check 1: Top-level statement type
            stmt_type = type(ast)
            if stmt_type not in self.ALLOWED_STATEMENTS:
                result.valid = False
                errors.append(
                    f"Statement type {stmt_type.__name__} not allowed. "
                    f"Only SELECT and WITH are permitted."
                )
            else:
                result.allowed_operations = True

            # Check 2: No DDL/DML
            blocked_found = []
            for node in ast.walk():
                if type(node) in self.BLOCKED_STATEMENTS:
                    blocked_found.append(type(node).__name__)

            if blocked_found:
                result.valid = False
                result.no_ddl = False
                result.no_dml = False
                errors.append(
                    f"Blocked operations found: {', '.join(blocked_found)}"
                )
            else:
                result.no_ddl = True
                result.no_dml = True
                result.no_export = True

            # Check 3: Schema allowlist
            tables = self._extract_tables(ast)
            disallowed_schemas = []

            for table in tables:
                if "." in table:
                    schema = table.split(".")[0]
                    if schema not in self.schema_allowlist:
                        disallowed_schemas.append(schema)

            if disallowed_schemas:
                result.valid = False
                result.schema_allowed = False
                errors.append(
                    f"Disallowed schemas: {', '.join(set(disallowed_schemas))}. "
                    f"Allowed: {', '.join(self.schema_allowlist)}"
                )
            else:
                result.schema_allowed = True

            # Check 4: No SELECT *
            has_select_star = self._check_select_star(ast)
            if has_select_star:
                result.valid = False
                result.no_select_star = False
                errors.append("SELECT * is not allowed. Specify columns explicitly.")
            else:
                result.no_select_star = True

        except Exception as e:
            result.valid = False
            errors.append(f"SQL parsing error: {str(e)}")
            logger.error("sql_parse_error", error=str(e), sql=sql)

        result.errors = errors
        return result

    def _extract_tables(self, ast: exp.Expression) -> list[str]:
        """
        Extract table names from AST.

        Args:
            ast: Parsed SQL AST

        Returns:
            List of table names (schema.table format)
        """
        tables = []

        for table_node in ast.find_all(exp.Table):
            table_name = table_node.name
            schema_name = (
                table_node.db if hasattr(table_node, "db") and table_node.db else None
            )

            if schema_name:
                tables.append(f"{schema_name}.{table_name}")
            else:
                tables.append(table_name)

        return tables

    def _check_select_star(self, ast: exp.Expression) -> bool:
        """
        Check if query contains SELECT *.

        Args:
            ast: Parsed SQL AST

        Returns:
            True if SELECT * found, False otherwise
        """
        for select_node in ast.find_all(exp.Select):
            for expr in select_node.expressions:
                if isinstance(expr, exp.Star):
                    return True

        return False
