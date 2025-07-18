#
#  Copyright (C) 2017-2025 Dremio Corporation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import pytest
from dremioai.tools.tools import RunSqlQuery
from dremioai.config import settings
from typing import Dict, Union
from contextlib import contextmanager, asynccontextmanager
from tempfile import TemporaryDirectory
from pathlib import Path

sql_test_statements = [
    {
        "sql": "SELECT * FROM users;",
        "allowed": True,
        "comment": "Simple SELECT query with wildcard",
    },
    {
        "sql": "SELECT name, email, created_at FROM users WHERE active = true AND age > 18;",
        "allowed": True,
        "comment": "SELECT with WHERE clause using multiple conditions",
    },
    {
        "sql": "SELECT u.name, p.title, p.created_at FROM users u INNER JOIN posts p ON u.id = p.user_id WHERE u.active = true;",
        "allowed": True,
        "comment": "SELECT with INNER JOIN and table aliases",
    },
    {
        "sql": """SELECT 
    d.department_name,
    COUNT(e.id) as employee_count,
    AVG(e.salary) as avg_salary,
    MAX(e.hire_date) as latest_hire
FROM departments d
LEFT JOIN employees e ON d.id = e.department_id
LEFT JOIN positions pos ON e.position_id = pos.id
WHERE d.active = true
GROUP BY d.id, d.department_name
HAVING COUNT(e.id) > 5
ORDER BY avg_salary DESC, d.department_name ASC;""",
        "allowed": True,
        "comment": "Complex SELECT with multiple JOINs, GROUP BY, HAVING, and ORDER BY",
    },
    {
        "sql": """SELECT name, email
FROM users
WHERE id IN (
    SELECT user_id 
    FROM orders 
    WHERE total_amount > 1000 
    AND order_date >= '2024-01-01'
);""",
        "allowed": True,
        "comment": "SELECT with subquery using IN clause",
    },
    {
        "sql": """SELECT u.name, u.email
FROM users u
WHERE EXISTS (
    SELECT 1 
    FROM orders o 
    WHERE o.user_id = u.id 
    AND o.total_amount > (
        SELECT AVG(total_amount) 
        FROM orders 
        WHERE user_id = u.id
    )
);""",
        "allowed": True,
        "comment": "SELECT with correlated subquery and EXISTS clause",
    },
    {
        "sql": """SELECT 
    name,
    department_id,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) as salary_rank,
    LAG(salary, 1) OVER (PARTITION BY department_id ORDER BY hire_date) as prev_salary,
    SUM(salary) OVER (PARTITION BY department_id) as dept_total_salary
FROM employees
WHERE active = true;""",
        "allowed": True,
        "comment": "SELECT with window functions (ROW_NUMBER, LAG, SUM OVER)",
    },
    {
        "sql": """WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('month', order_date) as month,
        SUM(total_amount) as monthly_total,
        COUNT(*) as order_count
    FROM orders
    WHERE order_date >= '2024-01-01'
    GROUP BY DATE_TRUNC('month', order_date)
),
sales_growth AS (
    SELECT 
        month,
        monthly_total,
        order_count,
        LAG(monthly_total) OVER (ORDER BY month) as prev_month_total,
        (monthly_total - LAG(monthly_total) OVER (ORDER BY month)) / 
        LAG(monthly_total) OVER (ORDER BY month) * 100 as growth_rate
    FROM monthly_sales
)
SELECT * FROM sales_growth WHERE growth_rate > 10;""",
        "allowed": True,
        "comment": "SELECT with multiple CTEs (Common Table Expressions)",
    },
    {
        "sql": """WITH RECURSIVE employee_hierarchy AS (
    SELECT id, name, manager_id, 0 as level
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    SELECT e.id, e.name, e.manager_id, eh.level + 1
    FROM employees e
    INNER JOIN employee_hierarchy eh ON e.manager_id = eh.id
)
SELECT * FROM employee_hierarchy ORDER BY level, name;""",
        "allowed": True,
        "comment": "Recursive CTE for hierarchical data traversal",
    },
    {
        "sql": "INSERT INTO users (name, email, age, active) VALUES ('John Doe', 'john@example.com', 30, true);",
        "allowed": False,
        "comment": "Simple INSERT statement with single row",
    },
    {
        "sql": """INSERT INTO products (name, price, category_id, description)
VALUES 
    ('Laptop', 999.99, 1, 'High-performance laptop'),
    ('Mouse', 29.99, 1, 'Wireless optical mouse'),
    ('Keyboard', 79.99, 1, 'Mechanical gaming keyboard');""",
        "allowed": False,
        "comment": "INSERT statement with multiple rows",
    },
    {
        "sql": """INSERT INTO user_audit (user_id, action, timestamp)
SELECT id, 'CREATED', created_at
FROM users
WHERE created_at >= CURRENT_DATE - INTERVAL '1 day';""",
        "allowed": False,
        "comment": "INSERT with SELECT subquery",
    },
    {
        "sql": "UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = 123;",
        "allowed": False,
        "comment": "Simple UPDATE statement",
    },
    {
        "sql": """UPDATE employees e
SET salary = e.salary * 1.1
FROM departments d
WHERE e.department_id = d.id
AND d.department_name = 'Engineering'
AND e.performance_rating >= 4;""",
        "allowed": False,
        "comment": "UPDATE with JOIN using FROM clause",
    },
    {
        "sql": """UPDATE orders
SET status = CASE 
    WHEN total_amount > 1000 THEN 'HIGH_VALUE'
    WHEN total_amount > 500 THEN 'MEDIUM_VALUE'
    ELSE 'STANDARD'
END,
priority = CASE
    WHEN customer_tier = 'PREMIUM' THEN 1
    WHEN customer_tier = 'GOLD' THEN 2
    ELSE 3
END
WHERE status = 'PENDING';""",
        "allowed": False,
        "comment": "UPDATE with CASE statements for conditional logic",
    },
    {
        "sql": "DELETE FROM temp_data WHERE created_at < CURRENT_DATE - INTERVAL '30 days';",
        "allowed": False,
        "comment": "Simple DELETE statement with date condition",
    },
    {
        "sql": """DELETE o
FROM orders o
INNER JOIN users u ON o.user_id = u.id
WHERE u.active = false AND o.status = 'CANCELLED';""",
        "allowed": False,
        "comment": "DELETE with JOIN operation",
    },
    {
        "sql": """CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);""",
        "allowed": False,
        "comment": "Simple CREATE TABLE with basic constraints",
    },
    {
        "sql": """CREATE TABLE IF NOT EXISTS order_items (
    id BIGSERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price >= 0),
    discount_percent DECIMAL(5,2) DEFAULT 0 CHECK (discount_percent >= 0 AND discount_percent <= 100),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    CONSTRAINT fk_order_items_product FOREIGN KEY (product_id) REFERENCES products(id),
    CONSTRAINT uk_order_product UNIQUE (order_id, product_id)
);""",
        "allowed": False,
        "comment": "Complex CREATE TABLE with constraints, foreign keys, and check constraints",
    },
    {
        "sql": "CREATE INDEX CONCURRENTLY idx_users_email_active ON users (email) WHERE active = true;",
        "allowed": False,
        "comment": "CREATE INDEX with partial index condition",
    },
    {
        "sql": """CREATE OR REPLACE VIEW active_user_orders AS
SELECT 
    u.id as user_id,
    u.name as user_name,
    u.email,
    o.id as order_id,
    o.total_amount,
    o.status,
    o.order_date
FROM users u
INNER JOIN orders o ON u.id = o.user_id
WHERE u.active = true AND o.status != 'CANCELLED';""",
        "allowed": False,
        "comment": "CREATE VIEW with JOIN and filtering",
    },
    {
        "sql": """ALTER TABLE users 
ADD COLUMN phone_number VARCHAR(20),
ADD COLUMN address_id INTEGER,
ADD CONSTRAINT fk_users_address FOREIGN KEY (address_id) REFERENCES addresses(id);""",
        "allowed": False,
        "comment": "ALTER TABLE to add columns and constraints",
    },
    {
        "sql": """ALTER TABLE products 
ALTER COLUMN description TYPE TEXT,
ALTER COLUMN price SET NOT NULL,
ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP;""",
        "allowed": False,
        "comment": "ALTER TABLE to modify existing columns",
    },
    {
        "sql": "DROP TABLE IF EXISTS temp_imports CASCADE;",
        "allowed": False,
        "comment": "DROP TABLE with CASCADE option",
    },
    {
        "sql": """WITH customer_metrics AS (
    SELECT 
        u.id,
        u.name,
        COUNT(o.id) as total_orders,
        SUM(o.total_amount) as total_spent,
        AVG(o.total_amount) as avg_order_value,
        MIN(o.order_date) as first_order_date,
        MAX(o.order_date) as last_order_date
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    WHERE u.active = true
    GROUP BY u.id, u.name
),
customer_segments AS (
    SELECT 
        *,
        CASE 
            WHEN total_spent > 5000 THEN 'VIP'
            WHEN total_spent > 1000 THEN 'PREMIUM'
            WHEN total_spent > 100 THEN 'REGULAR'
            ELSE 'NEW'
        END as segment,
        NTILE(4) OVER (ORDER BY total_spent DESC) as spending_quartile,
        PERCENT_RANK() OVER (ORDER BY total_orders) as order_percentile
    FROM customer_metrics
)
SELECT 
    name,
    segment,
    total_orders,
    total_spent,
    avg_order_value,
    spending_quartile,
    ROUND(order_percentile * 100, 2) as order_percentile_rank,
    EXTRACT(DAYS FROM (last_order_date - first_order_date)) as customer_lifetime_days
FROM customer_segments
WHERE total_orders > 0
ORDER BY total_spent DESC, total_orders DESC;""",
        "allowed": True,
        "comment": "Complex query with multiple CTEs, window functions, and advanced analytics",
    },
    {
        "sql": """INSERT INTO user_preferences (user_id, preference_key, preference_value)
VALUES (123, 'theme', 'dark')
ON CONFLICT (user_id, preference_key)
DO UPDATE SET 
    preference_value = EXCLUDED.preference_value,
    updated_at = CURRENT_TIMESTAMP;""",
        "allowed": False,
        "comment": "UPSERT operation using ON CONFLICT clause",
    },
    {
        "sql": """SELECT 'active' as user_type, COUNT(*) as count FROM users WHERE active = true
UNION ALL
SELECT 'inactive' as user_type, COUNT(*) as count FROM users WHERE active = false
UNION ALL
SELECT 'total' as user_type, COUNT(*) as count FROM users;""",
        "allowed": True,
        "comment": "SELECT with UNION ALL for combining multiple result sets",
    },
    {
        "sql": """DELETE FROM notifications n
WHERE EXISTS (
    SELECT 1 FROM users u 
    WHERE u.id = n.user_id 
    AND u.active = false
)
AND n.created_at < CURRENT_DATE - INTERVAL '90 days';""",
        "allowed": False,
        "comment": "Complex DELETE with EXISTS subquery",
    },
    {
        "sql": """CREATE OR REPLACE FUNCTION calculate_order_tax(order_total DECIMAL, tax_rate DECIMAL DEFAULT 0.08)
RETURNS DECIMAL AS $$
BEGIN
    RETURN ROUND(order_total * tax_rate, 2);
END;
$$ LANGUAGE plpgsql;""",
        "allowed": False,
        "comment": "CREATE FUNCTION with default parameters and PL/pgSQL",
    },
    {
        "sql": """BEGIN;
    UPDATE accounts SET balance = balance - 100 WHERE id = 1;
    UPDATE accounts SET balance = balance + 100 WHERE id = 2;
    INSERT INTO transactions (from_account, to_account, amount, type) 
    VALUES (1, 2, 100, 'TRANSFER');
COMMIT;""",
        "allowed": False,
        "comment": "Transaction block with multiple statements",
    },
    {
        "sql": """
SELECT 
    a.industry,
    COUNT(DISTINCT o.id) as won_deals,
    COUNT(DISTINCT o.account_id) as unique_accounts,
    AVG(o.amount) as avg_deal_size,
    SUM(o.amount) as total_revenue
FROM "eu-central-1-glue".salesforce.opportunity o
JOIN "eu-central-1-glue".salesforce.account a ON o.account_id = a.id
WHERE o.is_won = true 
    AND o.is_deleted = false
    AND a.is_deleted = false
    AND o.close_date >= '2023-01-01'
    AND a.industry IS NOT NULL
GROUP BY a.industry
ORDER BY won_deals DESC
LIMIT 20
""",
        "allowed": True,
        "comment": "With delete in column name 1",
    },
    {
        "sql": """SELECT COUNT(*) as total_opportunities 
FROM "eu-central-1-glue".salesforce.opportunity 
WHERE is_deleted = false
""",
        "allowed": True,
        "comment": "With delete in column name 2",
    },
    {
        "sql": """COPY INTO "optimized_internal-data-lake-s3"."us"."jobs"."data"
        FROM '@eu-dev-internal-datalake/us/jobs_json/2025-05-29-20:00'
        REGEX '.json$' FILE_FORMAT 'json' (TIMESTAMP_FORMAT 'YYYY-MM-DD"T"HH24:MI:SS.FFF"Z"')
        """,
        "allowed": False,
        "comment": "COPY INTO statement",
    },
]


@contextmanager
def mock_settings(dml_allowed: bool):
    old_settings = settings.instance()
    try:
        settings._settings.set(
            settings.Settings.model_validate(
                {
                    "dremio": {
                        "uri": "https://test-dremio-uri.com",
                        "allow_dml": dml_allowed,
                    }
                }
            )
        )
        yield
    finally:
        settings._settings.set(old_settings)


@pytest.mark.parametrize(
    "dml_allowed",
    [pytest.param(False, id="DML not allowed"), pytest.param(True, id="DML Allowed")],
)
@pytest.mark.parametrize(
    "s", [pytest.param(s, id=s["comment"]) for s in sql_test_statements]
)
def test_run_sql_safety(s: Dict[str, Union[str, bool]], dml_allowed: bool):
    with mock_settings(dml_allowed):
        allowed = s["allowed"] if not dml_allowed else True
        try:
            RunSqlQuery.ensure_query_allowed(s["sql"])
            assert allowed, f'should not be allowed: {s["sql"]}'
        except ValueError:
            assert not allowed, f'should be allowed: {s["sql"]}'


@pytest.mark.asyncio
async def test_run_sql_query_through_mcp_server():
    """Test RunSqlQuery tool invocation through MCP server"""
    # Import MCP client components
    from mcp.client.session import ClientSession
    from mcp.client.stdio import stdio_client, StdioServerParameters
    from dremioai.servers import mcp as mcp_server
    from dremioai.config.tools import ToolType

    @contextmanager
    def mock_settings_for_mcp(mode: ToolType):
        """Create mock settings for testing MCP server"""
        try:
            old = settings.instance()
            with TemporaryDirectory() as temp_dir:
                temp_dir = Path(temp_dir)
                settings._settings.set(
                    settings.Settings.model_validate(
                        {
                            "dremio": {
                                "uri": "https://test-dremio-uri.com",
                                "pat": "test-pat",
                                "project_id": "test-project-id",
                            },
                            "tools": {"server_mode": mode},
                        }
                    )
                )
                cfg = temp_dir / "config.yaml"
                settings.write_settings(cfg=cfg, inst=settings.instance())
                yield settings.instance(), cfg
        finally:
            settings._settings.set(old)

    @asynccontextmanager
    async def mcp_server_session(cfg: Path):
        """Create an MCP server instance with mock settings"""
        params = mcp_server.create_default_mcpserver_config()
        params["args"].extend(["--cfg", str(cfg)])
        params = StdioServerParameters(command=params["command"], args=params["args"])
        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            yield session

    test_query = "SELECT 1 as test_column"

    # Create MCP server configuration
    with mock_settings_for_mcp(ToolType.FOR_DATA_PATTERNS) as (_, cfg):
        # Start MCP server and create client session
        async with mcp_server_session(cfg) as session:
            # Verify RunSqlQuery tool is available
            tools = await session.list_tools()
            tool_names = {tool.name for tool in tools.tools}
            assert "RunSqlQuery" in tool_names

            # Call the RunSqlQuery tool through MCP
            # This will fail because we don't have real Dremio credentials,
            # but we can verify the tool is invoked and returns an error response
            result = await session.call_tool("RunSqlQuery", arguments={"s": test_query})

            # Verify we get a response (even if it's an error)
            assert result.content is not None
            assert len(result.content) > 0

            # The response should be a string representation of a dictionary
            response_text = result.content[0].text
            assert isinstance(response_text, str)

            # Since we don't have real credentials, we expect an error response
            # The response should contain either "error" or "results" key
            # Let's verify the response has the expected structure
            import ast

            try:
                # Try to parse the response as a Python literal
                parsed_response = ast.literal_eval(response_text)
                assert isinstance(parsed_response, dict)
                # Should have either "results" or "error" key
                assert "result" in parsed_response or "error" in parsed_response

                # If it's an error response, verify it has the expected error structure
                if "error" in parsed_response:
                    assert "message" in parsed_response
                    assert (
                        parsed_response["message"]
                        == "The query failed. Please check the syntax and try again"
                    )

            except (ValueError, SyntaxError):
                # If we can't parse it as a literal, it might be JSON
                import json

                try:
                    parsed_response = json.loads(response_text)
                    assert isinstance(parsed_response, dict)
                    assert "results" in parsed_response or "error" in parsed_response
                except json.JSONDecodeError:
                    # If neither works, just verify we got some response
                    assert len(response_text) > 0


@pytest.mark.asyncio
async def test_run_sql_query_through_fastmcp_direct():
    """Test RunSqlQuery tool invocation directly through FastMCP server"""
    from dremioai.servers import mcp as mcp_server
    from dremioai.config.tools import ToolType
    from unittest.mock import AsyncMock, patch

    test_query = "SELECT 1 as test_column"

    @contextmanager
    def mock_settings_for_fastmcp(mode: ToolType):
        """Create mock settings for testing FastMCP server"""
        try:
            old = settings.instance()
            settings._settings.set(
                settings.Settings.model_validate(
                    {
                        "dremio": {
                            "uri": "https://test-dremio-uri.com",
                            "pat": "test-pat",
                            "project_id": "test-project-id",
                        },
                        "tools": {"server_mode": mode},
                    }
                )
            )
            yield settings.instance()
        finally:
            settings._settings.set(old)

    # Create FastMCP server directly
    with mock_settings_for_fastmcp(ToolType.FOR_DATA_PATTERNS):
        # Mock the sql.run_query function to avoid actual database calls
        with patch(
            "dremioai.api.dremio.sql.run_query", new_callable=AsyncMock
        ) as mock_run_query:
            # Create a mock DataFrame response
            import pandas as pd

            mock_df = pd.DataFrame([{"test_column": 1}])
            mock_run_query.return_value = mock_df

            # Initialize FastMCP server with tools
            fastmcp_server = mcp_server.init(mode=ToolType.FOR_DATA_PATTERNS)

            # Verify RunSqlQuery tool is registered
            tools = await fastmcp_server.list_tools()
            tool_names = {tool.name for tool in tools}
            assert "RunSqlQuery" in tool_names

            # Call the RunSqlQuery tool directly through FastMCP
            result = await fastmcp_server.call_tool("RunSqlQuery", {"s": test_query})

            # Verify the mock was called with correct parameters
            mock_run_query.assert_called_once_with(
                query=f"/* dremioai: submitter=RunSqlQuery */\n{test_query}",
                use_df=True,
            )

            # FastMCP call_tool returns a tuple: (content_blocks, metadata)
            # The actual result is in the metadata under 'result' key
            assert isinstance(result, tuple)
            assert len(result) == 2

            content_blocks, metadata = result
            assert "result" in metadata
            actual_result = metadata["result"]

            # Verify the result structure
            assert isinstance(actual_result, dict)
            assert "results" in actual_result
            assert len(actual_result["results"]) == 1
            assert actual_result["results"][0]["test_column"] == 1

            # Also verify the content blocks contain the JSON representation
            assert len(content_blocks) == 1
            import json

            content_json = json.loads(content_blocks[0].text)
            assert content_json == actual_result
