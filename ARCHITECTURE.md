# Enterprise Analytics MCP Server Architecture

## Overview

This document describes the architecture of the Dremio MCP server as an enterprise-grade analytics platform compatible with ChatGPT Enterprise and AWS Bedrock.

## System Architecture

```
User Query (Natural Language)
    ↓
ChatGPT Enterprise / AWS Bedrock
    ↓
┌─────────────────────────────────────────────────────────┐
│                   Gateway Layer                         │
│  - Authentication (OAuth2/Bearer/Azure AD/Cognito)     │
│  - Request routing                                      │
│  - Rate limiting                                        │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│                  Resolver Component                     │
│  - Intent classification (what/why/compare)            │
│  - Entity extraction                                    │
│  - Domain identification                                │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│              Planner & Grounder                         │
│  - Fuzzy term matching                                 │
│  - Semantic layer integration                           │
│  - Join graph validation                                │
│  - Policy enforcement                                   │
└─────────────────────────────────────────────────────────┘
    ↓
    ├─ "what"  → Standard Flow
    ├─ "why"   → Diagnostics Agent
    └─ "compare" → Comparison Flow
    ↓
┌─────────────────────────────────────────────────────────┐
│             Compiler & Validator                        │
│  - LLM-based SQL generation                            │
│  - AST validation                                       │
│  - Security checks                                      │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│                  Safety Gate                            │
│  - EXPLAIN analysis                                     │
│  - Cost estimation                                      │
│  - Quota enforcement                                    │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│                  Execute                                │
│  - Dremio SQL API                                       │
│  - Result pagination                                    │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│              Results Processor                          │
│  - Data formatting                                      │
│  - Visualization selection                              │
│  - Response assembly                                    │
└─────────────────────────────────────────────────────────┘
    ↓
Return to Client (ChatGPT/Bedrock/Standard MCP)
```

## Component Details

### 1. Gateway Layer

**Responsibilities:**
- Multi-protocol authentication (OAuth2, Bearer, Azure AD, AWS Cognito)
- Request routing to appropriate handlers
- Rate limiting and quota management
- CORS handling for web clients

**Endpoints:**
- `/mcp/{project_id}/sse` - Standard MCP SSE endpoint
- `/analytics/query` - Analytics query endpoint (ChatGPT/Bedrock)
- `/analytics/health` - Health check
- `/.well-known/oauth-authorization-server` - OAuth metadata

### 2. Resolver Component

**Purpose:** Classify user intent and extract entities

**Query Types:**
- `what` - Descriptive queries (show me X by Y)
- `why` - Diagnostic queries (why did X happen?)
- `compare` - Comparison queries (compare X to Y)

**Implementation:**
- Pattern matching for common query structures
- LLM-based intent classification for complex queries
- Entity extraction (metrics, dimensions, filters, time periods)
- Domain identification based on entities

**Output:**
```python
{
  "query_type": "what",
  "entities": {
    "metrics": ["revenue"],
    "dimensions": ["product_category"],
    "filters": [],
    "time_period": null
  },
  "domains": ["sales", "commerce"],
  "raw_query": "Show me revenue by product category"
}
```

### 3. Planner & Grounder

**Purpose:** Map user terms to canonical terms and validate data model

**Fuzzy Matching:**
- Uses RapidFuzz for term similarity scoring
- Threshold: 0.8 (configurable)
- Value dictionary: Maps user terms → canonical terms
- Example: "sales" (user) → "revenue" (canonical, score: 0.92)

**Semantic Layer Integration:**
- Queries Dremio catalog API for metric definitions
- Retrieves dimension metadata
- Validates join paths between domains
- Checks user access policies

**Join Graph:**
- Defines allowed domain combinations
- Example: `sales ⟷ commerce` (via product_id)
- Blocks invalid cross-domain queries

**Output:**
```python
{
  "metrics": [{
    "canonical": "revenue",
    "definition": "SUM(order_amount)",
    "table": "sales.orders",
    "user_term": "sales",
    "match_score": 0.92
  }],
  "dimensions": [{
    "canonical": "product_category",
    "table": "commerce.products",
    "column": "category",
    "user_term": "product category",
    "match_score": 0.95
  }],
  "join_path": [{
    "from": "sales.orders",
    "to": "commerce.products",
    "condition": "sales.orders.product_id = commerce.products.product_id"
  }],
  "policy_checks": {
    "domain_access": true,
    "metric_access": true
  }
}
```

### 4. Compiler & Validator

**Purpose:** Generate and validate SQL

**SQL Generation:**
- Uses LLM (Claude/GPT-4) to generate Dremio-compatible SQL
- Provides grounded plan as context
- Includes metric definitions and join paths
- Enforces best practices (GROUP BY, aggregations)

**AST Validation:**
- Parses SQL using sqlglot
- Checks allowed operations (SELECT, WITH, JOIN, WHERE, GROUP BY)
- Blocks DDL/DML/EXPORT statements
- Validates schema allowlist
- Prevents SELECT *
- Checks for SQL injection patterns

**Output:**
```python
{
  "sql": "SELECT p.category, SUM(o.order_amount) as revenue...",
  "validated": true,
  "ast_checks": {
    "no_ddl": true,
    "no_dml": true,
    "schema_allowed": true,
    "no_select_star": true
  }
}
```

### 5. Safety Gate

**Purpose:** Pre-execution cost and limit validation

**EXPLAIN Analysis:**
- Runs EXPLAIN on Dremio to get execution plan
- Extracts estimated row count
- Extracts estimated cost (DCU)
- Identifies reflection usage

**Limit Checks:**
- Row limit: Default 1M (configurable per user/project)
- Cost limit: Default 100 DCU (configurable)
- User quota: Tracks usage across time window
- Date range: Prevents excessive historical queries

**Output:**
```python
{
  "approved": true,
  "estimated_rows": 45127,
  "estimated_cost_dcu": 5.8,
  "reflection_used": "products_orders_monthly",
  "user_quota": {
    "used": 47.3,
    "available": 952.7,
    "window": "daily"
  }
}
```

### 6. Diagnostics Agent (Why Queries)

**Purpose:** Automated root cause analysis

**Recipe Types:**
- `compare_periods` - Compare metric across time periods
- `decompose_variance` - Break down changes by dimension
- `rank_drivers` - Identify top contributing factors
- `check_events` - Look for correlated events

**Execution Flow:**
1. Compare baseline vs current period
2. Decompose by hardcoded dimensions (product, region, channel)
3. Check event tables (promotions, stockouts, pricing)
4. Rank drivers by impact
5. Calculate confidence score

**Output:**
```python
{
  "status": "diagnosed",
  "confidence": 0.92,
  "drivers": [
    {
      "factor": "promo_reduction",
      "impact": -70000,
      "impact_pct": 58,
      "evidence_sql": "...",
      "evidence_data": [...]
    },
    {
      "factor": "stockouts",
      "impact": -50000,
      "impact_pct": 42,
      "evidence_sql": "...",
      "evidence_data": [...]
    }
  ],
  "narrative": "Revenue dropped $120K due to promo days reduction (9→2) and stockouts in 4 warehouses"
}
```

### 7. Results Processor

**Purpose:** Format results for consumption

**Visualization Selection:**
- Pattern matching based on data shape
- Categorical + metric → bar chart
- Time series → line chart
- 2+ metrics → multi-series chart
- Large cardinality → table

**Response Formats:**
- **Standard MCP:** JSON response via SSE
- **ChatGPT Enterprise:** Formatted for ChatGPT display
- **AWS Bedrock:** Bedrock-compatible response format

**Output:**
```python
{
  "data": [...],
  "visualization": {
    "type": "bar_chart",
    "x": "product_category",
    "y": "revenue",
    "config": {
      "title": "Revenue by Product Category",
      "x_label": "Category",
      "y_label": "Revenue ($)"
    }
  },
  "metadata": {
    "sql": "SELECT...",
    "job_id": "abc-123",
    "runtime_ms": 847,
    "cost_dcu": 5.8,
    "trace_id": "trace-xyz",
    "rows_returned": 15
  }
}
```

## Side Rails

### Identity & Auth Bridge

**Supported Providers:**
- Azure AD (ChatGPT Enterprise)
- AWS Cognito (Bedrock)
- OAuth2 (generic)
- Bearer token (API access)

**Token Exchange:**
- User token → Dremio short-lived token
- Caching with TTL
- Automatic refresh

### Policy & Quotas

**Semantic-Level Policies:**
- Domain access control (who can access which domain)
- Metric access control (who can query which metrics)
- Row-level security (delegated to Dremio)

**Execution-Level Policies:**
- Row limits per query
- Cost limits per query
- Daily/hourly quota per user
- Concurrent query limits

### Observability & Audit

**Metrics (Prometheus):**
- Query count by user/project/domain
- Query latency (p50, p95, p99)
- Cost per query
- Error rate
- Quota consumption

**Audit Logs:**
- User ID, session ID, timestamp
- Raw query + grounded query
- Generated SQL (hashed for security)
- Dremio job ID
- Result metadata (rows, bytes, runtime)
- Policy decisions
- Errors and rejections

**Tracing:**
- Unique trace ID per request
- Spans for each component
- Correlation with Dremio job IDs

## Integration Points

### ChatGPT Enterprise

**Authentication:**
- Azure AD OAuth2 flow
- JWT token validation

**Endpoint:**
- POST `/analytics/query`
- Request format: `{"query": "Show me revenue by category", "user_id": "...", "session_id": "..."}`
- Response format: ChatGPT-compatible JSON

### AWS Bedrock

**Authentication:**
- AWS Cognito
- IAM role-based access

**Endpoint:**
- POST `/analytics/query`
- Request format: Bedrock-compatible JSON
- Response format: Bedrock agent response format

## Deployment Architecture

### Kubernetes (Helm)

**Components:**
- MCP Server (FastAPI + SSE)
- Metrics Server (Prometheus exporter)
- Redis (caching, quota tracking)
- PostgreSQL (audit logs, configuration)

**Scaling:**
- Horizontal Pod Autoscaler (HPA)
- Target: 70% CPU/memory
- Min replicas: 2
- Max replicas: 10

**Security:**
- Non-root containers
- Read-only filesystem
- TLS/SSL termination at ingress
- Secret management (Kubernetes secrets)
- Network policies

## Configuration

### Environment Variables

```yaml
# Dremio
DREMIO_URI: https://api.dremio.cloud
DREMIO_PROJECT_ID: <uuid>

# Authentication
AUTH_PROVIDER: azure_ad  # azure_ad, cognito, oauth2
AZURE_AD_TENANT_ID: <tenant>
AZURE_AD_CLIENT_ID: <client>

# LLM for SQL Generation
LLM_PROVIDER: anthropic  # anthropic, openai, bedrock
LLM_API_KEY: <key>

# Safety Limits
SAFETY_MAX_ROWS: 1000000
SAFETY_MAX_COST_DCU: 100
QUOTA_DAILY_DCU: 1000

# Fuzzy Matching
FUZZY_THRESHOLD: 0.8

# Observability
LOG_LEVEL: INFO
ENABLE_AUDIT_LOGS: true
TRACES_ENABLED: true
```

## Performance Targets

| Query Type | Target Latency | Typical Latency |
|-----------|---------------|-----------------|
| Simple "what" | < 2s | ~1.5s |
| Multi-domain "what" | < 3s | ~2.4s |
| "Why" diagnostic | < 5s | ~4.1s |

## Security Layers

1. **Azure AD / Cognito** - User authentication
2. **Semantic Policy** - Domain/metric access
3. **AST Validation** - SQL injection prevention
4. **Safety Gate** - Resource limit enforcement
5. **Dremio RBAC** - Table/row/column security

## Future Enhancements

- WebSocket support for real-time updates
- Query result caching (Redis)
- Multi-language support
- Custom visualization plugins
- Advanced diagnostics recipes
- Machine learning-based anomaly detection
- Natural language explanations
