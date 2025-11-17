# Enterprise Analytics MCP Server

## Overview

The Dremio MCP Server now includes production-grade enterprise analytics capabilities compatible with **ChatGPT Enterprise** and **AWS Bedrock**. This document describes the new features and integration points.

## Architecture

The server implements a complete analytics pipeline:

```
Natural Language Query
    ↓
┌─────────────────────────────────────────────────────────┐
│  Resolver: Intent Classification                       │
│  - what: Descriptive queries                           │
│  - why: Diagnostic/root cause analysis                 │
│  - compare: Comparison queries                         │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│  Planner & Grounder: Fuzzy Matching                    │
│  - Maps user terms → canonical terms (0.8 threshold)   │
│  - Validates join paths                                │
│  - Checks access policies                              │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│  Compiler & Validator: SQL Generation                  │
│  - LLM-based or rule-based SQL generation              │
│  - AST validation (prevents DDL/DML/injection)         │
│  - Schema allowlist enforcement                        │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│  Safety Gate: Pre-execution Checks                     │
│  - EXPLAIN analysis for cost estimation                │
│  - Row/cost limit enforcement                          │
│  - User quota tracking                                 │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│  Execute: Dremio Query Execution                       │
│  - Async query execution                               │
│  - Result pagination                                   │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│  Results Processor: Formatting & Visualization         │
│  - Auto-selects chart type (bar, line, pie, etc.)      │
│  - Formats for ChatGPT/Bedrock/MCP                     │
│  - Includes metadata and trace IDs                     │
└─────────────────────────────────────────────────────────┘
```

## Key Features

### 1. **Intent Classification**

The system automatically classifies queries:

- **What queries**: "Show me revenue by product category"
  - Generates descriptive analytics
  - Returns data with visualizations

- **Why queries**: "Why did revenue drop last month?"
  - Triggers diagnostic agent
  - Performs root cause analysis
  - Returns waterfall charts with drivers

- **Compare queries**: "Compare Q1 to Q2 sales"
  - Performs period-over-period analysis
  - Highlights variances

### 2. **Fuzzy Matching**

Maps user terminology to canonical terms with configurable threshold (default 0.8):

```
User: "Show me sales by product type"
  ↓
Fuzzy Match:
  "sales" → "revenue" (score: 0.92)
  "product type" → "product_category" (score: 0.95)
  ↓
Canonical: "revenue by product_category"
```

### 3. **Security & Validation**

Multi-layered security:

1. **Authentication**: OAuth2, Bearer token, Azure AD, AWS Cognito
2. **AST Validation**: Prevents SQL injection, blocks DDL/DML
3. **Schema Allowlist**: Restricts data access
4. **Safety Gate**: Pre-execution cost/row limits
5. **Dremio RBAC**: Row/column-level security

### 4. **Automatic Visualization**

Intelligently selects chart types based on data shape:

| Data Pattern | Visualization |
|--------------|---------------|
| 1 categorical + 1 metric | Bar chart |
| Date + metric(s) | Line chart |
| Multiple metrics over time | Multi-series chart |
| Low cardinality categorical | Pie chart |
| 2 categorical + 1 metric | Heatmap |
| "Why" query results | Waterfall chart |

### 5. **Diagnostics Agent**

Automated root cause analysis for "why" queries:

- Compares baseline vs current period
- Decomposes variance by dimensions
- Checks event tables (promotions, stockouts, pricing)
- Ranks drivers by impact
- Calculates confidence score
- Generates natural language narrative

## Integration

### ChatGPT Enterprise

#### 1. Configure Azure AD Authentication

```yaml
# config.yaml
auth_provider: azure_ad
azure_ad:
  tenant_id: your-tenant-id
  client_id: your-client-id
  client_secret: your-client-secret
```

#### 2. Register Webhook in ChatGPT Enterprise

**Webhook URL**: `https://your-server.com/analytics/chatgpt/query`

**Headers**:
```
Authorization: Bearer {azure_ad_token}
X-OpenAI-User-ID: {user_id}
```

**Request Format**:
```json
{
  "query": "Show me revenue by product category",
  "user_id": "user@company.com",
  "session_id": "session-123"
}
```

**Response Format**:
```json
{
  "content": {
    "type": "analytics_result",
    "narrative": "Revenue by product category",
    "data": [...],
    "visualization": {
      "type": "bar_chart",
      "config": {
        "x": "product_category",
        "y": "revenue",
        "title": "Revenue by Product Category"
      }
    }
  },
  "metadata": {
    "sql": "SELECT ...",
    "runtime_ms": 847,
    "trace_id": "trace-xyz"
  }
}
```

#### 3. Example ChatGPT Prompts

```
User: "Show me our top 5 products by revenue this quarter"
→ Returns bar chart with top 5 products

User: "Why did profit drop in October?"
→ Returns diagnostic waterfall chart with drivers

User: "Compare regional sales for Q3 vs Q4"
→ Returns multi-series comparison chart
```

### AWS Bedrock

#### 1. Configure AWS Cognito Authentication

```yaml
# config.yaml
auth_provider: aws_cognito
aws_cognito:
  region: us-east-1
  user_pool_id: your-pool-id
  app_client_id: your-client-id
```

#### 2. Create Bedrock Agent Action Group

**API Schema** (`openapi.yaml`):
```yaml
openapi: 3.0.0
info:
  title: Dremio Analytics API
  version: 1.0.0
paths:
  /analytics/bedrock/action:
    post:
      summary: Execute analytics query
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                query:
                  type: string
                  description: Natural language query
      responses:
        '200':
          description: Query results
```

**Lambda Function** (optional, if using Lambda proxy):
```python
import requests

def lambda_handler(event, context):
    # Forward to Dremio MCP server
    response = requests.post(
        "https://your-server.com/analytics/bedrock/action",
        json=event,
        headers={
            "Authorization": f"Bearer {get_token()}"
        }
    )
    return response.json()
```

#### 3. Bedrock Request/Response Format

**Request**:
```json
{
  "messageVersion": "1.0",
  "agent": {
    "userId": "user-123"
  },
  "actionGroup": "dremio_analytics",
  "function": "query_data",
  "parameters": [
    {
      "name": "query",
      "value": "Show me revenue trends for last 6 months"
    }
  ]
}
```

**Response**:
```json
{
  "messageVersion": "1.0",
  "response": {
    "actionGroup": "dremio_analytics",
    "function": "query_data",
    "functionResponse": {
      "responseBody": {
        "TEXT": {
          "body": "Revenue trends for the last 6 months"
        },
        "data": [...],
        "visualization": {
          "type": "line_chart",
          "x_axis": "month",
          "y_axis": "revenue"
        }
      }
    }
  }
}
```

## API Endpoints

### Standard Analytics Endpoint

```bash
POST /analytics/query
Content-Type: application/json
Authorization: Bearer {token}

{
  "query": "Show me revenue by region",
  "user_id": "user@example.com",
  "response_format": "mcp_standard"  # or "chatgpt_enterprise" or "aws_bedrock"
}
```

### ChatGPT-specific Endpoint

```bash
POST /analytics/chatgpt/query
Content-Type: application/json
Authorization: Bearer {azure_ad_token}
X-OpenAI-User-ID: user@example.com

{
  "query": "Why did sales drop last month?"
}
```

### Bedrock-specific Endpoint

```bash
POST /analytics/bedrock/action
Content-Type: application/json

{
  "messageVersion": "1.0",
  "agent": {"userId": "user-123"},
  "actionGroup": "dremio_analytics",
  "function": "query_data",
  "parameters": [
    {"name": "query", "value": "Compare Q1 to Q2"}
  ]
}
```

### Health Check

```bash
GET /analytics/health

Response:
{
  "status": "healthy",
  "version": "1.0.0",
  "components": {
    "resolver": "ok",
    "planner": "ok",
    "compiler": "ok",
    "safety_gate": "ok",
    "diagnostics": "ok",
    "results_processor": "ok"
  }
}
```

## Configuration

### Environment Variables

```bash
# Dremio Connection
DREMIO_URI=https://api.dremio.cloud
DREMIO_PROJECT_ID=your-project-id
DREMIO_PAT=@/path/to/token  # or direct token

# Authentication
AUTH_PROVIDER=azure_ad  # or aws_cognito, oauth2
AZURE_AD_TENANT_ID=tenant-id
AZURE_AD_CLIENT_ID=client-id

# Safety Limits
SAFETY_MAX_ROWS=1000000
SAFETY_MAX_COST_DCU=100
QUOTA_DAILY_DCU=1000

# Fuzzy Matching
FUZZY_THRESHOLD=0.8

# LLM for SQL Generation (optional)
LLM_PROVIDER=anthropic  # or openai, bedrock
LLM_API_KEY=your-api-key

# Observability
LOG_LEVEL=INFO
ENABLE_AUDIT_LOGS=true
TRACES_ENABLED=true
```

### Configuration File

```yaml
# ~/.config/dremioai/config.yaml

dremio:
  uri: https://api.dremio.cloud
  project_id: abc-123
  pat: @/path/to/dremio-token

analytics:
  fuzzy_threshold: 0.8
  max_rows: 1000000
  max_cost_dcu: 100.0
  schema_allowlist:
    - sales
    - commerce
    - customer
    - finance

auth:
  provider: azure_ad
  azure_ad:
    tenant_id: your-tenant
    client_id: your-client

observability:
  log_level: INFO
  enable_audit_logs: true
  enable_traces: true
```

## Deployment

### Kubernetes (Helm)

```bash
# Install Helm chart
helm install dremio-mcp ./helm/dremio-mcp \
  --set dremio.uri=https://api.dremio.cloud \
  --set dremio.projectId=your-project-id \
  --set dremio.pat=your-pat \
  --set mcp.enableStreamingHttp=true \
  --set analytics.enabled=true \
  --set ingress.enabled=true \
  --set ingress.hostname=dremio-mcp.company.com
```

**Production Helm Values** (`values-prod.yaml`):
```yaml
dremio:
  uri: https://api.dremio.cloud
  projectId: abc-123
  pat: ""  # Use secret instead

mcp:
  enableStreamingHttp: true
  port: 8000

analytics:
  enabled: true
  fuzzyThreshold: 0.8
  maxRows: 1000000
  maxCostDCU: 100

auth:
  provider: azure_ad
  azureAD:
    tenantId: your-tenant
    clientId: your-client
    clientSecretRef:
      name: azure-ad-secret
      key: client-secret

ingress:
  enabled: true
  hostname: dremio-mcp.company.com
  tls:
    enabled: true
    secretName: tls-cert

autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70

resources:
  limits:
    cpu: 2000m
    memory: 4Gi
  requests:
    cpu: 500m
    memory: 1Gi
```

### Docker

```bash
# Build image
docker build -t dremio-mcp:latest .

# Run container
docker run -d \
  -p 8000:8000 \
  -e DREMIO_URI=https://api.dremio.cloud \
  -e DREMIO_PROJECT_ID=your-project \
  -e DREMIO_PAT=your-token \
  -e AUTH_PROVIDER=azure_ad \
  -e AZURE_AD_TENANT_ID=your-tenant \
  dremio-mcp:latest \
  dremio-mcp-server run --transport streamable-http --port 8000
```

## Query Examples

### What Queries (Descriptive)

```
1. "Show me revenue by product category"
   → Returns: Bar chart with categories and revenue

2. "What are the top 10 customers by order volume?"
   → Returns: Table with customer rankings

3. "Display monthly sales trend for 2024"
   → Returns: Line chart with time series

4. "Get profit margin by region and channel"
   → Returns: Heatmap with 2D breakdown
```

### Why Queries (Diagnostic)

```
1. "Why did revenue drop last month?"
   → Diagnostics Agent analyzes:
     - Period comparison (Oct vs Sep)
     - Dimension decomposition (product, region, channel)
     - Event correlation (promotions, stockouts, pricing)
     - Returns: Waterfall chart with ranked drivers

2. "What caused the profit increase in Q3?"
   → Returns: Diagnostic analysis with contributing factors

3. "Why are orders down in APAC?"
   → Returns: Region-specific root cause analysis
```

### Compare Queries

```
1. "Compare Q1 to Q2 revenue by product"
   → Returns: Multi-series bar chart with comparisons

2. "Show me year-over-year growth by region"
   → Returns: Comparison chart with YoY %

3. "Revenue this month vs last month"
   → Returns: Period comparison visualization
```

## Performance

### Latency Targets

| Query Type | Target | Typical |
|-----------|--------|---------|
| Simple "what" | < 2s | ~1.5s |
| Multi-domain "what" | < 3s | ~2.4s |
| "Why" diagnostic | < 5s | ~4.1s |

### Optimization

- **Reflections**: Automatically uses Dremio reflections when available
- **Query Cost Estimation**: Pre-execution EXPLAIN to prevent expensive queries
- **Result Pagination**: Limits visualization to 1000 rows max
- **Fuzzy Matching Cache**: Caches term matches for performance

## Security Best Practices

1. **Use OAuth2/Azure AD/Cognito**: Never hardcode tokens
2. **Enable TLS**: Always use HTTPS in production
3. **Schema Allowlist**: Restrict access to approved schemas
4. **Row Limits**: Enforce max rows per query
5. **Cost Limits**: Set DCU limits per query and per user
6. **Audit Logging**: Enable comprehensive audit trails
7. **Network Policies**: Use Kubernetes network policies
8. **Secret Management**: Use Kubernetes secrets or AWS Secrets Manager

## Monitoring & Observability

### Metrics (Prometheus)

Available at `:9091/metrics`:

```
# Query count by intent
mcp_analytics_queries_total{intent="what"} 1234
mcp_analytics_queries_total{intent="why"} 456

# Query latency
mcp_analytics_query_duration_seconds{intent="what"} 1.5

# Safety gate rejections
mcp_analytics_rejections_total{reason="row_limit"} 12

# Fuzzy match scores
mcp_analytics_fuzzy_match_score{term="revenue"} 0.95
```

### Audit Logs

Structured JSON logs include:

```json
{
  "event": "analytics_query",
  "timestamp": "2025-01-17T10:30:00Z",
  "trace_id": "trace-xyz",
  "user_id": "user@company.com",
  "query": "Show me revenue by category",
  "intent": "what",
  "grounded_metrics": ["revenue"],
  "grounded_dimensions": ["product_category"],
  "sql_hash": "abc123...",
  "estimated_rows": 45127,
  "estimated_cost_dcu": 5.8,
  "actual_rows": 15,
  "runtime_ms": 847,
  "job_id": "dremio-job-123",
  "approved": true
}
```

### Tracing

Each query gets a unique `trace_id` for end-to-end tracking through:
1. Resolver
2. Planner
3. Compiler
4. Safety Gate
5. Execution
6. Results Processing

## Troubleshooting

### Common Issues

**1. Fuzzy match score below threshold**

```
Error: "Could not match term 'salles' (best match: 0.72 < 0.8)"
Solution: Add synonym to value dictionary or lower fuzzy_threshold
```

**2. Safety gate rejection**

```
Error: "Estimated rows (2M) exceeds limit (1M)"
Solution: Add filters to reduce data volume or increase limit
```

**3. Schema not in allowlist**

```
Error: "Schema 'internal' not in allowlist"
Solution: Add schema to allowlist or use approved schema
```

**4. Authentication failure**

```
Error: "401 Unauthorized"
Solution: Check token validity, Azure AD config, or Cognito settings
```

## Roadmap

- [ ] LLM-based SQL generation (optional, configurable)
- [ ] Advanced caching (Redis) for query results
- [ ] Multi-language support
- [ ] Custom visualization plugins
- [ ] ML-based anomaly detection in diagnostics
- [ ] Natural language explanations
- [ ] WebSocket support for real-time updates
- [ ] Query cost prediction models

## Support

- **Documentation**: `/docs` (Swagger UI)
- **Health Check**: `/analytics/health`
- **Metrics**: `:9091/metrics`
- **Architecture**: `ARCHITECTURE.md`
- **Issues**: GitHub Issues

## License

Apache 2.0 (same as Dremio MCP Server)
