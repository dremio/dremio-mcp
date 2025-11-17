# ChatGPT Enterprise Integration Guide

This guide shows how to integrate the Dremio MCP Server with ChatGPT Enterprise.

## Prerequisites

1. ChatGPT Enterprise account with admin access
2. Azure AD tenant (for authentication)
3. Dremio Cloud or Enterprise instance
4. Kubernetes cluster or cloud hosting for the MCP server

## Step-by-Step Setup

### 1. Deploy Dremio MCP Server

#### Option A: Kubernetes (Recommended for Production)

```bash
# Create namespace
kubectl create namespace dremio-mcp

# Create Azure AD secret
kubectl create secret generic azure-ad-secret \
  --from-literal=client-secret=YOUR_AZURE_AD_CLIENT_SECRET \
  -n dremio-mcp

# Create Dremio PAT secret
kubectl create secret generic dremio-secret \
  --from-literal=pat=YOUR_DREMIO_PAT \
  -n dremio-mcp

# Install Helm chart
helm install dremio-mcp ./helm/dremio-mcp \
  --namespace dremio-mcp \
  --values examples/values-chatgpt.yaml
```

**values-chatgpt.yaml**:
```yaml
dremio:
  uri: https://api.dremio.cloud
  projectId: your-project-id
  patRef:
    name: dremio-secret
    key: pat

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
    tenantId: your-tenant-id
    clientId: your-client-id
    clientSecretRef:
      name: azure-ad-secret
      key: client-secret

ingress:
  enabled: true
  className: nginx
  hostname: dremio-mcp.yourcompany.com
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
  tls:
    enabled: true
    secretName: dremio-mcp-tls

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

metrics:
  enabled: true
  serviceMonitor:
    enabled: true
```

#### Option B: Docker (Development/Testing)

```bash
docker run -d \
  --name dremio-mcp \
  -p 8000:8000 \
  -e DREMIO_URI=https://api.dremio.cloud \
  -e DREMIO_PROJECT_ID=your-project-id \
  -e DREMIO_PAT=your-pat \
  -e AUTH_PROVIDER=azure_ad \
  -e AZURE_AD_TENANT_ID=your-tenant-id \
  -e AZURE_AD_CLIENT_ID=your-client-id \
  -e AZURE_AD_CLIENT_SECRET=your-secret \
  dremio-mcp:latest \
  dremio-mcp-server run --transport streamable-http --port 8000
```

### 2. Configure Azure AD

#### Register Application

1. Go to Azure Portal → Azure Active Directory → App registrations
2. Click "New registration"
3. Name: "Dremio MCP for ChatGPT"
4. Supported account types: Single tenant
5. Redirect URI: Leave blank (will be configured later)
6. Click "Register"

#### Configure API Permissions

1. Go to "API permissions"
2. Add permission → Microsoft Graph → Delegated permissions
3. Select: `User.Read`, `openid`, `profile`, `email`
4. Click "Add permissions"
5. Click "Grant admin consent"

#### Create Client Secret

1. Go to "Certificates & secrets"
2. Click "New client secret"
3. Description: "ChatGPT Integration"
4. Expires: 12 months (or as per policy)
5. Click "Add"
6. **Copy the secret value** (you won't see it again)

#### Note Configuration Values

You'll need:
- **Tenant ID**: From "Overview" page
- **Client ID**: Application (client) ID from "Overview"
- **Client Secret**: The value you just copied

### 3. Configure ChatGPT Enterprise

#### Create Custom Action

1. Go to ChatGPT Enterprise Admin Console
2. Navigate to "Actions" or "Integrations"
3. Click "Create new action"
4. Name: "Dremio Analytics"
5. Description: "Query Dremio data with natural language"

#### Authentication Setup

```json
{
  "type": "oauth2",
  "oauth2": {
    "authorization_url": "https://login.microsoftonline.com/YOUR_TENANT_ID/oauth2/v2.0/authorize",
    "token_url": "https://login.microsoftonline.com/YOUR_TENANT_ID/oauth2/v2.0/token",
    "client_id": "YOUR_CLIENT_ID",
    "client_secret": "YOUR_CLIENT_SECRET",
    "scope": "openid profile email"
  }
}
```

#### API Configuration

**Base URL**: `https://dremio-mcp.yourcompany.com`

**Endpoint**: `/analytics/chatgpt/query`

**Method**: POST

**Headers**:
```json
{
  "Content-Type": "application/json",
  "X-OpenAI-User-ID": "{{user.email}}"
}
```

**Request Schema**:
```json
{
  "type": "object",
  "properties": {
    "query": {
      "type": "string",
      "description": "Natural language query about data"
    }
  },
  "required": ["query"]
}
```

**Response Schema**:
```json
{
  "type": "object",
  "properties": {
    "content": {
      "type": "object",
      "properties": {
        "narrative": {"type": "string"},
        "data": {"type": "array"},
        "visualization": {"type": "object"}
      }
    },
    "metadata": {
      "type": "object",
      "properties": {
        "sql": {"type": "string"},
        "runtime_ms": {"type": "integer"},
        "trace_id": {"type": "string"}
      }
    }
  }
}
```

### 4. Test the Integration

#### Test from ChatGPT

1. Open ChatGPT Enterprise
2. Type: `@Dremio Analytics Show me revenue by product category`
3. You should see:
   - A natural language response
   - A bar chart visualization
   - The underlying SQL (if you ask for it)

#### Example Queries

**Descriptive Queries**:
```
@Dremio Analytics Show me top 10 customers by revenue
@Dremio Analytics What are monthly sales trends for 2024?
@Dremio Analytics Display profit margin by region
@Dremio Analytics Get order volume by channel and segment
```

**Diagnostic Queries**:
```
@Dremio Analytics Why did revenue drop last month?
@Dremio Analytics What caused the profit increase in Q3?
@Dremio Analytics Explain the sales decline in APAC region
```

**Comparison Queries**:
```
@Dremio Analytics Compare Q1 to Q2 sales
@Dremio Analytics Show year-over-year growth by product
@Dremio Analytics Revenue this quarter vs last quarter
```

### 5. Configure User Access

#### Azure AD Groups (Optional but Recommended)

1. Create AD group: "Dremio Analytics Users"
2. Add users to group
3. Update Azure AD app to require group assignment
4. Configure Dremio to map AD groups to roles

#### Dremio RBAC

1. In Dremio, create roles matching your AD groups
2. Grant appropriate permissions:
   ```sql
   GRANT SELECT ON sales TO ROLE "analytics-users";
   GRANT SELECT ON commerce TO ROLE "analytics-users";
   ```

### 6. Monitoring

#### Check Health

```bash
curl https://dremio-mcp.yourcompany.com/analytics/health
```

Expected response:
```json
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

#### View Metrics

```bash
curl https://dremio-mcp.yourcompany.com:9091/metrics
```

#### Check Logs

```bash
kubectl logs -n dremio-mcp -l app=dremio-mcp --tail=100 -f
```

### 7. Customize Semantic Layer

#### Update Value Dictionary

Edit `src/dremioai/analytics/planner.py`:

```python
VALUE_DICTIONARY = {
    # Add your custom mappings
    "revenue": ["revenue", "sales", "bookings", "ARR", "income"],
    "customers": ["customers", "accounts", "clients", "organizations"],
    "product_category": ["category", "product_type", "vertical", "segment"],
    # ... more mappings
}
```

#### Update Metric Definitions

```python
METRIC_DEFINITIONS = {
    "revenue": {
        "definition": "SUM(order_amount)",
        "table": "sales.orders",
        "column": "order_amount",
        "aggregation": "SUM",
    },
    "arr": {
        "definition": "SUM(annual_contract_value)",
        "table": "sales.subscriptions",
        "column": "annual_contract_value",
        "aggregation": "SUM",
    },
    # ... more metrics
}
```

## Advanced Configuration

### Enable LLM-based SQL Generation

For complex queries, enable LLM-based SQL generation:

```yaml
# values-chatgpt.yaml
analytics:
  enabled: true
  useLLM: true
  llmProvider: anthropic  # or openai, bedrock

llm:
  anthropic:
    apiKey: your-api-key
  # or
  openai:
    apiKey: your-api-key
```

### Configure Custom Limits

```yaml
analytics:
  fuzzyThreshold: 0.85  # Higher = stricter matching
  maxRows: 5000000      # Max rows per query
  maxCostDCU: 500       # Max cost per query
  quotaDailyDCU: 10000  # User daily quota
```

### Schema Allowlist

```yaml
analytics:
  schemaAllowlist:
    - sales
    - commerce
    - customer
    - finance
    - marketing
    # Don't include: internal, sys$, test
```

## Troubleshooting

### Issue: "401 Unauthorized"

**Cause**: Azure AD authentication failure

**Solutions**:
1. Verify tenant ID, client ID, client secret
2. Check Azure AD app permissions granted
3. Ensure user is in allowed groups
4. Check token expiry

```bash
# Test authentication
curl -X POST https://dremio-mcp.yourcompany.com/analytics/chatgpt/query \
  -H "Authorization: Bearer YOUR_AZURE_TOKEN" \
  -H "X-OpenAI-User-ID: user@company.com" \
  -d '{"query": "test"}'
```

### Issue: "Could not match term"

**Cause**: Fuzzy matching threshold too high

**Solutions**:
1. Add synonym to value dictionary
2. Lower fuzzy threshold
3. Use more standard terminology

### Issue: "Safety gate rejection"

**Cause**: Query exceeds limits

**Solutions**:
1. Add date filters to reduce row count
2. Request limit increase
3. Optimize query with more specific filters

### Issue: "Slow query performance"

**Cause**: No Dremio reflection used

**Solutions**:
1. Create reflections on frequently queried data
2. Check EXPLAIN output
3. Optimize table partitioning

## Best Practices

1. **Start with limited schemas**: Only expose necessary data
2. **Set conservative limits initially**: Increase as needed
3. **Monitor costs closely**: Track DCU usage
4. **Use reflections**: Pre-aggregate common queries
5. **Enable audit logging**: Track all queries
6. **Regular security reviews**: Audit user access
7. **Train users on query patterns**: Provide examples

## Example User Guide

Share this with your ChatGPT users:

```markdown
# Dremio Analytics in ChatGPT

## How to Use

Simply type `@Dremio Analytics` followed by your question.

## Examples

**Show me data**: "Show me revenue by product"
**Why questions**: "Why did sales drop last month?"
**Comparisons**: "Compare Q1 to Q2"
**Trends**: "What are the trends in customer growth?"

## Tips

- Be specific about time periods: "last month", "Q3 2024"
- Use business terms: "revenue", "customers", "products"
- Ask follow-up questions for deeper analysis
- Request to see the SQL if you're curious how it works
```

## Security Checklist

- [ ] Azure AD properly configured
- [ ] Client secret stored in Kubernetes secret
- [ ] TLS/SSL enabled on ingress
- [ ] Network policies configured
- [ ] Schema allowlist in place
- [ ] Row/cost limits configured
- [ ] User quotas enabled
- [ ] Audit logging enabled
- [ ] Monitoring alerts configured
- [ ] Regular security reviews scheduled
