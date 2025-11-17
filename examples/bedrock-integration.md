# AWS Bedrock Integration Guide

This guide shows how to integrate the Dremio MCP Server with AWS Bedrock Agents.

## Prerequisites

1. AWS Account with Bedrock access
2. Dremio Cloud or Enterprise instance
3. AWS ECS/EKS cluster or Lambda for hosting
4. IAM permissions for Bedrock, Lambda, and API Gateway

## Architecture Overview

```
User Query
    ↓
Amazon Bedrock Agent
    ↓
Lambda Function (optional)
    ↓
Dremio MCP Server (ECS/EKS)
    ↓
Dremio Cloud/Enterprise
```

## Step-by-Step Setup

### 1. Deploy Dremio MCP Server on AWS

#### Option A: ECS Fargate (Recommended for Production)

```bash
# Build and push Docker image to ECR
aws ecr create-repository --repository-name dremio-mcp

# Tag and push
docker build -t dremio-mcp:latest .
docker tag dremio-mcp:latest YOUR_ACCOUNT.dkr.ecr.us-east-1.amazonaws.com/dremio-mcp:latest
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin YOUR_ACCOUNT.dkr.ecr.us-east-1.amazonaws.com
docker push YOUR_ACCOUNT.dkr.ecr.us-east-1.amazonaws.com/dremio-mcp:latest
```

**ECS Task Definition** (`task-definition.json`):
```json
{
  "family": "dremio-mcp",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "1024",
  "memory": "2048",
  "containerDefinitions": [
    {
      "name": "dremio-mcp",
      "image": "YOUR_ACCOUNT.dkr.ecr.us-east-1.amazonaws.com/dremio-mcp:latest",
      "portMappings": [
        {
          "containerPort": 8000,
          "protocol": "tcp"
        }
      ],
      "environment": [
        {
          "name": "DREMIO_URI",
          "value": "https://api.dremio.cloud"
        },
        {
          "name": "DREMIO_PROJECT_ID",
          "value": "your-project-id"
        },
        {
          "name": "AUTH_PROVIDER",
          "value": "aws_cognito"
        }
      ],
      "secrets": [
        {
          "name": "DREMIO_PAT",
          "valueFrom": "arn:aws:secretsmanager:us-east-1:ACCOUNT:secret:dremio-pat"
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/dremio-mcp",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "ecs"
        }
      },
      "command": [
        "dremio-mcp-server",
        "run",
        "--transport",
        "streamable-http",
        "--port",
        "8000"
      ]
    }
  ]
}
```

**Create ECS Service**:
```bash
# Create cluster
aws ecs create-cluster --cluster-name dremio-mcp

# Register task definition
aws ecs register-task-definition --cli-input-json file://task-definition.json

# Create service
aws ecs create-service \
  --cluster dremio-mcp \
  --service-name dremio-mcp \
  --task-definition dremio-mcp \
  --desired-count 2 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-xxx],securityGroups=[sg-xxx],assignPublicIp=ENABLED}" \
  --load-balancers "targetGroupArn=arn:aws:elasticloadbalancing:...,containerName=dremio-mcp,containerPort=8000"
```

#### Option B: EKS (Kubernetes)

```bash
# Use the Helm chart
helm install dremio-mcp ./helm/dremio-mcp \
  --namespace dremio-mcp \
  --create-namespace \
  --values examples/values-bedrock.yaml
```

**values-bedrock.yaml**:
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
  provider: aws_cognito
  awsCognito:
    region: us-east-1
    userPoolId: us-east-1_XXXXXX
    appClientId: your-app-client-id

service:
  type: LoadBalancer
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-type: "nlb"
    service.beta.kubernetes.io/aws-load-balancer-internal: "true"

autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70
```

### 2. Configure AWS Cognito (Authentication)

#### Create User Pool

```bash
# Create user pool
aws cognito-idp create-user-pool \
  --pool-name dremio-mcp-users \
  --policies "PasswordPolicy={MinimumLength=8,RequireUppercase=true,RequireLowercase=true,RequireNumbers=true}" \
  --auto-verified-attributes email

# Create app client
aws cognito-idp create-user-pool-client \
  --user-pool-id us-east-1_XXXXXX \
  --client-name dremio-mcp-bedrock \
  --generate-secret \
  --explicit-auth-flows ALLOW_USER_PASSWORD_AUTH ALLOW_REFRESH_TOKEN_AUTH
```

#### Create Users/Groups

```bash
# Create admin group
aws cognito-idp create-group \
  --group-name analytics-users \
  --user-pool-id us-east-1_XXXXXX \
  --description "Users with access to Dremio analytics"

# Create user
aws cognito-idp admin-create-user \
  --user-pool-id us-east-1_XXXXXX \
  --username user@company.com \
  --user-attributes Name=email,Value=user@company.com Name=email_verified,Value=true

# Add user to group
aws cognito-idp admin-add-user-to-group \
  --user-pool-id us-east-1_XXXXXX \
  --username user@company.com \
  --group-name analytics-users
```

### 3. Create API Gateway (Optional for Lambda)

If using Lambda as a proxy:

```bash
# Create REST API
aws apigateway create-rest-api \
  --name dremio-mcp-api \
  --description "Dremio MCP Analytics API" \
  --endpoint-configuration types=REGIONAL
```

**OpenAPI Specification** (`openapi.yaml`):
```yaml
openapi: 3.0.0
info:
  title: Dremio Analytics API
  version: 1.0.0
  description: Analytics API for AWS Bedrock integration
paths:
  /analytics/bedrock/action:
    post:
      summary: Execute analytics query
      description: Process natural language query and return results
      operationId: queryAnalytics
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              properties:
                messageVersion:
                  type: string
                  example: "1.0"
                agent:
                  type: object
                  properties:
                    userId:
                      type: string
                actionGroup:
                  type: string
                function:
                  type: string
                parameters:
                  type: array
                  items:
                    type: object
                    properties:
                      name:
                        type: string
                      value:
                        type: string
      responses:
        '200':
          description: Successful response
          content:
            application/json:
              schema:
                type: object
                properties:
                  messageVersion:
                    type: string
                  response:
                    type: object
      x-amazon-apigateway-integration:
        type: aws_proxy
        httpMethod: POST
        uri: arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:ACCOUNT:function:dremio-mcp-proxy/invocations
```

### 4. Create Lambda Function (Optional Proxy)

**Lambda Function** (`lambda_function.py`):
```python
import json
import os
import requests
from typing import Dict, Any

DREMIO_MCP_URL = os.environ['DREMIO_MCP_URL']

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda proxy for Bedrock Agent to Dremio MCP Server.

    This function forwards Bedrock agent requests to the Dremio MCP server
    and returns properly formatted responses.
    """

    # Parse request
    try:
        body = json.loads(event.get('body', '{}')) if isinstance(event.get('body'), str) else event

        # Log request
        print(f"Received request: {json.dumps(body)}")

        # Forward to Dremio MCP server
        response = requests.post(
            f"{DREMIO_MCP_URL}/analytics/bedrock/action",
            json=body,
            timeout=30
        )

        # Return response
        result = response.json()
        print(f"Response: {json.dumps(result)}")

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps(result)
        }

    except Exception as e:
        print(f"Error: {str(e)}")

        # Return error in Bedrock format
        return {
            'statusCode': 200,  # Still 200 for Bedrock
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                "messageVersion": "1.0",
                "response": {
                    "actionGroup": body.get('actionGroup', 'dremio_analytics'),
                    "function": body.get('function', 'query_data'),
                    "functionResponse": {
                        "responseState": "FAILURE",
                        "responseBody": {
                            "TEXT": {
                                "body": f"Error processing query: {str(e)}"
                            }
                        }
                    }
                }
            })
        }

```

**Deploy Lambda**:
```bash
# Create deployment package
zip -r function.zip lambda_function.py
pip install requests -t .
zip -r function.zip .

# Create Lambda function
aws lambda create-function \
  --function-name dremio-mcp-proxy \
  --runtime python3.11 \
  --role arn:aws:iam::ACCOUNT:role/lambda-execution-role \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://function.zip \
  --timeout 30 \
  --memory-size 512 \
  --environment Variables={DREMIO_MCP_URL=http://dremio-mcp-lb.us-east-1.elb.amazonaws.com}
```

### 5. Create Bedrock Agent

#### Configure Agent

```bash
# Create agent
aws bedrock-agent create-agent \
  --agent-name dremio-analytics \
  --description "Query Dremio data with natural language" \
  --foundation-model anthropic.claude-v2 \
  --instruction "You are a data analytics assistant. Help users query and analyze their business data using natural language. When users ask about data, use the dremio_analytics action group to execute queries."
```

#### Create Action Group

**Action Group Configuration** (`action-group.json`):
```json
{
  "actionGroupName": "dremio_analytics",
  "description": "Execute analytics queries on Dremio data",
  "actionGroupExecutor": {
    "lambda": "arn:aws:lambda:us-east-1:ACCOUNT:function:dremio-mcp-proxy"
  },
  "apiSchema": {
    "s3": {
      "s3BucketName": "your-bucket",
      "s3ObjectKey": "openapi.yaml"
    }
  }
}
```

```bash
# Upload OpenAPI schema to S3
aws s3 cp openapi.yaml s3://your-bucket/openapi.yaml

# Create action group
aws bedrock-agent create-agent-action-group \
  --agent-id AGENT_ID \
  --agent-version DRAFT \
  --cli-input-json file://action-group.json

# Prepare agent
aws bedrock-agent prepare-agent --agent-id AGENT_ID

# Create alias
aws bedrock-agent create-agent-alias \
  --agent-id AGENT_ID \
  --agent-alias-name production \
  --description "Production version"
```

### 6. Test the Integration

#### Test via AWS Console

1. Go to Amazon Bedrock Console
2. Navigate to Agents
3. Select "dremio-analytics" agent
4. Click "Test"

**Test Queries**:
```
Show me revenue by product category
Why did sales drop last month?
Compare Q1 to Q2 performance
What are the top 10 customers by revenue?
```

#### Test via API

```python
import boto3
import json

bedrock_agent_runtime = boto3.client('bedrock-agent-runtime')

response = bedrock_agent_runtime.invoke_agent(
    agentId='AGENT_ID',
    agentAliasId='ALIAS_ID',
    sessionId='test-session-1',
    inputText='Show me revenue by product category'
)

# Print response
for event in response['completion']:
    print(event)
```

#### Test Lambda Directly

```bash
aws lambda invoke \
  --function-name dremio-mcp-proxy \
  --payload file://test-payload.json \
  response.json

cat response.json
```

**test-payload.json**:
```json
{
  "messageVersion": "1.0",
  "agent": {
    "userId": "test-user"
  },
  "actionGroup": "dremio_analytics",
  "function": "query_data",
  "parameters": [
    {
      "name": "query",
      "value": "Show me revenue by category"
    }
  ]
}
```

### 7. Configure IAM Permissions

#### Lambda Execution Role

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:us-east-1:ACCOUNT:secret:dremio-*"
    }
  ]
}
```

#### Bedrock Agent Role

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel"
      ],
      "Resource": "arn:aws:bedrock:*::foundation-model/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction"
      ],
      "Resource": "arn:aws:lambda:us-east-1:ACCOUNT:function:dremio-mcp-proxy"
    }
  ]
}
```

## Monitoring & Troubleshooting

### CloudWatch Logs

```bash
# View Lambda logs
aws logs tail /aws/lambda/dremio-mcp-proxy --follow

# View ECS logs
aws logs tail /ecs/dremio-mcp --follow

# Filter for errors
aws logs filter-log-events \
  --log-group-name /aws/lambda/dremio-mcp-proxy \
  --filter-pattern "ERROR"
```

### Metrics

```bash
# Lambda metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Duration \
  --dimensions Name=FunctionName,Value=dremio-mcp-proxy \
  --start-time 2024-01-01T00:00:00Z \
  --end-time 2024-01-02T00:00:00Z \
  --period 3600 \
  --statistics Average
```

### Common Issues

#### Issue: "Lambda timeout"

**Solution**: Increase timeout
```bash
aws lambda update-function-configuration \
  --function-name dremio-mcp-proxy \
  --timeout 60
```

#### Issue: "Connection refused"

**Solution**: Check security groups
```bash
# Ensure Lambda can reach ECS/EKS
# Add Lambda security group to ECS/EKS ingress rules
```

#### Issue: "Authentication failed"

**Solution**: Verify Cognito configuration
```bash
# Test Cognito authentication
aws cognito-idp initiate-auth \
  --auth-flow USER_PASSWORD_AUTH \
  --client-id YOUR_CLIENT_ID \
  --auth-parameters USERNAME=user@company.com,PASSWORD=password
```

## Cost Optimization

### Use Lambda with Reserved Concurrency

```bash
aws lambda put-function-concurrency \
  --function-name dremio-mcp-proxy \
  --reserved-concurrent-executions 10
```

### ECS Fargate Spot

```json
{
  "capacityProviders": ["FARGATE_SPOT"],
  "defaultCapacityProviderStrategy": [
    {
      "capacityProvider": "FARGATE_SPOT",
      "weight": 1
    }
  ]
}
```

### Enable ECS Auto Scaling

```bash
aws application-autoscaling register-scalable-target \
  --service-namespace ecs \
  --resource-id service/dremio-mcp/dremio-mcp \
  --scalable-dimension ecs:service:DesiredCount \
  --min-capacity 2 \
  --max-capacity 10
```

## Security Best Practices

1. **Use VPC Endpoints**: Private connectivity to AWS services
2. **Enable Encryption**: ECS task encryption, S3 bucket encryption
3. **Rotate Secrets**: Regular rotation of Dremio PAT and Cognito secrets
4. **Least Privilege IAM**: Minimal permissions for all roles
5. **Network Isolation**: Private subnets for ECS/Lambda
6. **WAF**: Add AWS WAF if exposing via API Gateway
7. **CloudTrail**: Enable logging for all API calls

## Example User Guide

```markdown
# Dremio Analytics via Bedrock

## How to Use

Open Amazon Bedrock Chat and ask questions about your data naturally.

## Examples

**Descriptive**:
- "Show me revenue by product category"
- "What are the top 10 customers?"
- "Display monthly sales trends"

**Diagnostic**:
- "Why did revenue drop last month?"
- "What caused the increase in costs?"

**Comparison**:
- "Compare this quarter to last quarter"
- "Show year-over-year growth"

## Tips

- Be specific about time periods
- Use your company's business terms
- Ask follow-up questions for deeper insights
```

## Terraform Example

```hcl
resource "aws_bedrock_agent" "dremio_analytics" {
  agent_name              = "dremio-analytics"
  description             = "Query Dremio data with natural language"
  foundation_model        = "anthropic.claude-v2"
  instruction             = "You are a data analytics assistant..."
  agent_resource_role_arn = aws_iam_role.bedrock_agent.arn
}

resource "aws_bedrock_agent_action_group" "dremio" {
  agent_id      = aws_bedrock_agent.dremio_analytics.agent_id
  agent_version = "DRAFT"

  action_group_name = "dremio_analytics"
  description       = "Execute analytics queries"

  action_group_executor {
    lambda = aws_lambda_function.dremio_proxy.arn
  }

  api_schema {
    s3 {
      s3_bucket_name = aws_s3_bucket.openapi.id
      s3_object_key  = "openapi.yaml"
    }
  }
}
```

## Next Steps

1. Configure custom value dictionary for your domain
2. Create Dremio reflections for common queries
3. Set up CloudWatch alarms for errors
4. Implement cost tracking and quotas
5. Train users on query patterns
