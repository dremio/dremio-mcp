# Dremio MCP Helm Chart - Quick Start Guide

This guide will help you quickly deploy the Dremio MCP Server on Kubernetes.

⚠️ **Important**: For production deployments, use OAuth + External Token Provider authentication instead of PAT. See [AUTHENTICATION.md](AUTHENTICATION.md).

## Prerequisites

1. **Kubernetes cluster** (1.19+)
2. **Helm** (3.2.0+)
3. **kubectl** configured to access your cluster
4. **Docker** for building the image
5. **Dremio Software instance**
6. **For Production**: OAuth 2.0 Identity Provider configured with Dremio External Token Provider
7. **For Development/Testing**: Dremio Personal Access Token (PAT)

## Step 1: Build the Docker Image

From the repository root:

```bash
# Build the image
docker build -t dremio-mcp:0.1.0 .

# If using a private registry, tag and push
docker tag dremio-mcp:0.1.0 <your-registry>/dremio-mcp:0.1.0
docker push <your-registry>/dremio-mcp:0.1.0
```

## Step 2: Choose Your Deployment Type

### Option A: Production (OAuth - Recommended)

For production deployments, deploy without PAT:

```bash
# Create a namespace
kubectl create namespace dremio

# Install with OAuth configuration (no PAT)
helm install dremio-mcp ./helm/dremio-mcp \
  --set dremio.uri=https://dremio.example.com:9047 \
  -n dremio
```

Then configure OAuth in your chat frontend. See [AUTHENTICATION.md](AUTHENTICATION.md) for details.

### Option B: Development/Testing (PAT)

⚠️ **For development and testing only - NOT for production**

```bash
# Install with PAT directly (for quick testing)
helm install dremio-mcp ./helm/dremio-mcp \
  --set dremio.uri=https://dremio.example.com:9047 \
  --set dremio.pat=your-personal-access-token \
  -n dremio
```

## Step 3: Verify the Deployment

```bash
# Check pod status
kubectl get pods -n dremio

# View logs
kubectl logs -f -l app.kubernetes.io/name=dremio-mcp -n dremio

# Check service
kubectl get svc -n dremio
```

## Step 4: Access the MCP Server

### Port Forward (for testing):

```bash
kubectl port-forward svc/dremio-mcp 8000:8000 -n dremio
```

Then access at `http://localhost:8000`

### Using Ingress (for production):

```bash
helm upgrade dremio-mcp ./helm/dremio-mcp \
  --set dremio.uri=https://dremio.example.com:9047 \
  --set ingress.enabled=true \
  --set ingress.className=nginx \
  --set ingress.hosts[0].host=dremio-mcp.example.com \
  --set ingress.hosts[0].paths[0].path=/ \
  --set ingress.hosts[0].paths[0].pathType=Prefix \
  -n dremio
```

## Common Commands

### Upgrade the deployment:

```bash
helm upgrade dremio-mcp ./helm/dremio-mcp \
  --set dremio.uri=https://dremio.example.com:9047 \
  -n dremio
```

### Rollback to previous version:

```bash
helm rollback dremio-mcp -n dremio
```

### Uninstall:

```bash
helm uninstall dremio-mcp -n dremio
```

### View current values:

```bash
helm get values dremio-mcp -n dremio
```

### View all resources:

```bash
helm get manifest dremio-mcp -n dremio
```

## Troubleshooting

### Pod not starting:

```bash
# Describe the pod
kubectl describe pod -l app.kubernetes.io/name=dremio-mcp -n dremio

# Check events
kubectl get events -n dremio --sort-by='.lastTimestamp'
```

### Check logs:

```bash
# Follow logs
kubectl logs -f -l app.kubernetes.io/name=dremio-mcp -n dremio

# Get logs from previous container (if crashed)
kubectl logs -l app.kubernetes.io/name=dremio-mcp -n dremio --previous
```

### Test connectivity to Dremio:

```bash
# Get a shell in the pod
kubectl exec -it <pod-name> -n dremio -- /bin/sh

# Test connection (if curl is available)
curl -v https://dremio.example.com:9047
```

## Advanced Configuration

### Enable Metrics:

```bash
helm upgrade dremio-mcp ./helm/dremio-mcp \
  --set dremio.uri=https://dremio.example.com:9047 \
  --set metrics.enabled=true \
  --set metrics.port=9091 \
  --set service.annotations."prometheus\.io/scrape"=true \
  --set service.annotations."prometheus\.io/port"=9091 \
  --set service.annotations."prometheus\.io/path"=/metrics \
  -n dremio
```

### Enable Autoscaling:

```bash
helm upgrade dremio-mcp ./helm/dremio-mcp \
  --set dremio.uri=https://dremio.example.com:9047 \
  --set autoscaling.enabled=true \
  --set autoscaling.minReplicas=2 \
  --set autoscaling.maxReplicas=10 \
  --set autoscaling.targetCPUUtilizationPercentage=80 \
  -n dremio
```

## Next Steps

- Review the full [README.md](README.md) for detailed configuration options
- Check the [examples](examples/) directory for more configuration examples
- Configure ingress and TLS for production use
- Set up monitoring and alerting
- Configure autoscaling based on your workload

## Getting Help

- Check the logs: `kubectl logs -f -l app.kubernetes.io/name=dremio-mcp -n dremio`
- Review Helm chart status: `helm status dremio-mcp -n dremio`
- Describe resources: `kubectl describe all -n dremio`

