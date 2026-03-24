# Ray Orchestrator

A distributed task orchestrator for docling-jobkit that provides fair task scheduling across multiple users using Ray and Redis.

## Overview

The Ray Orchestrator implements a fair round-robin scheduling algorithm at the task level, ensuring that all users get equal opportunity to have their tasks processed, regardless of queue size. It uses:

- **Ray Core with Persistent Actors**: For distributed computing with warm converter managers
- **Redis**: For state management, task queues, and pub/sub communication
- **Fair Scheduling**: Round-robin task distribution across users
- **Resource Limits**: Configurable per-user concurrent task and queue limits
- **Persistent Converter Managers**: Warm models reused across tasks for efficiency

## Key Features

### 1. Fair Task Scheduling
- **Round-robin at task level**: Each user gets one task processed per dispatch round
- **Per-user queues**: Tasks are organized by user in separate Redis queues
- **No starvation**: Users with large queues don't monopolize resources

### 2. Resource Management
- **Concurrent task limits**: Control how many tasks a user can process simultaneously
- **Optional queue limits**: Limit queue size with optional 429 rejection
- **Document limits**: Optional per-user document processing limits

### 3. Fault Tolerance
- **Automatic retries**: Configurable retry logic for failed tasks
- **Ray Actor recovery**: Dispatcher automatically restarts on failure
- **Redis HA support**: Compatible with Redis Sentinel and Redis Cluster
- **OOM protection**: Handles out-of-memory conditions gracefully

### 4. Scalability
- **Ray Data parallelism**: Documents processed in parallel across Ray actors
- **Lazy task unpacking**: Tasks remain packed until scheduled
- **Distributed processing**: Scales across multiple nodes in Ray cluster

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Application                        │
│              (calls orchestrator.enqueue())                  │
└────────────────────────┬────────────────────────────────────┘
                         │
                ┌────────▼────────┐
                │RayOrchestrator│
                │   (implements    │
                │ BaseOrchestrator)│
                └────────┬────────┘
                         │
                ┌────────▼────────┐
                │  Redis State    │
                │                 │
                │ user:*:tasks    │
                │ task:{id}       │
                │ user:*:limits   │
                │ task:{id}:result│
                └────────┬────────┘
                         │
                ┌────────▼──────────────┐
                │ Fair Task Dispatcher  │
                │   (Ray Actor)         │
                │                       │
                │ Round-robin: Pull 1   │
                │ task per user         │
                └────────┬──────────────┘
                         │
                ┌────────▼──────────────┐
                │ Actor Pool Manager    │
                │ (Round-robin)         │
                └────────┬──────────────┘
                         │
            ┌────────────┴────────────┐
            │                         │
   ┌────────▼────────┐       ┌───────▼────────┐
   │ DocumentProcessor│      │ DocumentProcessor│
   │ Actor (Persistent)│     │ Actor (Persistent)│
   │                 │       │                │
   │ WARM:           │       │ WARM:          │
   │ - Converter Mgr │       │ - Converter Mgr│
   │ - Models Loaded │       │ - Models Loaded│
   │ - Memory Monitor│       │ - Memory Monitor│
   └────────┬────────┘       └───────┬────────┘
            │                        │
            └────────────┬───────────┘
                         │
                ┌────────▼────────┐
                │  Task Results   │
                │                 │
                │  → Redis        │
                └─────────────────┘
```

**Key Improvement**: Persistent actors with warm converter managers eliminate model reload overhead between tasks.

## Installation

### Prerequisites

1. **Ray**: Install Ray with data support
   ```bash
   pip install "ray[data]>=2.0.0"
   ```

2. **Redis**: Install Redis server or use a managed Redis service
   ```bash
   # On macOS
   brew install redis
   brew services start redis
   
   # On Ubuntu
   sudo apt-get install redis-server
   sudo systemctl start redis
   ```

3. **Redis Python Client**: Install async Redis client
   ```bash
   pip install redis
   ```

4. **msgpack**: For efficient result serialization
   ```bash
   pip install msgpack
   ```

## Configuration

### Basic Configuration

```python
from docling_jobkit.orchestrators.ray import (
    RayOrchestrator,
    RayOrchestratorConfig,
)
from docling_jobkit.convert.manager import DoclingConverterManager

# Configure orchestrator
config = RayOrchestratorConfig(
    # Redis connection
    redis_url="redis://localhost:6379/",
    
    # Fair scheduling
    dispatcher_interval=2.0,  # Check for new tasks every 2 seconds
    
    # Resource limits
    max_concurrent_tasks=5,  # Max 5 tasks per user simultaneously
    max_queued_tasks=None,   # Unlimited queue (default)
    
    # Ray configuration
    ray_address=None,  # None = local, or "ray://cluster:10001"
    ray_namespace="docling",
)

# Create converter manager
cm = DoclingConverterManager()

# Create orchestrator
orchestrator = RayOrchestrator(
    config=config,
    converter_manager=cm,
)
```

### Advanced Configuration

```python
config = RayOrchestratorConfig(
    # Redis HA (Sentinel)
    redis_url="redis+sentinel://sentinel-host:26379/mymaster/0",
    redis_max_connections=50,
    
    # Queue limits with rejection
    max_queued_tasks=100,
    enable_queue_limit_rejection=True,  # Return 429 when limit exceeded
    
    # Document limits
    max_documents=50,
    enable_document_limits=True,
    
    # Fault tolerance
    max_task_retries=3,
    retry_delay=5.0,
    dispatcher_max_restarts=-1,  # Unlimited restarts
    
    # Timeouts
    task_timeout=3600.0,  # 1 hour per task
    document_timeout=300.0,  # 5 minutes per document
    
    # Health monitoring
    enable_heartbeat=True,
    heartbeat_interval=30.0,
    heartbeat_timeout=90.0,
    
    # Resource management
    ray_memory_limit_per_worker="4GB",
    enable_oom_protection=True,
)
    
    # mTLS Configuration
    enable_mtls=True,
    ray_cluster_name="my-ray-cluster",
```

## Usage

### Basic Usage

```python
import asyncio
from docling_jobkit.datamodel.http_inputs import FileSource
from docling_jobkit.datamodel.task_targets import InBodyTarget

async def main():
    # Start processing queue
    queue_task = asyncio.create_task(orchestrator.process_queue())
    
    # Enqueue a task
    sources = [
        FileSource(filename="document.pdf", content=pdf_bytes)
    ]
    
    task = await orchestrator.enqueue(
        sources=sources,
        target=InBodyTarget(),
    )
    
    print(f"Task {task.task_id} enqueued")
    
    # Wait for completion
    while not task.is_completed():
        await asyncio.sleep(1)
        task = await orchestrator.task_status(task.task_id)
    
    # Get result
    result = await orchestrator.task_result(task.task_id)
    print(f"Converted {result.num_converted} documents")
    print(f"Succeeded: {result.num_succeeded}, Failed: {result.num_failed}")

asyncio.run(main())
```

### Multi-User Fair Scheduling

```python
# User A enqueues 100 tasks
for i in range(100):
    task = await orchestrator.enqueue(sources=[...], target=InBodyTarget())
    task.metadata["user_id"] = "user_a"

# User B enqueues 10 tasks
for i in range(10):
    task = await orchestrator.enqueue(sources=[...], target=InBodyTarget())
    task.metadata["user_id"] = "user_b"

# Fair scheduling ensures:
# - User B's tasks are processed alongside User A's
# - User A doesn't monopolize all resources
# - Each dispatch round processes 1 task from each user
```

### Queue Limit Handling

```python
from docling_jobkit.orchestrators.ray import QueueLimitExceededError

config = RayOrchestratorConfig(
    max_queued_tasks=10,
    enable_queue_limit_rejection=True,
)

try:
    task = await orchestrator.enqueue(sources=[...], target=InBodyTarget())
except QueueLimitExceededError as e:
    print(f"Queue full: {e}")
    # Return 429 to client
```

### Health Monitoring

```python
# Check connections
await orchestrator.check_connection()

# Get statistics
stats = await orchestrator.get_stats()
print(f"Queue size: {stats['queue_size']}")
print(f"Users with tasks: {stats['users_with_tasks']}")
print(f"Dispatcher active: {stats['dispatcher']['active']}")
```

## Deployment

### Local Development

```python
# Use local Ray and Redis
config = RayOrchestratorConfig(
    redis_url="redis://localhost:6379/",
    ray_address=None,  # Start local Ray
)
```

### Production with Ray Cluster

```python
# Connect to existing Ray cluster
config = RayOrchestratorConfig(
    redis_url="redis://redis-service:6379/",
    ray_address="ray://ray-head:10001",
    ray_namespace="docling-prod",
)
```

### Kubernetes Deployment

See `docs/ray-cluster-deployment/` for Kubernetes deployment examples with:

## mTLS Authentication

The Ray Orchestrator supports mTLS (mutual TLS) authentication for secure communication with Ray clusters deployed on OpenShift/Kubernetes using the CodeFlare operator.

### Overview

When enabled, mTLS provides:
- **Secure communication**: Encrypted connections between Ray client and cluster
- **Mutual authentication**: Both client and server verify each other's identity
- **Certificate management**: Automatic certificate generation using codeflare-sdk

### Requirements

mTLS support requires the `codeflare-sdk` package, which is included in the `ray` optional dependencies:

```bash
pip install docling-jobkit[ray]
```

### Configuration

#### Python API

```python
from docling_jobkit.orchestrators.ray import RayOrchestratorConfig

config = RayOrchestratorConfig(
    # Enable mTLS
    enable_mtls=True,
    ray_cluster_name="my-ray-cluster",  # Required when enable_mtls=True
    
    # Ray cluster connection
    ray_address="ray://my-cluster-head:10001",
    ray_namespace="docling-namespace",
    
    # Redis configuration
    redis_url="redis://redis-service:6379/",
)
```

#### Environment Variables

```bash
# Enable mTLS
export DOCLING_ENABLE_MTLS=true
export DOCLING_RAY_CLUSTER_NAME=my-ray-cluster

# Ray cluster connection
export DOCLING_RAY_ADDRESS=ray://my-cluster-head:10001
export DOCLING_RAY_NAMESPACE=docling-namespace

# Redis configuration
export DOCLING_REDIS_URL=redis://redis-service:6379/
```

See `dev/.env-mtls-example` for a complete configuration example.

### How It Works

When `enable_mtls=True`, the orchestrator:

1. **Validates configuration**: Ensures `ray_cluster_name` is provided
2. **Generates certificates**: Uses `codeflare_sdk.generate_cert.generate_tls_cert()`
3. **Sets environment variables**: Calls `codeflare_sdk.generate_cert.export_env()`
4. **Initializes Ray**: Connects to cluster using generated certificates

The following environment variables are automatically set by codeflare-sdk:
- `RAY_USE_TLS=1`: Enables TLS in Ray
- `RAY_TLS_SERVER_CERT`: Path to server certificate
- `RAY_TLS_SERVER_KEY`: Path to server key
- `RAY_TLS_CA_CERT`: Path to CA certificate

### Example: OpenShift/Kubernetes Deployment

```python
import asyncio
from docling_jobkit.orchestrators.ray import (
    RayOrchestrator,
    RayOrchestratorConfig,
)
from docling_jobkit.convert.manager import DoclingConverterManager

async def main():
    # Configure with mTLS for production Ray cluster
    config = RayOrchestratorConfig(
        # mTLS configuration
        enable_mtls=True,
        ray_cluster_name="docling-ray-cluster",
        
        # Ray cluster (deployed via CodeFlare operator)
        ray_address="ray://docling-ray-cluster-head-svc:10001",
        ray_namespace="docling-prod",
        
        # Redis (managed service or deployed)
        redis_url="redis://redis-ha-service:6379/",
        
        # Resource limits
        max_concurrent_tasks=10,
        max_queued_tasks=100,
    )
    
    # Create orchestrator
    cm = DoclingConverterManager()
    orchestrator = RayOrchestrator(config=config, converter_manager=cm)
    
    # Verify connection
    await orchestrator.check_connection()
    print("Connected to Ray cluster with mTLS")
    
    # Start processing
    await orchestrator.process_queue()

asyncio.run(main())
```

### Error Handling

The implementation handles common error scenarios:

#### Missing Cluster Name
```python
# This will raise ValueError
config = RayOrchestratorConfig(
    enable_mtls=True,
    # ray_cluster_name not provided
)
# ValueError: ray_cluster_name must be provided when enable_mtls is True
```

#### Missing codeflare-sdk
```python

#### Missing Secret Name Error
```
RuntimeError: Failed to generate mTLS certificates: Missing the required parameter `name` when calling `read_namespaced_secret`
```

**Cause**: The codeflare-sdk is trying to read the Ray cluster's CA secret but cannot find it.

**Solutions**:
1. Verify the Ray cluster is deployed with TLS enabled
2. Check that the CA secret exists: `kubectl get secret -n <namespace> | grep ca-secret`
3. Ensure the secret name follows the pattern: `<cluster-name>-ca-secret-<hash>`
4. Verify you have proper RBAC permissions to read secrets in the namespace
5. Check that you're running inside the same Kubernetes cluster as the Ray cluster


#### RBAC Permissions Error
```
secrets is forbidden: User "system:serviceaccount:<namespace>:default" cannot list resource "secrets"
```

**Cause**: The pod's ServiceAccount doesn't have permissions to read secrets in the namespace.

**Solution**: Create a ServiceAccount with proper RBAC permissions:

```yaml
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: docling-serve-ray-sa
  namespace: docling
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: docling-serve-ray-secret-reader
  namespace: docling
rules:
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: docling-serve-ray-secret-reader-binding
  namespace: docling
subjects:
  - kind: ServiceAccount
    name: docling-serve-ray-sa
    namespace: docling
roleRef:
  kind: Role
  name: docling-serve-ray-secret-reader
  apiGroup: rbac.authorization.k8s.io
```

Then update your Deployment to use the ServiceAccount:
```yaml
spec:
  template:
    spec:
      serviceAccountName: docling-serve-ray-sa
```

See the complete example in `docs/deploy-examples/docling-ray.yaml`.

# If codeflare-sdk is not installed
# ImportError: codeflare-sdk is required for mTLS support.

#### Ray Client Connection Timeout with mTLS
```
ConnectionError: ray client connection timeout
```

**Cause**: The Ray client cannot establish a connection to the Ray cluster, even though mTLS certificates were generated successfully.

**Common Issues**:

1. **Port Mismatch**: Ensure you're using the correct port for Ray client connections (typically 10001, not 6379)
   ```bash
   # Correct
   ray://docling-ray-cluster-head-svc:10001
   
   # Wrong
   ray://docling-ray-cluster-head-svc:6379  # This is GCS port
   ```

2. **Ray Cluster TLS Not Enabled**: The Ray cluster itself must be configured with TLS enabled
   ```yaml
   # In RayCluster spec
   spec:
     headGroupSpec:
       rayStartParams:
         # TLS must be enabled on the Ray cluster
       template:
         spec:
           containers:
             - name: ray-head
               env:
                 - name: RAY_USE_TLS
                   value: '1'  # Must be enabled
   ```

3. **Certificate Mismatch**: Client certificates must match the Ray cluster's CA
   - Verify the CA secret name matches: `<cluster-name>-ca-secret-<hash>`
   - Check certificates are valid: `openssl x509 -in /path/to/cert -text -noout`

4. **Network Connectivity**: Test basic connectivity first
   ```bash
   # From the API pod
   nc -zv docling-ray-cluster-head-svc 10001
   ```

5. **Ray Cluster Not Ready**: Ensure Ray cluster is fully running
   ```bash
   kubectl get raycluster docling-ray-cluster -n docling
   kubectl get pods -n docling -l ray.io/cluster=docling-ray-cluster
   ```

**Debug Steps**:
1. Check the debug logs for mTLS environment variables
2. Verify Ray cluster has TLS enabled: `kubectl describe raycluster docling-ray-cluster -n docling`
3. Check Ray head pod logs: `kubectl logs -n docling <ray-head-pod> -c ray-head`
4. Test without mTLS first to isolate the issue
5. Compare client and server TLS configurations

# Install with: pip install docling-jobkit[ray]
```

#### Certificate Generation Failure
```python
# If certificate generation fails
# RuntimeError: Failed to generate mTLS certificates: <error details>
```

### Deployment Considerations

#### Local Development
For local development without mTLS:
```python
config = RayOrchestratorConfig(
    enable_mtls=False,  # Default
    ray_address=None,   # Start local Ray
)
```

#### Staging/Production
For production deployments with mTLS:
```python
config = RayOrchestratorConfig(
    enable_mtls=True,
    ray_cluster_name="prod-ray-cluster",
    ray_address="ray://ray-head-svc:10001",
)
```

### Troubleshooting

#### Certificate Generation Issues
1. Verify Ray cluster is deployed and accessible
2. Check cluster name matches the deployed Ray cluster
3. Ensure namespace is correct
4. Review codeflare-sdk logs for details

#### Connection Failures
1. Verify mTLS is enabled on the Ray cluster
2. Check network connectivity to Ray head node
3. Ensure certificates are generated successfully
4. Review Ray client logs for TLS errors

#### Environment Variable Conflicts
If you manually set Ray TLS environment variables, they may conflict with auto-generated ones. Let codeflare-sdk manage these variables when `enable_mtls=True`.

### Security Best Practices

1. **Enable mTLS in production**: Always use mTLS for production Ray clusters
2. **Rotate certificates**: Follow your organization's certificate rotation policy
3. **Secure Redis**: Use TLS for Redis connections in production
4. **Network policies**: Implement Kubernetes network policies to restrict access
5. **RBAC**: Use Kubernetes RBAC to control access to Ray clusters

### References

- [CodeFlare SDK Documentation](https://github.com/project-codeflare/codeflare-sdk)
- [Ray TLS/mTLS Documentation](https://docs.ray.io/en/latest/ray-core/configure.html#tls-authentication)
- [CodeFlare Operator](https://github.com/project-codeflare/codeflare-operator)

- Ray cluster setup
- Redis deployment (or managed Redis)
- Docling-serve integration
- Health checks and monitoring

## Redis Schema

### Per-User Task Queues
- **Key**: `user:{user_id}:tasks`
- **Type**: List (FIFO)
- **Value**: JSON-serialized Task objects

### Task Metadata
- **Key**: `task:{task_id}`
- **Type**: Hash
- **Fields**: status, user_id, created_at, started_at, finished_at, progress, error_message

### Task Results
- **Key**: `task:{task_id}:result`
- **Type**: String (msgpack-serialized)
- **TTL**: Configurable (default: 4 hours)

### User Limits
- **Key**: `user:{user_id}:limits`
- **Type**: Hash
- **Fields**: max_concurrent_tasks, max_documents, active_tasks, active_documents

### User Statistics
- **Key**: `user:{user_id}:stats`
- **Type**: Hash
- **Fields**: total_tasks, total_documents, successful_documents, failed_documents

## Performance Tuning

### Dispatcher Interval
- Lower values (0.5-1.0s): More responsive, higher Redis load
- Higher values (2.0-5.0s): Lower overhead, slightly higher latency

### Concurrent Task Limits
- Balance between fairness and throughput
- Consider available Ray worker resources
- Monitor memory usage

### Ray Data Parallelism
- Auto-detect (None): Ray determines optimal parallelism
- Manual: Set based on cluster size and document complexity

### Redis Connection Pool
- Increase `redis_max_connections` for high-throughput scenarios
- Monitor Redis connection usage

## Troubleshooting

### Dispatcher Not Processing Tasks
1. Check dispatcher health: `await orchestrator.check_connection()`
2. Verify Ray cluster is running: `ray status`
3. Check Redis connectivity: `redis-cli ping`
4. Review dispatcher logs for errors

### High Memory Usage
1. Enable OOM protection: `enable_oom_protection=True`
2. Set worker memory limits: `ray_memory_limit_per_worker="4GB"`
3. Reduce concurrent task limits
4. Monitor Ray object store usage

### Tasks Stuck in Queue
1. Check user limits: May have reached concurrent task limit
2. Verify dispatcher is active: Check heartbeat
3. Review task error messages in Redis
4. Check Ray worker availability

### Redis Connection Issues
1. Verify Redis is running and accessible
2. Check Redis URL format
3. For HA setups, verify Sentinel/Cluster configuration
4. Monitor Redis memory usage

## Comparison with Other Orchestrators

| Feature | Local | RQ | Ray |
|---------|-------|----|----|
| Fair Scheduling | ❌ | ❌ | ✅ |
| Distributed | ❌ | ✅ | ✅ |
| Per-User Queues | ❌ | ❌ | ✅ |
| Resource Limits | ❌ | ❌ | ✅ |
| Fault Tolerance | ⚠️ | ✅ | ✅ |
| Scalability | Low | Medium | High |
| Setup Complexity | Low | Medium | High |
