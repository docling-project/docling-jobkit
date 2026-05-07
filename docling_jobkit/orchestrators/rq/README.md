# RQ Orchestrator

RQ-based orchestrator for Docling document processing. Uses Redis as the queue and result store; workers run in separate processes via `rq` workers.

## Configuration

`RQOrchestratorConfig` reads from environment variables (or constructor kwargs). The most relevant knobs:

| Field | Default | Purpose |
|---|---|---|
| `redis_url` | `redis://localhost:6379/` | Redis connection URL — see below |
| `redis_max_connections` | `50` | Connections in the Redis pool |
| `results_ttl` | `4h` | TTL for job results |
| `failure_ttl` | `4h` | TTL for failed-job records |
| `redis_gate_concurrency` | `max_connections - 10` | Cap on caller-facing Redis ops to keep connections free for background work |

## Redis Connection URL

`redis_url` accepts the following schemes:

| Scheme | Example | Use |
|---|---|---|
| `redis://` | `redis://host:6379/0` | Single node |
| `rediss://` | `rediss://host:6380/0` | Single node, TLS |
| `unix://` | `unix:///var/run/redis.sock` | Unix socket |
| `redis+sentinel://` | `redis+sentinel://[user:pass@]h1[:p],h2[:p],…/master[/db][?…]` | Sentinel HA |
| `rediss+sentinel://` | as above, synonym for `?ssl=true` | Sentinel HA, TLS to master |

Sentinel URL parts:

- `userinfo` — Redis master credentials
- comma-separated netloc hosts — Sentinel daemons (default port `26379`)
- path — master/service name and optional db (default `0`)
- query params:
  - `sentinel_username`, `sentinel_password` — credentials for the Sentinel daemons themselves (separate from the master credentials in the userinfo)
  - `ssl=true` — TLS to the master (alternative to `rediss+sentinel://`)

Redis Cluster is not supported: orchestrator transactions span multiple keys and would surface CROSSSLOT errors. Cluster URLs and `?cluster=…` are rejected at config-build time.

## Examples

```python
from docling_jobkit.orchestrators.rq.orchestrator import (
    RQOrchestrator, RQOrchestratorConfig,
)

# Single node
config = RQOrchestratorConfig(redis_url="redis://localhost:6379/")

# Sentinel-fronted HA cluster
config = RQOrchestratorConfig(
    redis_url=(
        "redis+sentinel://:redis-master-password@s1,s2,s3/mymaster"
        "?sentinel_password=sentinel-daemon-password"
    ),
)

orchestrator = RQOrchestrator(config)
```

## Worker

Workers are launched out-of-process and connect to the same Redis. See `docling_jobkit.orchestrators.rq.worker.CustomRQWorker` for the in-tree worker class. It reuses the same `RQOrchestratorConfig` so worker and orchestrator stay in sync (including Sentinel routing — the worker heartbeat thread also uses the resolved master).
