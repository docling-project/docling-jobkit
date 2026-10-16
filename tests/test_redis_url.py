import redis
import redis.asyncio as async_redis
from redis.asyncio.sentinel import (
    SentinelConnectionPool as AsyncSentinelConnectionPool,
)
from redis.sentinel import SentinelConnectionPool

from docling_jobkit.orchestrators._redis_url import (
    async_pool_from_url,
    parse_sentinel_url,
    sync_pool_from_url,
)
from docling_jobkit.orchestrators.rq.orchestrator import (
    RQOrchestrator,
    RQOrchestratorConfig,
)

SENTINEL_URL = "redis+sentinel://:s3cr%40t@s1:26380,s2/mymaster/2"


def test_parse_sentinel_url():
    hosts, service_name, master_kwargs = parse_sentinel_url(SENTINEL_URL)
    assert hosts == [("s1", 26380), ("s2", 26379)]
    assert service_name == "mymaster"
    assert master_kwargs == {"db": 2, "password": "s3cr@t"}


def test_sentinel_url_builds_sentinel_pools():
    sync_pool = sync_pool_from_url(SENTINEL_URL, max_connections=5)
    async_pool = async_pool_from_url(SENTINEL_URL, max_connections=5)
    assert isinstance(sync_pool, SentinelConnectionPool)
    assert isinstance(async_pool, AsyncSentinelConnectionPool)
    assert sync_pool.service_name == "mymaster"
    assert sync_pool.max_connections == 5


def test_standard_url_builds_plain_pools():
    assert type(sync_pool_from_url("redis://localhost:6379/0")) is redis.ConnectionPool
    assert (
        type(async_pool_from_url("redis://localhost:6379/0"))
        is async_redis.ConnectionPool
    )


def test_rq_queue_uses_sentinel_pool():
    conn, _ = RQOrchestrator.make_rq_queue(RQOrchestratorConfig(redis_url=SENTINEL_URL))
    assert isinstance(conn.connection_pool, SentinelConnectionPool)
