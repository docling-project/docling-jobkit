from typing import Any
from urllib.parse import unquote, urlsplit

import redis
import redis.asyncio as async_redis
from redis.asyncio.sentinel import (
    Sentinel as AsyncSentinel,
    SentinelConnectionPool as AsyncSentinelConnectionPool,
)
from redis.sentinel import Sentinel, SentinelConnectionPool

SENTINEL_SCHEME = "redis+sentinel"
DEFAULT_SENTINEL_PORT = 26379


def is_sentinel_url(url: str) -> bool:
    return url.startswith(f"{SENTINEL_SCHEME}://")


def parse_sentinel_url(url: str) -> tuple[list[tuple[str, int]], str, dict[str, Any]]:
    """Parse redis+sentinel://[[user]:password@]host[:port][,host[:port]]/service[/db]."""
    parts = urlsplit(url)
    userinfo, _, hostlist = parts.netloc.rpartition("@")
    hosts = []
    for host in hostlist.split(","):
        name, _, port = host.partition(":")
        hosts.append((name, int(port) if port else DEFAULT_SENTINEL_PORT))

    service_name, _, db = parts.path.strip("/").partition("/")
    if not service_name:
        raise ValueError(f"Sentinel URL must include a service name: {url}")

    master_kwargs: dict[str, Any] = {"db": int(db) if db else 0}
    if userinfo:
        username, _, password = userinfo.partition(":")
        if username:
            master_kwargs["username"] = unquote(username)
        if password:
            master_kwargs["password"] = unquote(password)
    return hosts, service_name, master_kwargs


def _sentinel_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in kwargs.items() if k.startswith("socket_")}


def sync_pool_from_url(url: str, **kwargs: Any) -> redis.ConnectionPool:
    """Build a sync connection pool for a standard or sentinel Redis URL."""
    if not is_sentinel_url(url):
        return redis.ConnectionPool.from_url(url, **kwargs)
    hosts, service_name, master_kwargs = parse_sentinel_url(url)
    sentinel = Sentinel(hosts, sentinel_kwargs=_sentinel_kwargs(kwargs))
    return SentinelConnectionPool(service_name, sentinel, **master_kwargs, **kwargs)


def async_pool_from_url(url: str, **kwargs: Any) -> async_redis.ConnectionPool:
    """Build an async connection pool for a standard or sentinel Redis URL."""
    if not is_sentinel_url(url):
        return async_redis.ConnectionPool.from_url(url, **kwargs)
    hosts, service_name, master_kwargs = parse_sentinel_url(url)
    sentinel = AsyncSentinel(hosts, sentinel_kwargs=_sentinel_kwargs(kwargs))
    return AsyncSentinelConnectionPool(
        service_name, sentinel, **master_kwargs, **kwargs
    )
