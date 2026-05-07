"""Unified Redis client factory for orchestrators.

Supports redis://, rediss://, unix://, redis+sentinel://, rediss+sentinel://.

Redis Cluster is rejected at config-build time: orchestrator transactions
and Lua scripts span keys that would land on different hash slots
(CROSSSLOT). Supporting cluster requires a keyspace redesign with hash
tags first, so the URL layer fails loud rather than silently misbehaving.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional
from urllib.parse import parse_qs, unquote, urlparse

import redis
import redis.asyncio as async_redis
import redis.asyncio.sentinel as async_sentinel
import redis.sentinel as sentinel

SENTINEL_URL_SCHEMES = ("redis+sentinel://", "rediss+sentinel://")
SINGLE_URL_SCHEMES = ("redis://", "rediss://", "unix://")
_CLUSTER_URL_SCHEMES = ("redis+cluster", "rediss+cluster", "redis-cluster")


class RedisMode(str, Enum):
    SINGLE = "single"
    SENTINEL = "sentinel"


class UnsupportedRedisModeError(ValueError):
    pass


def validate_url(url: str) -> None:
    """Run from a Pydantic model_validator so bad URLs raise at config build."""
    detect_mode(url)
    if url.startswith(SENTINEL_URL_SCHEMES):
        parse_sentinel_url(url)


def detect_mode(url: str) -> RedisMode:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()

    if "cluster" in parse_qs(parsed.query):
        raise UnsupportedRedisModeError(
            "Redis Cluster is not supported (?cluster=… is a no-op in "
            "redis-py and our orchestrators rely on multi-key transactions "
            "that fail with CROSSSLOT in cluster mode). Use redis:// for "
            "single-node or redis+sentinel:// for HA."
        )

    match scheme:
        case "redis" | "rediss" | "unix":
            return RedisMode.SINGLE
        case "redis+sentinel" | "rediss+sentinel":
            return RedisMode.SENTINEL
        case s if s in _CLUSTER_URL_SCHEMES:
            raise UnsupportedRedisModeError(
                f"Redis Cluster URL scheme '{s}://' is not supported. "
                "Our orchestrators use multi-key transactions and Lua "
                "scripts that span hash slots; running them against a "
                "cluster would surface as CROSSSLOT errors at runtime. "
                "Use redis:// for single-node or redis+sentinel:// for HA."
            )
        case _:
            raise ValueError(
                f"Unsupported redis URL scheme: '{scheme}://'. "
                "Expected one of: redis://, rediss://, unix://, "
                "redis+sentinel://, rediss+sentinel://."
            )


@dataclass(frozen=True)
class SentinelTarget:
    hosts: list[tuple[str, int]]
    service_name: str
    db: int = 0
    username: Optional[str] = None
    password: Optional[str] = None
    sentinel_username: Optional[str] = None
    sentinel_password: Optional[str] = None
    ssl: bool = False


def parse_sentinel_url(url: str) -> SentinelTarget:
    """Parse `redis+sentinel://[user:pass@]h1[:p],h2[:p],…/master[/db][?…]`.

    Userinfo carries the master creds. Sentinel-daemon creds and ssl ride in
    query params (`sentinel_username`, `sentinel_password`, `ssl`). The
    `rediss+sentinel://` scheme is a synonym for `?ssl=true`.
    """
    if not url.startswith(SENTINEL_URL_SCHEMES):
        raise ValueError(
            f"Sentinel URL must start with one of {SENTINEL_URL_SCHEMES}"
        )

    parsed = urlparse(url)
    netloc = parsed.netloc
    if "@" in netloc:
        userinfo, _, hostpart = netloc.rpartition("@")
    else:
        userinfo, hostpart = "", netloc

    username: Optional[str] = None
    password: Optional[str] = None
    if userinfo:
        if ":" in userinfo:
            user, _, pwd = userinfo.partition(":")
            username = unquote(user) if user else None
            password = unquote(pwd) if pwd else None
        else:
            username = unquote(userinfo)

    if not hostpart:
        raise ValueError("Sentinel URL must include at least one sentinel host")
    hosts: list[tuple[str, int]] = []
    for entry in hostpart.split(","):
        entry = entry.strip()
        if not entry:
            continue
        host, sep, port = entry.rpartition(":")
        if sep:
            hosts.append((host, int(port)))
        else:
            hosts.append((port, 26379))
    if not hosts:
        raise ValueError("Sentinel URL must include at least one sentinel host")

    path_parts = [p for p in parsed.path.split("/") if p]
    if not path_parts:
        raise ValueError(
            "Sentinel URL must include the master/service name in the path "
            "(e.g. /mymaster)"
        )
    service_name = unquote(path_parts[0])
    db = int(path_parts[1]) if len(path_parts) > 1 else 0

    query = {k: v[-1] for k, v in parse_qs(parsed.query).items() if v}
    ssl = parsed.scheme == "rediss+sentinel" or _to_bool(query.get("ssl"))

    return SentinelTarget(
        hosts=hosts,
        service_name=service_name,
        db=db,
        username=username,
        password=password,
        sentinel_username=query.get("sentinel_username"),
        sentinel_password=query.get("sentinel_password"),
        ssl=ssl,
    )


def _to_bool(value: Optional[str]) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


@dataclass(frozen=True)
class RedisSettings:
    url: str = "redis://localhost:6379/"
    max_connections: int = 50
    socket_timeout: Optional[float] = None
    socket_connect_timeout: Optional[float] = None
    decode_responses: bool = False


def _master_kwargs(target: SentinelTarget, s: RedisSettings) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "db": target.db,
        "max_connections": s.max_connections,
        "socket_timeout": s.socket_timeout,
        "socket_connect_timeout": s.socket_connect_timeout,
        "decode_responses": s.decode_responses,
    }
    if target.username is not None:
        kwargs["username"] = target.username
    if target.password is not None:
        kwargs["password"] = target.password
    if target.ssl:
        kwargs["ssl"] = True
    return kwargs


def _sentinel_kwargs(target: SentinelTarget, s: RedisSettings) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "socket_timeout": s.socket_timeout,
        "socket_connect_timeout": s.socket_connect_timeout,
    }
    if target.sentinel_username is not None:
        kwargs["username"] = target.sentinel_username
    if target.sentinel_password is not None:
        kwargs["password"] = target.sentinel_password
    return kwargs


def make_sync_client(s: RedisSettings) -> redis.Redis:
    match detect_mode(s.url):
        case RedisMode.SENTINEL:
            target = parse_sentinel_url(s.url)
            sentinel_client = sentinel.Sentinel(
                target.hosts,
                sentinel_kwargs=_sentinel_kwargs(target, s),
            )
            return sentinel_client.master_for(
                target.service_name, **_master_kwargs(target, s)
            )
        case RedisMode.SINGLE:
            pool = redis.ConnectionPool.from_url(
                s.url,
                max_connections=s.max_connections,
                socket_timeout=s.socket_timeout,
                socket_connect_timeout=s.socket_connect_timeout,
                decode_responses=s.decode_responses,
            )
            return redis.Redis(connection_pool=pool)


def make_async_client(s: RedisSettings) -> async_redis.Redis:
    match detect_mode(s.url):
        case RedisMode.SENTINEL:
            target = parse_sentinel_url(s.url)
            sentinel_client = async_sentinel.Sentinel(
                target.hosts,
                sentinel_kwargs=_sentinel_kwargs(target, s),
            )
            return sentinel_client.master_for(
                target.service_name, **_master_kwargs(target, s)
            )
        case RedisMode.SINGLE:
            pool = async_redis.ConnectionPool.from_url(
                s.url,
                max_connections=s.max_connections,
                socket_timeout=s.socket_timeout,
                socket_connect_timeout=s.socket_connect_timeout,
                decode_responses=s.decode_responses,
            )
            return async_redis.Redis(connection_pool=pool)
