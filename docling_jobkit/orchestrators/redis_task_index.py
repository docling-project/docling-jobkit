# SPDX-License-Identifier: MIT

"""Shared Redis mechanics for the tenant-scoped active-task index.

The index is derived state that keeps a page read cheap: one sorted set per
tenant holds the ids of that tenant's active tasks in listing order, and one
compact hash per task holds the fields a listing item needs. Every key lives
in a namespace that isolates one backend - and one backend instance - from
every other one, so a backend can rebuild its index without touching another
one's data.

The ordering rule is owned by ``docling_jobkit.datamodel.task_listing`` and
reused here, so a Redis score and a Python sort key can never disagree.
"""

import datetime
from typing import Any, Optional

from docling_jobkit.datamodel.task_listing import sortable_created_at_score

# Version of the index layout. A change to the keys or to the stored fields
# bumps the version, which gives every backend a fresh, empty namespace
# instead of a half-compatible index.
INDEX_VERSION = 1

# Bounded lifetime of the cold-build lock: a builder that dies mid-build
# releases the lock by expiry instead of blocking every later build.
BUILD_LOCK_TTL_SECONDS = 300

# A Redis score is a double, so only integers below this bound stay exact.
# The creation-time scores are microseconds since the epoch and stay far
# below it.
EXACT_SCORE_LIMIT = 2**53

__all__ = [
    "BUILD_LOCK_TTL_SECONDS",
    "EXACT_SCORE_LIMIT",
    "INDEX_VERSION",
    "acquire_build_lock",
    "acquire_lock",
    "build_lock_key",
    "decode_optional_text",
    "generation_key",
    "index_namespace",
    "index_score",
    "invalidate_index",
    "invalidate_ready_generation",
    "is_index_ready",
    "is_ready_at_generation",
    "mark_index_ready",
    "mark_ready_if_generation",
    "queue_invalidate_ready_if_present",
    "ready_generation",
    "ready_key",
    "release_build_lock",
    "release_lock",
    "sortable_created_at_score",
    "task_record_key",
    "tenant_build_lock_key",
    "tenant_generation_key",
    "tenant_index_key",
    "tenant_ready_key",
]


def index_namespace(backend: str, scope: str = "") -> str:
    """Namespace that isolates one backend's index from every other one.

    *scope* names the instance inside the backend - the queue a backend reads
    from - so two backends that serve different queues never share an index.
    """
    namespace = f"docling:taskindex:{backend}:v{INDEX_VERSION}"
    if scope:
        namespace = f"{namespace}:{scope}"
    return namespace


def tenant_index_key(namespace: str, tenant_id: str) -> str:
    """Sorted set holding the tenant's active task ids in listing order."""
    return f"{namespace}:tenant:{tenant_id}:tasks"


def task_record_key(namespace: str, task_id: str) -> str:
    """Compact hash holding the fields a listing item needs."""
    return f"{namespace}:task:{task_id}"


def ready_key(namespace: str) -> str:
    """Flag marking the namespace's index as built and maintained.

    The stored value is the ready generation the build was fenced against, so
    a flag left behind by an earlier generation is never trusted.
    """
    return f"{namespace}:ready"


def tenant_ready_key(namespace: str, tenant_id: str) -> str:
    """Readiness flag of a single tenant, for backends that build one at a time."""
    return f"{namespace}:tenant:{tenant_id}:ready"


def generation_key(namespace: str) -> str:
    """Generation counter that fences readiness publication for a namespace."""
    return f"{namespace}:ready-generation"


def tenant_generation_key(namespace: str, tenant_id: str) -> str:
    """Generation counter that fences one tenant's readiness publication."""
    return f"{namespace}:tenant:{tenant_id}:ready-generation"


def build_lock_key(namespace: str) -> str:
    """Key of the lock that keeps a cold build single-flight."""
    return f"{namespace}:build-lock"


def tenant_build_lock_key(namespace: str, tenant_id: str) -> str:
    """Build lock of a single tenant, for backends that build one at a time."""
    return f"{namespace}:tenant:{tenant_id}:build-lock"


def index_score(created_at: datetime.datetime) -> int:
    """Sorted-set score that orders a tenant's tasks newest first.

    The score is the negated count of microseconds since the UTC epoch, so an
    ascending range yields the newest task first while two tasks created in
    the same microsecond keep the ascending task-id tie-break Redis applies to
    equal scores.
    """
    return sortable_created_at_score(created_at)


async def is_index_ready(redis: Any, namespace: str) -> bool:
    """Report whether the namespace's index is built for the current generation."""
    return await is_ready_at_generation(
        redis, generation_key(namespace), ready_key(namespace)
    )


async def mark_index_ready(
    redis: Any, namespace: str, expected_generation: str
) -> bool:
    """Advertise the namespace's index only if its generation did not advance.

    A partial or contended build must never look like a complete index, and a
    writer that invalidated readiness during the build must win.
    """
    return await mark_ready_if_generation(
        redis,
        generation_key(namespace),
        ready_key(namespace),
        expected_generation,
    )


async def invalidate_index(redis: Any, namespace: str) -> int:
    """Advance the generation and drop the readiness flag atomically."""
    return await invalidate_ready_generation(
        redis, generation_key(namespace), ready_key(namespace)
    )


_INIT_READY_GENERATION_SCRIPT = """
local generation = redis.call('GET', KEYS[1])
if not generation then
    redis.call('SET', KEYS[1], '0')
    return '0'
end
return generation
"""


async def ready_generation(redis: Any, key: str) -> str:
    """Return the generation a builder must fence against, creating zero first.

    A first enabled build must establish the generation before it enumerates
    candidates, otherwise a concurrent disabled writer would see no bookkeeping
    to invalidate and the builder could publish an incomplete index. The
    initialization is atomic, and only enabled builders call this helper, so a
    never-enabled deployment still creates no index bookkeeping.
    """
    value = await redis.eval(_INIT_READY_GENERATION_SCRIPT, 1, key)
    return decode_optional_text(value) or "0"


_IS_READY_AT_GENERATION_SCRIPT = """
local ready = redis.call('GET', KEYS[2])
if not ready then
    return 0
end
local generation = redis.call('GET', KEYS[1])
if not generation then
    generation = '0'
end
if ready == generation then
    return 1
end
return 0
"""


async def is_ready_at_generation(
    redis: Any, generation_key: str, ready_key: str
) -> bool:
    """Report whether *ready_key* was published for the current generation."""
    return bool(
        await redis.eval(_IS_READY_AT_GENERATION_SCRIPT, 2, generation_key, ready_key)
    )


_MARK_READY_IF_GENERATION_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if not current then
    current = '0'
end
if current ~= ARGV[1] then
    return 0
end
redis.call('SET', KEYS[2], ARGV[1])
return 1
"""


async def mark_ready_if_generation(
    redis: Any,
    generation_key: str,
    ready_key: str,
    expected_generation: str,
) -> bool:
    """Publish *ready_key* only while the generation still equals the fence."""
    return bool(
        await redis.eval(
            _MARK_READY_IF_GENERATION_SCRIPT,
            2,
            generation_key,
            ready_key,
            expected_generation,
        )
    )


_INVALIDATE_READY_GENERATION_SCRIPT = """
local generation = redis.call('INCR', KEYS[1])
redis.call('DEL', KEYS[2])
return generation
"""


async def invalidate_ready_generation(
    redis: Any, generation_key: str, ready_key: str
) -> int:
    """Advance the generation and delete readiness in one atomic step.

    The generation advances even when no ready marker exists, so a builder
    that captured the previous value can never publish an obsolete index.
    """
    return int(
        await redis.eval(
            _INVALIDATE_READY_GENERATION_SCRIPT, 2, generation_key, ready_key
        )
    )


_INVALIDATE_READY_IF_PRESENT_SCRIPT = """
if redis.call('EXISTS', KEYS[1]) == 1 or redis.call('EXISTS', KEYS[2]) == 1 then
    redis.call('INCR', KEYS[1])
    redis.call('DEL', KEYS[2])
    return 1
end
return 0
"""


def queue_invalidate_ready_if_present(
    target: Any, generation_key: str, ready_key: str
) -> Any:
    """Invalidate readiness only when this scope ever had index bookkeeping.

    Passing a pipeline queues the script into the surrounding transaction, so
    a disabled producer's metadata/queue write and the invalidation commit
    together. Passing a client returns the script result directly. A deployment
    that never enabled the index has neither key, so this is a no-op and does
    not create index bookkeeping.
    """
    return target.eval(
        _INVALIDATE_READY_IF_PRESENT_SCRIPT, 2, generation_key, ready_key
    )


async def acquire_build_lock(
    redis: Any,
    namespace: str,
    token: str,
    ttl: int = BUILD_LOCK_TTL_SECONDS,
) -> bool:
    """Try to own the cold build of a namespace.

    The lock is bounded by its time to live, so a builder that dies never
    blocks the index forever.
    """
    return await acquire_lock(redis, build_lock_key(namespace), token, ttl=ttl)


async def acquire_lock(
    redis: Any,
    key: str,
    token: str,
    ttl: int = BUILD_LOCK_TTL_SECONDS,
) -> bool:
    """Try to own the build guarded by *key*.

    The lock is bounded by its time to live, so a builder that dies never
    blocks the index forever.
    """
    acquired = await redis.set(key, token, nx=True, ex=ttl)
    return bool(acquired)


_RELEASE_LOCK_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


async def release_build_lock(redis: Any, namespace: str, token: str) -> None:
    """Release the cold-build lock, but only the lock this caller owns.

    The token keeps a caller that lost its lock - because the lock expired -
    from deleting the lock of the caller that owns it now.
    """
    await release_lock(redis, build_lock_key(namespace), token)


async def release_lock(redis: Any, key: str, token: str) -> None:
    """Release the build lock at *key*, but only the lock this caller owns."""
    await redis.eval(_RELEASE_LOCK_SCRIPT, 1, key, token)


def decode_optional_text(value: Optional[Any]) -> Optional[str]:
    """Decode a Redis value into text, keeping ``None`` for a missing value."""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    return str(value)
