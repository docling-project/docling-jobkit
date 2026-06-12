"""Tests for per-tenant in-flight converter-unit accounting.

Fan-out (S3 children, page slices, passthrough) is bounded per tenant by the
in-flight converter-unit counter, ceiling'd at ``max_concurrent_tasks``. These
tests cover the Redis acquire/release primitives with a faithful in-memory
re-implementation of their Lua scripts (the suite has no live Redis), keeping the
coverage hermetic and isolated from the Session A dispatcher tests.
"""

import pytest

from docling_jobkit.orchestrators.ray.redis_helper import (
    _ACQUIRE_CONVERTER_UNIT_LUA,
    _RELEASE_CONVERTER_UNITS_LUA,
    RedisStateManager,
)


class _FakeConverterUnitRedis:
    """In-memory redis simulating the converter-unit Lua scripts.

    Only the operations exercised by acquire/release are modelled. A hash
    "exists" iff it has at least one field (matching Redis semantics), so an
    execution lease must be primed before units can be acquired.
    """

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, int]] = {}
        self._scripts: dict[str, str] = {}
        self._next_sha = 0

    def prime_execution_lease(self, task_id: str) -> None:
        self.hashes[f"task:{task_id}:execution"] = {"replica_id": 1}

    def drop_execution_lease(self, task_id: str) -> None:
        self.hashes.pop(f"task:{task_id}:execution", None)

    def converter_units(self, tenant_id: str) -> int:
        return self.hashes.get(f"tenant:{tenant_id}:limits", {}).get(
            "converter_units", 0
        )

    def held_units(self, task_id: str) -> int:
        return self.hashes.get(f"task:{task_id}:execution", {}).get(
            "converter_units", 0
        )

    async def script_load(self, script: str) -> str:
        sha = f"sha-{self._next_sha}"
        self._next_sha += 1
        self._scripts[sha] = script
        return sha

    async def evalsha(self, sha: str, numkeys: int, *args: object) -> int:
        script = self._scripts[sha]
        keys = [str(a) for a in args[:numkeys]]
        argv = [int(a) for a in args[numkeys:]]
        if script == _ACQUIRE_CONVERTER_UNIT_LUA:
            return self._acquire(keys[0], keys[1], argv[0])
        if script == _RELEASE_CONVERTER_UNITS_LUA:
            return self._release(keys[0], keys[1], argv[0])
        raise AssertionError("unexpected script")

    def _exists(self, key: str) -> bool:
        return bool(self.hashes.get(key))

    def _acquire(self, limits_key: str, execution_key: str, ceiling: int) -> int:
        if not self._exists(execution_key):
            return -1
        limits = self.hashes.setdefault(limits_key, {})
        current = limits.get("converter_units", 0)
        if current >= ceiling:
            return 0
        limits["converter_units"] = current + 1
        execution = self.hashes.setdefault(execution_key, {})
        execution["converter_units"] = execution.get("converter_units", 0) + 1
        return 1

    def _release(self, limits_key: str, execution_key: str, count: int) -> int:
        execution = self.hashes.get(execution_key, {})
        held = execution.get("converter_units", 0)
        rel = min(count, held)
        if rel <= 0:
            return 0
        limits = self.hashes.setdefault(limits_key, {})
        limits["converter_units"] = limits.get("converter_units", 0) - rel
        execution["converter_units"] = held - rel
        return rel


def _manager() -> tuple[RedisStateManager, _FakeConverterUnitRedis]:
    manager = RedisStateManager(redis_url="redis://localhost:6379/")
    fake = _FakeConverterUnitRedis()
    manager.redis = fake  # type: ignore[assignment]
    return manager, fake


@pytest.mark.asyncio
async def test_acquire_grants_up_to_ceiling_then_returns_zero() -> None:
    manager, fake = _manager()
    fake.prime_execution_lease("task-1")

    granted = [
        await manager.acquire_converter_unit("tenant-a", "task-1", ceiling=3)
        for _ in range(5)
    ]

    assert granted == [1, 1, 1, 0, 0]
    assert fake.converter_units("tenant-a") == 3
    assert fake.held_units("task-1") == 3


@pytest.mark.asyncio
async def test_acquire_on_missing_lease_returns_minus_one() -> None:
    manager, fake = _manager()
    # No execution lease primed: task terminalized/reconciled.

    granted = await manager.acquire_converter_unit("tenant-a", "task-1", ceiling=5)

    assert granted == -1
    assert fake.converter_units("tenant-a") == 0


@pytest.mark.asyncio
async def test_release_clamps_to_held_and_double_release_is_noop() -> None:
    manager, fake = _manager()
    fake.prime_execution_lease("task-1")
    await manager.acquire_converter_unit("tenant-a", "task-1", ceiling=5)
    await manager.acquire_converter_unit("tenant-a", "task-1", ceiling=5)

    # Release more than held -> clamps to 2.
    released = await manager.release_converter_units("tenant-a", "task-1", 5)
    assert released == 2
    assert fake.converter_units("tenant-a") == 0
    assert fake.held_units("task-1") == 0

    # Second release has nothing left to free.
    assert await manager.release_converter_units("tenant-a", "task-1", 5) == 0
    assert fake.converter_units("tenant-a") == 0


@pytest.mark.asyncio
async def test_tenants_have_independent_ceilings() -> None:
    manager, fake = _manager()
    fake.prime_execution_lease("task-a")
    fake.prime_execution_lease("task-b")

    for _ in range(3):
        await manager.acquire_converter_unit("tenant-a", "task-a", ceiling=2)
    granted_b = await manager.acquire_converter_unit("tenant-b", "task-b", ceiling=2)

    assert fake.converter_units("tenant-a") == 2  # capped at its own ceiling
    assert granted_b == 1  # tenant-b unaffected
    assert fake.converter_units("tenant-b") == 1


@pytest.mark.asyncio
async def test_release_recycles_budget() -> None:
    manager, fake = _manager()
    fake.prime_execution_lease("task-1")

    # Fill the budget, then a child completes and the next one reuses the unit.
    await manager.acquire_converter_unit("tenant-a", "task-1", ceiling=2)
    await manager.acquire_converter_unit("tenant-a", "task-1", ceiling=2)
    assert await manager.acquire_converter_unit("tenant-a", "task-1", ceiling=2) == 0
    await manager.release_converter_units("tenant-a", "task-1", 1)
    assert await manager.acquire_converter_unit("tenant-a", "task-1", ceiling=2) == 1
    assert fake.converter_units("tenant-a") == 2


@pytest.mark.asyncio
async def test_non_positive_release_count_is_noop() -> None:
    manager, fake = _manager()
    fake.prime_execution_lease("task-1")

    assert await manager.release_converter_units("tenant-a", "task-1", 0) == 0
    assert fake.converter_units("tenant-a") == 0
