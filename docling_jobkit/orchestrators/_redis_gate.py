import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator

from docling_jobkit.orchestrators.base_orchestrator import RedisBackpressureError


class RedisCallerGate:
    def __init__(self, concurrency: int):
        self._semaphore = asyncio.Semaphore(concurrency)

    @asynccontextmanager
    async def acquire(self, wait_timeout: float) -> AsyncIterator[None]:
        if wait_timeout > 0.0:
            try:
                await asyncio.wait_for(self._semaphore.acquire(), timeout=wait_timeout)
            except asyncio.TimeoutError as exc:
                raise RedisBackpressureError(
                    "Redis-backed orchestrator is saturated; please retry shortly."
                ) from exc
        else:
            if self._semaphore.locked():
                raise RedisBackpressureError(
                    "Redis-backed orchestrator is saturated; please retry shortly."
                )
            await self._semaphore.acquire()
        try:
            yield
        finally:
            self._semaphore.release()
