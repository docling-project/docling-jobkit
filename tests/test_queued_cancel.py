# SPDX-License-Identifier: MIT

"""Queued-only cancellation: atomic claim-vs-cancel arbitration in all engines.

Every conversion in these tests is a deterministic fake, so no model is
downloaded. The RQ and Ray engines arbitrate through Redis, so those tests
require a real Redis server (DOCLING_TEST_REDIS_URL, default
redis://localhost:6379/). An unreachable Redis fails the test instead of
skipping it, because silently skipping would hide the whole cross-process
guarantee.
"""

import asyncio
import os
import threading
import uuid
from typing import Any, AsyncIterator, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import redis as sync_redis

from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling.datamodel.service.responses import DoclingTaskResult, DocumentResultItem
from docling.datamodel.service.targets import InBodyTarget
from docling.datamodel.service.tasks import TaskType

from docling_jobkit.datamodel.result import ExportDocumentResponse
from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.base_orchestrator import (
    BaseOrchestrator,
    CancellationConflictReason,
    TaskCancellationConflictError,
    TaskNotFoundError,
)

_REDIS_URL = os.getenv("DOCLING_TEST_REDIS_URL", "redis://localhost:6379/")


def _unique() -> str:
    return uuid.uuid4().hex[:12]


def _fake_result() -> DoclingTaskResult:
    return DoclingTaskResult(
        result=DocumentResultItem(
            document=ExportDocumentResponse(filename="doc.md", md_content="hello"),
            status=ConversionStatus.SUCCESS,
        ),
        processing_time=0.01,
        num_converted=1,
        num_succeeded=1,
        num_failed=0,
    )


def _new_task(task_id: str, tenant_id: str | None = None) -> Task:
    task = Task(
        task_id=task_id,
        task_type=TaskType.CONVERT,
        target=InBodyTarget(),
        convert_options=ConvertDocumentsOptions(),
        sources=[],
    )
    if tenant_id is not None:
        task.metadata["tenant_id"] = tenant_id
    return task


@pytest.fixture
def sync_conn():
    conn = sync_redis.Redis.from_url(_REDIS_URL)
    try:
        conn.ping()
    except Exception as exc:  # pragma: no cover - environment failure
        pytest.fail(
            f"Redis at {_REDIS_URL} is required for queued-cancellation coverage: {exc}"
        )
    yield conn
    conn.close()


def _purge_redis(conn, token: str, task_ids: list[str]) -> None:
    """Remove every key a cancellation test created, keeping Redis clean."""
    keys: set[bytes] = set()
    for pattern in (
        f"*cancel72:{token}*",
        f"rq:*cancel72-{token}*",
        f"tenant:*cancel72-{token}*",
    ):
        keys |= set(conn.scan_iter(match=pattern))
    for task_id in task_ids:
        keys.add(f"task:{task_id}".encode())
        keys.add(f"task:{task_id}:dispatch".encode())
        keys.add(f"task:{task_id}:execution".encode())
        keys.add(f"docling:tasks:{task_id}:metadata".encode())
    if keys:
        conn.delete(*keys)


class _RecordingNotifier:
    """Notifier double that records every subscriber notification."""

    def __init__(self) -> None:
        self.notified: list[str] = []
        self.queue_notifications = 0
        self.added: list[str] = []
        self.removed: list[str] = []

    async def add_task(self, task_id: str) -> None:
        self.added.append(task_id)

    async def remove_task(self, task_id: str) -> None:
        self.removed.append(task_id)

    async def notify_task_subscribers(self, task_id: str) -> None:
        self.notified.append(task_id)

    async def notify_queue_positions(self) -> None:
        self.queue_notifications += 1


async def _wait_until(predicate, timeout: float = 10.0, message: str = "timeout"):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"{message} (waited {timeout}s)")


class TestCancellationContract:
    """Engine-independent part of the cancellation contract."""

    def test_cancelled_is_a_terminal_status(self):
        task = _new_task("t1")
        assert not task.is_completed()

        task.set_status(TaskStatus.CANCELLED)

        assert task.task_status == TaskStatus.CANCELLED
        assert task.is_completed()
        assert task.finished_at is not None

    def test_conflict_reasons_are_machine_readable(self):
        assert (
            BaseOrchestrator.cancellation_conflict_reason(TaskStatus.STARTED)
            == CancellationConflictReason.ALREADY_STARTED
        )
        for status in (TaskStatus.SUCCESS, TaskStatus.FAILURE):
            assert (
                BaseOrchestrator.cancellation_conflict_reason(status)
                == CancellationConflictReason.ALREADY_FINISHED
            )
        assert BaseOrchestrator.cancellation_conflict_reason(TaskStatus.PENDING) is None
        assert (
            BaseOrchestrator.cancellation_conflict_reason(TaskStatus.CANCELLED) is None
        )

    def test_conflict_error_exposes_reason_and_status(self):
        error = TaskCancellationConflictError(
            CancellationConflictReason.ALREADY_STARTED, TaskStatus.STARTED
        )

        assert error.reason.value == "already_started"
        assert error.task_status is TaskStatus.STARTED
        assert "already_started" in str(error)

    @pytest.mark.asyncio
    async def test_base_interface_reports_unsupported(self):
        class _Orchestrator(BaseOrchestrator):
            async def enqueue(self, *args, **kwargs):  # pragma: no cover
                raise NotImplementedError

            async def queue_size(self):  # pragma: no cover
                raise NotImplementedError

            async def get_queue_position(self, task_id):  # pragma: no cover
                raise NotImplementedError

            async def process_queue(self):  # pragma: no cover
                raise NotImplementedError

            async def warm_up_caches(self):  # pragma: no cover
                raise NotImplementedError

            async def clear_converters(self):  # pragma: no cover
                raise NotImplementedError

            async def check_connection(self):  # pragma: no cover
                raise NotImplementedError

            async def task_result(self, task_id):  # pragma: no cover
                raise NotImplementedError

        with pytest.raises(NotImplementedError):
            await _Orchestrator().cancel_queued_task("whatever")


def _local_orchestrator(num_workers: int = 1):
    from docling_jobkit.orchestrators.local.orchestrator import (
        LocalOrchestrator,
        LocalOrchestratorConfig,
    )

    cm = MagicMock()
    cm.config.allow_external_plugins = False
    cm.config.max_file_size = None
    cm.convert_documents.return_value = []
    config = LocalOrchestratorConfig(num_workers=num_workers, shared_models=True)
    return LocalOrchestrator(config=config, converter_manager=cm), cm


class _FakeConversion:
    """Patch target recording which tasks actually reached conversion.

    ``entered``/``release`` let a test pin one conversion open while it cancels
    a later task, which is the interleaving that matters: a worker that is busy
    converting must not be able to claim the cancelled task.
    """

    def __init__(
        self,
        entered: threading.Event | None = None,
        release: threading.Event | None = None,
    ) -> None:
        self.converted: list[str] = []
        self._entered = entered
        self._release = release

    def __call__(self, **kwargs: Any) -> DoclingTaskResult:
        task = kwargs["task"]
        self.converted.append(task.task_id)
        if self._entered is not None:
            self._entered.set()
        if self._release is not None:
            if not self._release.wait(timeout=30):  # pragma: no cover
                raise AssertionError("conversion gate was never released")
        return _fake_result()


class TestLocalQueuedCancellation:
    @staticmethod
    async def _enqueue(orchestrator, count: int) -> list[Task]:
        return [
            await orchestrator.enqueue(
                sources=[],
                convert_options=ConvertDocumentsOptions(),
                targets=[InBodyTarget()],
            )
            for _ in range(count)
        ]

    @pytest.mark.asyncio
    async def test_cancel_skips_conversion_and_keeps_other_tasks_running(self):
        orchestrator, _ = _local_orchestrator(num_workers=1)
        entered = threading.Event()
        release = threading.Event()
        conversion = _FakeConversion(entered=entered, release=release)
        orchestrator.bind_notifier(_RecordingNotifier())
        tasks = await self._enqueue(orchestrator, 3)
        cancelled_id = tasks[1].task_id

        with patch(
            "docling_jobkit.orchestrators.local.worker.process_exportable_results",
            conversion,
        ):
            queue_task = asyncio.create_task(orchestrator.process_queue())
            try:
                # Pin the worker inside the first conversion, so the
                # cancellation below provably races a busy worker.
                await _wait_until(
                    entered.is_set, message="first task never started converting"
                )
                await orchestrator.cancel_queued_task(cancelled_id)
                release.set()
                await _wait_until(
                    lambda: all(
                        orchestrator.tasks[task.task_id].task_status
                        != TaskStatus.PENDING
                        for task in tasks
                    ),
                    message="workers did not drain the queue",
                )
            finally:
                release.set()
                queue_task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await queue_task

        assert orchestrator.tasks[cancelled_id].task_status == TaskStatus.CANCELLED
        assert orchestrator.tasks[cancelled_id].finished_at is not None
        assert orchestrator.tasks[cancelled_id].is_completed()
        assert cancelled_id not in conversion.converted
        assert await orchestrator.task_result(cancelled_id) is None

        for task in (tasks[0], tasks[2]):
            assert orchestrator.tasks[task.task_id].task_status == TaskStatus.SUCCESS
            assert task.task_id in conversion.converted

        assert await orchestrator.queue_size() == 0

    @pytest.mark.asyncio
    async def test_repeated_cancellation_is_idempotent(self):
        orchestrator, _ = _local_orchestrator()
        (task,) = await self._enqueue(orchestrator, 1)

        first = await orchestrator.cancel_queued_task(task.task_id)
        second = await orchestrator.cancel_queued_task(task.task_id)

        assert first.task_status == TaskStatus.CANCELLED
        assert second.task_id == first.task_id
        assert second.finished_at == first.finished_at

    @pytest.mark.asyncio
    async def test_cancellation_publishes_to_observers(self):
        orchestrator, _ = _local_orchestrator()
        notifier = _RecordingNotifier()
        orchestrator.bind_notifier(notifier)
        (task,) = await self._enqueue(orchestrator, 1)

        await orchestrator.cancel_queued_task(task.task_id)

        assert task.task_id in notifier.notified
        assert notifier.queue_notifications == 1

    @pytest.mark.asyncio
    async def test_claim_wins_over_cancellation(self):
        orchestrator, _ = _local_orchestrator()
        (task,) = await self._enqueue(orchestrator, 1)

        assert orchestrator.claim_task_started(task.task_id) is True

        with pytest.raises(TaskCancellationConflictError) as excinfo:
            await orchestrator.cancel_queued_task(task.task_id)
        assert excinfo.value.reason is CancellationConflictReason.ALREADY_STARTED
        assert orchestrator.tasks[task.task_id].task_status is TaskStatus.STARTED

    @pytest.mark.asyncio
    async def test_cancellation_wins_over_late_claim(self):
        orchestrator, _ = _local_orchestrator()
        (task,) = await self._enqueue(orchestrator, 1)

        await orchestrator.cancel_queued_task(task.task_id)

        assert orchestrator.claim_task_started(task.task_id) is False
        assert orchestrator.tasks[task.task_id].task_status is TaskStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_finished_task_conflicts(self):
        orchestrator, _ = _local_orchestrator()
        (task,) = await self._enqueue(orchestrator, 1)
        orchestrator.tasks[task.task_id].set_status(TaskStatus.SUCCESS)

        with pytest.raises(TaskCancellationConflictError) as excinfo:
            await orchestrator.cancel_queued_task(task.task_id)

        assert excinfo.value.reason is CancellationConflictReason.ALREADY_FINISHED
        assert excinfo.value.task_status is TaskStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_unknown_task_raises_not_found(self):
        orchestrator, _ = _local_orchestrator()

        with pytest.raises(TaskNotFoundError):
            await orchestrator.cancel_queued_task("missing-task")

    @pytest.mark.asyncio
    async def test_queue_position_is_released(self):
        orchestrator, _ = _local_orchestrator()
        first, second = await self._enqueue(orchestrator, 2)

        assert await orchestrator.get_queue_position(second.task_id) == 2

        await orchestrator.cancel_queued_task(second.task_id)

        assert await orchestrator.get_queue_position(second.task_id) is None
        assert await orchestrator.get_queue_position(first.task_id) == 1
        assert await orchestrator.queue_size() == 1


class TestRqQueuedCancellation:
    """RQ arbitration happens in Redis, so these tests need a real server."""

    @pytest.fixture
    def token(self) -> str:
        return _unique()

    @pytest.fixture
    async def orchestrator(self, token: str, sync_conn):
        from docling_jobkit.orchestrators.rq.orchestrator import (
            RQOrchestrator,
            RQOrchestratorConfig,
        )

        config = RQOrchestratorConfig(
            redis_url=_REDIS_URL,
            queue_name=f"cancel72-{token}",
            results_prefix=f"docling:cancel72:{token}:results",
            heartbeat_key_prefix=f"docling:cancel72:{token}:alive",
            sub_channel=f"docling:cancel72:{token}:updates",
            results_ttl=600,
        )
        orch = RQOrchestrator(config=config)
        created: list[str] = []
        yield orch, created
        await orch.close()
        _purge_redis(sync_conn, token, created)

    async def _enqueue(self, orchestrator, created: list[str]) -> Task:
        task = await orchestrator.enqueue(
            sources=[],
            convert_options=ConvertDocumentsOptions(),
            targets=[InBodyTarget()],
        )
        created.append(task.task_id)
        return task

    async def _durable_task(self, orchestrator, task_id: str) -> Optional[Task]:
        return await orchestrator._get_task_from_redis(task_id)

    @pytest.mark.asyncio
    async def test_review_worker_before_enqueue_metadata_cannot_be_cancelled(
        self, orchestrator, sync_conn, tmp_path
    ):
        from docling_jobkit.datamodel.task import validate_task
        from docling_jobkit.orchestrators.rq.worker import _run_docling_task

        orch, created = orchestrator
        enqueue = orch._rq_queue.enqueue
        converter = MagicMock()
        converter.config.max_file_size = None
        converter.convert_documents.side_effect = RuntimeError(
            "review conversion entered"
        )
        job = MagicMock(connection=sync_conn)

        def dequeue_before_metadata(*args, **kwargs):
            queued = enqueue(*args, **kwargs)
            worker_task = validate_task(kwargs["kwargs"]["task_data"])
            with patch(
                "docling_jobkit.orchestrators.rq.worker.get_current_job",
                return_value=job,
            ):
                with pytest.raises(RuntimeError, match="review conversion entered"):
                    _run_docling_task(worker_task, converter, orch.config, tmp_path)
            return queued

        with patch.object(
            orch._rq_queue, "enqueue", side_effect=dequeue_before_metadata
        ):
            task = await self._enqueue(orch, created)
        converter.convert_documents.assert_called_once()
        with pytest.raises(TaskCancellationConflictError):
            await orch.cancel_queued_task(task.task_id)

    @pytest.mark.asyncio
    async def test_review_stale_pending_cannot_reopen_claim(self, orchestrator):
        from docling_jobkit.orchestrators.rq.orchestrator import RQOrchestrator
        from docling_jobkit.orchestrators.rq.worker import claim_task_started

        orch, created = orchestrator
        task = await self._enqueue(orch, created)
        peer = RQOrchestrator(config=orch.config)
        try:
            stale = await peer.get_raw_task(task.task_id)
            assert stale.task_status == TaskStatus.PENDING
            assert (
                claim_task_started(orch._redis_conn, task.task_id, metadata_ttl=600)
                == TaskStatus.STARTED
            )
            # A pending update/refresh that was already in flight must not undo
            # the worker's committed claim and re-enable queued cancellation.
            await peer._on_task_status_changed(stale)
            with pytest.raises(TaskCancellationConflictError):
                await orch.cancel_queued_task(task.task_id)
        finally:
            await peer.close()

    @pytest.mark.asyncio
    async def test_review_stale_peer_update_preserves_cancelled(self, orchestrator):
        from docling_jobkit.orchestrators.rq.orchestrator import RQOrchestrator
        from docling_jobkit.orchestrators.rq.worker import claim_task_started

        orch, created = orchestrator
        task = await self._enqueue(orch, created)
        peer = RQOrchestrator(config=orch.config)
        try:
            stale = await peer.get_raw_task(task.task_id)
            await orch.cancel_queued_task(task.task_id)
            # A peer already handling a status update has a stale cached Task.
            stale.set_status(TaskStatus.STARTED)
            await peer._on_task_status_changed(stale)
            durable = await orch._get_task_from_redis(task.task_id)
            worker_claim = claim_task_started(
                orch._redis_conn, task.task_id, metadata_ttl=600
            )
            assert (durable.task_status, worker_claim) == (
                TaskStatus.CANCELLED,
                TaskStatus.CANCELLED,
            )
        finally:
            await peer.close()

    @pytest.mark.asyncio
    async def test_cancel_queued_task_is_terminal_and_idempotent(
        self, orchestrator, sync_conn
    ):
        from rq.job import Job

        orch, created = orchestrator
        task = await self._enqueue(orch, created)
        assert await orch.queue_size() == 1

        cancelled = await orch.cancel_queued_task(task.task_id)
        again = await orch.cancel_queued_task(task.task_id)

        assert cancelled.task_status == TaskStatus.CANCELLED
        assert cancelled.finished_at is not None
        assert cancelled.is_completed()
        assert again.task_id == cancelled.task_id
        assert await orch.queue_size() == 0
        assert await orch.task_result(task.task_id) is None

        # Physical queue removal is cleanup on top of the durable transition.
        job = Job.fetch(task.task_id, connection=orch._redis_conn)
        assert job.get_status().value == "canceled"

        # A status poll and a late RQ refresh must not resurrect the task.
        polled = await orch.task_status(task.task_id)
        assert polled.task_status == TaskStatus.CANCELLED
        await orch._update_task_from_rq(task.task_id)
        assert orch.tasks[task.task_id].task_status == TaskStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_cancellation_is_visible_to_another_instance(
        self, orchestrator, token
    ):
        from docling_jobkit.orchestrators.rq.orchestrator import RQOrchestrator

        orch, created = orchestrator
        task = await self._enqueue(orch, created)
        await orch.cancel_queued_task(task.task_id)

        peer = RQOrchestrator(config=orch.config)
        try:
            seen = await peer.task_status(task.task_id)
            assert seen.task_status == TaskStatus.CANCELLED
            assert seen.finished_at is not None
        finally:
            await peer.close()

    @pytest.mark.asyncio
    async def test_worker_claim_loses_against_cancellation(
        self, orchestrator, sync_conn
    ):
        from docling_jobkit.orchestrators.rq.worker import claim_task_started

        orch, created = orchestrator
        task = await self._enqueue(orch, created)
        await orch.cancel_queued_task(task.task_id)

        claim = claim_task_started(
            sync_conn, task.task_id, metadata_ttl=orch.config.results_ttl
        )

        assert claim is TaskStatus.CANCELLED
        durable = await self._durable_task(orch, task.task_id)
        assert durable is not None
        assert durable.task_status == TaskStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_worker_claim_wins_and_blocks_cancellation(
        self, orchestrator, sync_conn
    ):
        from docling_jobkit.orchestrators.rq.worker import claim_task_started

        orch, created = orchestrator
        task = await self._enqueue(orch, created)

        claim = claim_task_started(
            sync_conn, task.task_id, metadata_ttl=orch.config.results_ttl
        )
        assert claim is TaskStatus.STARTED

        with pytest.raises(TaskCancellationConflictError) as excinfo:
            await orch.cancel_queued_task(task.task_id)
        assert excinfo.value.reason is CancellationConflictReason.ALREADY_STARTED
        durable = await self._durable_task(orch, task.task_id)
        assert durable is not None
        assert durable.task_status is TaskStatus.STARTED

    @pytest.mark.asyncio
    async def test_completed_task_conflicts(self, orchestrator):
        orch, created = orchestrator
        task = await self._enqueue(orch, created)
        tracked = orch.tasks[task.task_id]
        tracked.set_status(TaskStatus.FAILURE)
        await orch._store_task_in_redis(tracked)

        with pytest.raises(TaskCancellationConflictError) as excinfo:
            await orch.cancel_queued_task(task.task_id)

        assert excinfo.value.reason is CancellationConflictReason.ALREADY_FINISHED
        assert excinfo.value.task_status is TaskStatus.FAILURE

    @pytest.mark.asyncio
    async def test_unknown_task_raises_not_found(self, orchestrator):
        orch, _ = orchestrator

        with pytest.raises(TaskNotFoundError):
            await orch.cancel_queued_task(f"missing-{_unique()}")

    @pytest.mark.asyncio
    async def test_cancelled_task_is_not_converted(
        self, orchestrator, sync_conn, tmp_path
    ):
        """The worker entry point returns before touching the converter."""
        from docling_jobkit.orchestrators.rq.worker import _run_docling_task

        orch, created = orchestrator
        task = await self._enqueue(orch, created)
        await orch.cancel_queued_task(task.task_id)

        conversion_manager = MagicMock()
        conversion_manager.config.max_file_size = None
        job = MagicMock()
        job.connection = sync_conn

        with (
            patch(
                "docling_jobkit.orchestrators.rq.worker.get_current_job",
                return_value=job,
            ),
            patch.object(sync_conn, "publish") as publish,
        ):
            result = _run_docling_task(
                task,
                conversion_manager,
                orch.config,
                tmp_path,
            )

        assert result is None
        conversion_manager.convert_documents.assert_not_called()
        publish.assert_not_called()
        durable = await self._durable_task(orch, task.task_id)
        assert durable is not None
        assert durable.task_status is TaskStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_durable_status_transition_rejects_illegal_moves(self):
        from docling_jobkit.orchestrators.rq.orchestrator import (
            apply_durable_status_transition,
        )

        rejected = apply_durable_status_transition(
            {"task_status": "started"},
            expected=TaskStatus.PENDING,
            new_status=TaskStatus.CANCELLED,
            timestamp="2026-01-01T00:00:00+00:00",
        )
        assert rejected is None

        cancelled = apply_durable_status_transition(
            {"task_status": "pending"},
            expected=TaskStatus.PENDING,
            new_status=TaskStatus.CANCELLED,
            timestamp="2026-01-01T00:00:00+00:00",
        )
        assert cancelled["task_status"] == "cancelled"
        assert cancelled["finished_at"] == "2026-01-01T00:00:00+00:00"

    @pytest.mark.asyncio
    async def test_store_watch_error_retries_and_preserves_winner(self, orchestrator):
        from redis.asyncio.client import Pipeline
        from redis.exceptions import WatchError

        orch, created = orchestrator
        task = await self._enqueue(orch, created)
        assert task.task_status == TaskStatus.PENDING

        orig_execute = Pipeline.execute
        attempts = {"injected": 0}

        async def flaky_execute(self, *args: Any, **kwargs: Any):  # type: ignore[no-untyped-def]
            if attempts["injected"] == 0:
                attempts["injected"] += 1
                try:
                    await self.reset()
                except Exception:
                    pass
                raise WatchError("injected conflict")
            return await orig_execute(self, *args, **kwargs)

        with patch.object(Pipeline, "execute", flaky_execute):
            cancelled = await orch.cancel_queued_task(task.task_id)

        assert attempts["injected"] == 1
        assert cancelled.task_status == TaskStatus.CANCELLED
        durable = await self._durable_task(orch, task.task_id)
        assert durable is not None
        assert durable.task_status == TaskStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_cancel_retry_re_reads_competing_started_winner(
        self, orchestrator, sync_conn
    ):
        from redis.asyncio.client import Pipeline
        from redis.exceptions import WatchError

        from docling_jobkit.orchestrators.rq.worker import claim_task_started

        orch, created = orchestrator
        task = await self._enqueue(orch, created)
        assert task.task_status == TaskStatus.PENDING

        orig_execute = Pipeline.execute
        attempts = {"injected": 0}

        async def flaky_execute(self, *args: Any, **kwargs: Any):  # type: ignore[no-untyped-def]
            if attempts["injected"] == 0:
                attempts["injected"] += 1
                # Real competing winner commits between our snapshot read and
                # our failed EXEC: the worker's production STARTED claim wins.
                winner = claim_task_started(sync_conn, task.task_id, metadata_ttl=600)
                assert winner == TaskStatus.STARTED
                try:
                    await self.reset()
                except Exception:
                    pass
                raise WatchError("injected conflict with competing STARTED")
            return await orig_execute(self, *args, **kwargs)

        with patch.object(Pipeline, "execute", flaky_execute):
            with pytest.raises(TaskCancellationConflictError) as excinfo:
                await orch.cancel_queued_task(task.task_id)

        # A stale-snapshot mutant that reuses its cached CANCELLED payload
        # without re-reading would return CANCELLED here instead of CONFLICT
        # and would overwrite the winner; the retry must re-read the winner.
        assert attempts["injected"] == 1
        assert excinfo.value.reason is CancellationConflictReason.ALREADY_STARTED
        assert excinfo.value.task_status is TaskStatus.STARTED
        durable = await self._durable_task(orch, task.task_id)
        assert durable is not None
        assert durable.task_status == TaskStatus.STARTED

    @pytest.mark.asyncio
    async def test_cancel_retry_preserves_competing_cancelled_idempotent(
        self, orchestrator
    ):
        from redis.asyncio.client import Pipeline
        from redis.exceptions import WatchError

        from docling_jobkit.orchestrators.rq.orchestrator import RQOrchestrator

        orch, created = orchestrator
        task = await self._enqueue(orch, created)
        assert task.task_status == TaskStatus.PENDING

        peer = RQOrchestrator(config=orch.config)
        orig_execute = Pipeline.execute
        attempts = {"injected": 0}
        peer_finished: list[Any] = []

        async def flaky_execute(self, *args: Any, **kwargs: Any):  # type: ignore[no-untyped-def]
            if attempts["injected"] == 0:
                attempts["injected"] += 1
                # A competing replica commits CANCELLED between our snapshot
                # and our failed EXEC; the retry must re-read it idempotently
                # instead of writing a second CANCELLED payload with a new
                # timestamp.
                peer_cancelled = await peer.cancel_queued_task(task.task_id)
                assert peer_cancelled.task_status == TaskStatus.CANCELLED
                assert peer_cancelled.finished_at is not None
                peer_finished.append(peer_cancelled.finished_at)
                try:
                    await self.reset()
                except Exception:
                    pass
                raise WatchError("injected conflict with competing CANCELLED")
            return await orig_execute(self, *args, **kwargs)

        try:
            with patch.object(Pipeline, "execute", flaky_execute):
                cancelled = await orch.cancel_queued_task(task.task_id)
        finally:
            await peer.close()

        assert attempts["injected"] == 1
        assert cancelled.task_status == TaskStatus.CANCELLED
        assert cancelled.finished_at == peer_finished[0]
        durable = await self._durable_task(orch, task.task_id)
        assert durable is not None
        assert durable.task_status == TaskStatus.CANCELLED
        assert durable.finished_at == peer_finished[0]

    @pytest.mark.asyncio
    async def test_claim_and_terminal_timestamps_stable_with_queue_progress(
        self, orchestrator, sync_conn
    ):
        from docling_jobkit.orchestrators.rq.worker import claim_task_started

        orch, created = orchestrator
        first = await self._enqueue(orch, created)
        second = await self._enqueue(orch, created)
        third = await self._enqueue(orch, created)
        assert await orch.queue_size() == 3

        assert (
            claim_task_started(sync_conn, first.task_id, metadata_ttl=600)
            == TaskStatus.STARTED
        )
        first_durable = await self._durable_task(orch, first.task_id)
        assert first_durable is not None
        assert first_durable.started_at is not None
        claimed_at = first_durable.started_at

        refresh = await orch._get_task_from_redis(first.task_id)
        assert refresh is not None
        await orch._store_task_in_redis(refresh)
        again = await self._durable_task(orch, first.task_id)
        assert again is not None
        assert again.task_status == TaskStatus.STARTED
        assert again.started_at == claimed_at

        cancelled = await orch.cancel_queued_task(second.task_id)
        assert cancelled.finished_at is not None
        finished_at = cancelled.finished_at
        again_cancelled = await orch.cancel_queued_task(second.task_id)
        assert again_cancelled.finished_at == finished_at
        assert await orch.queue_size() == 2

        assert (
            claim_task_started(sync_conn, third.task_id, metadata_ttl=600)
            == TaskStatus.STARTED
        )
        third_durable = await self._durable_task(orch, third.task_id)
        assert third_durable is not None
        assert third_durable.task_status == TaskStatus.STARTED

    @pytest.mark.asyncio
    async def test_enqueue_error_rolls_back_durable_memory_notifier(
        self, orchestrator, sync_conn
    ):
        orch, created = orchestrator
        recorder = _RecordingNotifier()
        orch.bind_notifier(recorder)

        with patch.object(
            orch._rq_queue, "enqueue", side_effect=RuntimeError("queue down")
        ):
            with pytest.raises(RuntimeError, match="queue down"):
                await self._enqueue(orch, created)

        assert len(recorder.added) == 1
        orphan_id = recorder.added[0]
        assert orphan_id not in orch.tasks
        assert recorder.removed == [orphan_id]
        assert await orch._get_task_from_redis(orphan_id) is None
        with pytest.raises(TaskNotFoundError):
            await orch.cancel_queued_task(orphan_id)

    @pytest.mark.asyncio
    async def test_absent_and_corrupt_metadata_fail_closed(
        self, orchestrator, sync_conn, tmp_path
    ):
        from docling_jobkit.orchestrators.rq.worker import (
            _run_docling_task,
            claim_task_started,
        )

        orch, created = orchestrator
        missing_id = f"missing-{_unique()}"
        assert claim_task_started(sync_conn, missing_id, metadata_ttl=600) is None

        ghost = _new_task(missing_id)
        converter = MagicMock()
        converter.config.max_file_size = None
        job = MagicMock(connection=sync_conn)
        with (
            patch(
                "docling_jobkit.orchestrators.rq.worker.get_current_job",
                return_value=job,
            ),
            patch.object(sync_conn, "publish") as publish,
        ):
            assert _run_docling_task(ghost, converter, orch.config, tmp_path) is None
        converter.convert_documents.assert_not_called()
        publish.assert_not_called()
        assert await orch.task_result(missing_id) is None

        victim = await self._enqueue(orch, created)
        sync_conn.set(f"docling:tasks:{victim.task_id}:metadata", b"not-json{")
        assert claim_task_started(sync_conn, victim.task_id, metadata_ttl=600) is None
        with (
            patch(
                "docling_jobkit.orchestrators.rq.worker.get_current_job",
                return_value=job,
            ),
            patch.object(sync_conn, "publish") as corrupt_publish,
        ):
            assert _run_docling_task(victim, converter, orch.config, tmp_path) is None
        corrupt_publish.assert_not_called()


def _ray_state_manager_cls():
    pytest.importorskip(
        "ray", reason="Ray extra is required for Ray queued-cancellation coverage"
    )
    from docling_jobkit.orchestrators.ray.redis_helper import RedisStateManager

    return RedisStateManager


class TestRayQueuedCancellation:
    @pytest.mark.asyncio
    async def test_review_real_converter_gate_and_normal_progress(self, manager, token):
        tenant_id = f"cancel72-{token}"
        (task,) = await self._enqueue(manager, tenant_id)
        assert await manager.dispatch_task_atomic(tenant_id, task.task_id, 0)
        assert not await manager.write_task_execution_lease(
            task.task_id, tenant_id, "review-replica"
        )
        assert await manager.acquire_converter_unit(tenant_id, task.task_id, 2) == -1
        assert (await manager.get_tenant_limits(tenant_id)).converter_units == 0

        assert await manager.mark_task_started(task.task_id, tenant_id)
        assert await manager.write_task_execution_lease(
            task.task_id, tenant_id, "review-replica"
        )
        assert await manager.acquire_converter_unit(tenant_id, task.task_id, 2) == 1
        assert (await manager.get_tenant_limits(tenant_id)).converter_units == 1
        assert await manager.release_converter_units(tenant_id, task.task_id, 1) == 1
        final = await manager.finalize_task_success_atomic(
            tenant_id, task.task_id, 0, _fake_result()
        )
        assert final.final_status == TaskStatus.SUCCESS
        assert (await manager.get_tenant_limits(tenant_id)).converter_units == 0
        assert await manager.get_tenant_active_task_count(tenant_id) == 0
        assert not await manager.write_task_execution_lease(
            task.task_id, tenant_id, "review-replica"
        )
        assert await manager.acquire_converter_unit(tenant_id, task.task_id, 2) == -1

    @pytest.mark.asyncio
    async def test_review_failed_start_claim_cannot_convert_then_cancel(
        self, manager, token, tmp_path
    ):
        from docling_jobkit.orchestrators.ray.models import TaskCancellationOutcome
        from docling_jobkit.orchestrators.ray.serve_deployment import (
            DoclingProcessorCoordinatorDeployment,
        )

        tenant_id = f"cancel72-{token}"
        (task,) = await self._enqueue(manager, tenant_id)
        assert await manager.dispatch_task_atomic(tenant_id, task.task_id, 0)
        cls = DoclingProcessorCoordinatorDeployment.func_or_class
        coordinator = cls.__new__(cls)
        coordinator.redis_manager = manager
        coordinator.replica_id = "review-replica"
        coordinator.scratch_dir = tmp_path
        coordinator.tasks_processed = 0
        coordinator.documents_processed = 0
        coordinator.last_task_time = None
        coordinator._maintain_execution_heartbeat = AsyncMock()
        cancellations = []

        async def fake_converter(**kwargs):
            # The remote converter has been invoked: cancellation must already
            # conflict, even if the initial Redis STARTED claim hit a transient error.
            cancellations.append(await manager.cancel_queued_task_atomic(task.task_id))
            return _fake_result()

        coordinator.converter_handle = MagicMock()
        coordinator.converter_handle.process_converter_request.remote = AsyncMock(
            side_effect=fake_converter
        )

        async def fake_process(task, workdir):
            return await coordinator._run_single_converter_call(task, request={})

        coordinator._process_task = fake_process
        with patch.object(
            manager,
            "mark_task_started",
            side_effect=RuntimeError("transient claim error"),
        ):
            try:
                await coordinator.process_task(task)
            except RuntimeError:
                # Refusing dispatch is a valid fail-closed outcome; an exception
                # after converter invocation is not.
                coordinator.converter_handle.process_converter_request.remote.assert_not_awaited()
        assert not cancellations or all(
            c.outcome == TaskCancellationOutcome.CONFLICT for c in cancellations
        )
        assert await manager.get_tenant_active_task_count(tenant_id) == 0
        assert (await manager.get_tenant_limits(tenant_id)).converter_units == 0
        assert await manager.get_task_execution_lease(task.task_id) is None
        assert await manager.get_task_dispatch_hash(task.task_id) == {}

    @pytest.mark.asyncio
    async def test_review_stale_pending_cannot_reopen_ray_claim(self, manager, token):
        from docling_jobkit.orchestrators.ray.models import TaskCancellationOutcome

        tenant_id = f"cancel72-{token}"
        (task,) = await self._enqueue(manager, tenant_id)
        assert await manager.mark_task_started(task.task_id, tenant_id)
        await manager.update_task_status(task.task_id, TaskStatus.PENDING)
        cancellation = await manager.cancel_queued_task_atomic(task.task_id)
        assert cancellation.outcome == TaskCancellationOutcome.CONFLICT

    @pytest.mark.asyncio
    async def test_review_late_status_writer_preserves_cancelled(self, manager, token):
        (task,) = await self._enqueue(manager, f"cancel72-{token}")
        await manager.cancel_queued_task_atomic(task.task_id)
        await manager.update_task_status(task.task_id, TaskStatus.STARTED)
        metadata = await manager.get_task_metadata_model(task.task_id)
        assert metadata.status == TaskStatus.CANCELLED

    @pytest.fixture
    def token(self) -> str:
        return _unique()

    @pytest.fixture
    async def manager(self, token: str, sync_conn):
        state_manager = _ray_state_manager_cls()(
            redis_url=_REDIS_URL,
            results_prefix=f"docling:cancel72:{token}:results",
            sub_channel=f"docling:cancel72:{token}:updates",
            max_concurrent_tasks=2,
            max_queued_tasks=10,
        )
        await state_manager.connect()
        # Task hashes are keyed by UUID, not by tenant, so the fixture has to
        # remember the ids it created in order to purge them afterwards.
        state_manager.test_created_task_ids: list[str] = []
        yield state_manager
        await state_manager.disconnect()
        _purge_redis(sync_conn, token, state_manager.test_created_task_ids)

    async def _enqueue(self, manager, tenant_id: str, count: int = 1) -> list[Task]:
        tasks = []
        for _ in range(count):
            task = _new_task(str(uuid.uuid4()), tenant_id)
            await manager.set_task_metadata(
                task_id=task.task_id,
                tenant_id=tenant_id,
                task_type=TaskType.CONVERT,
                task_size=0,
                status=TaskStatus.PENDING,
            )
            await manager.enqueue_task(tenant_id, task)
            tasks.append(task)
            manager.test_created_task_ids.append(task.task_id)
        return tasks

    @pytest.mark.asyncio
    async def test_cancel_queued_task_is_atomic_and_releases_queue(
        self, manager, token
    ):
        from docling_jobkit.orchestrators.ray.models import TaskCancellationOutcome

        tenant_id = f"cancel72-{token}"
        (task,) = await self._enqueue(manager, tenant_id)
        assert await manager.get_tenant_queue_size(tenant_id) == 1
        assert (await manager.get_tenant_limits(tenant_id)).queued_tasks == 1

        first = await manager.cancel_queued_task_atomic(task.task_id)
        second = await manager.cancel_queued_task_atomic(task.task_id)

        assert first.outcome is TaskCancellationOutcome.CANCELLED
        assert first.queue_entry_removed is True
        assert first.reservation_released is False
        assert second.outcome is TaskCancellationOutcome.ALREADY_CANCELLED

        metadata = await manager.get_task_metadata_model(task.task_id)
        assert metadata is not None
        assert metadata.status is TaskStatus.CANCELLED
        assert metadata.finished_at is not None

        assert await manager.get_tenant_queue_size(tenant_id) == 0
        limits = await manager.get_tenant_limits(tenant_id)
        assert limits.queued_tasks == 0
        counters = await manager.get_tenant_task_counters(tenant_id)
        assert counters.tasks_cancelled_total == 1
        assert counters.tasks_started_total == 0

    @pytest.mark.asyncio
    async def test_cancel_unknown_task(self, manager):
        from docling_jobkit.orchestrators.ray.models import TaskCancellationOutcome

        result = await manager.cancel_queued_task_atomic(f"missing-{_unique()}")

        assert result.outcome is TaskCancellationOutcome.NOT_FOUND

    @pytest.mark.asyncio
    async def test_cancel_conflicts_with_started_and_finished(self, manager, token):
        from docling_jobkit.orchestrators.ray.models import TaskCancellationOutcome

        tenant_id = f"cancel72-{token}"
        started, finished = await self._enqueue(manager, tenant_id, 2)
        await manager.update_task_status(started.task_id, TaskStatus.STARTED)
        await manager.update_task_status(finished.task_id, TaskStatus.SUCCESS)

        started_result = await manager.cancel_queued_task_atomic(started.task_id)
        finished_result = await manager.cancel_queued_task_atomic(finished.task_id)

        assert started_result.outcome is TaskCancellationOutcome.CONFLICT
        assert started_result.status is TaskStatus.STARTED
        assert finished_result.outcome is TaskCancellationOutcome.CONFLICT
        assert finished_result.status is TaskStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_worker_claim_loses_against_cancellation(self, manager, token):
        from docling_jobkit.orchestrators.ray.models import TaskCancellationOutcome

        tenant_id = f"cancel72-{token}"
        (task,) = await self._enqueue(manager, tenant_id)
        cancellation = await manager.cancel_queued_task_atomic(task.task_id)
        assert cancellation.outcome is TaskCancellationOutcome.CANCELLED

        claimed = await manager.mark_task_started(task.task_id, tenant_id)

        assert claimed is False
        metadata = await manager.get_task_metadata_model(task.task_id)
        assert metadata is not None
        assert metadata.status is TaskStatus.CANCELLED
        assert metadata.started_at is None
        counters = await manager.get_tenant_task_counters(tenant_id)
        assert counters.tasks_started_total == 0

    @pytest.mark.asyncio
    async def test_worker_claim_wins_and_blocks_cancellation(self, manager, token):
        from docling_jobkit.orchestrators.ray.models import TaskCancellationOutcome

        tenant_id = f"cancel72-{token}"
        (task,) = await self._enqueue(manager, tenant_id)

        assert await manager.mark_task_started(task.task_id, tenant_id) is True

        result = await manager.cancel_queued_task_atomic(task.task_id)
        assert result.outcome is TaskCancellationOutcome.CONFLICT
        assert result.status is TaskStatus.STARTED

    @pytest.mark.asyncio
    async def test_cancelled_task_cannot_acquire_a_converter_unit(self, manager, token):
        """No execution lease means the replica aborts before converting."""
        tenant_id = f"cancel72-{token}"
        (task,) = await self._enqueue(manager, tenant_id)
        await manager.cancel_queued_task_atomic(task.task_id)

        lease_written = await manager.write_task_execution_lease(
            task.task_id, tenant_id, "replica-a"
        )
        granted = await manager.acquire_converter_unit(
            tenant_id, task.task_id, ceiling=2
        )

        assert lease_written is False
        assert await manager.get_task_execution_lease(task.task_id) is None
        assert granted == -1

    @pytest.mark.asyncio
    async def test_cancellation_releases_a_dispatch_reservation(self, manager, token):
        from docling_jobkit.orchestrators.ray.models import TaskCancellationOutcome

        tenant_id = f"cancel72-{token}"
        (task,) = await self._enqueue(manager, tenant_id)
        assert await manager.dispatch_task_atomic(tenant_id, task.task_id, 0) is True
        assert await manager.get_tenant_active_task_count(tenant_id) == 1
        assert (await manager.get_tenant_limits(tenant_id)).active_tasks == 1

        result = await manager.cancel_queued_task_atomic(task.task_id)

        assert result.outcome is TaskCancellationOutcome.CANCELLED
        assert result.reservation_released is True
        assert await manager.get_tenant_active_task_count(tenant_id) == 0
        assert (await manager.get_tenant_limits(tenant_id)).active_tasks == 0
        assert await manager.get_task_dispatch_hash(task.task_id) == {}
        assert await manager.get_task_execution_lease(task.task_id) is None

    @pytest.mark.asyncio
    async def test_cancelled_entry_never_blocks_the_queue(self, manager, token):
        """A stale cancelled payload at the queue front is dropped, not dispatched."""
        tenant_id = f"cancel72-{token}"
        stale, survivor = await self._enqueue(manager, tenant_id, 2)
        # Terminalize without touching the queue, the way a cancellation that
        # lost its cleanup would leave things behind.
        await manager.update_task_status(stale.task_id, TaskStatus.CANCELLED)

        assert await manager.dispatch_task_atomic(tenant_id, stale.task_id, 0) is False
        assert await manager.get_tenant_queue_size(tenant_id) == 1
        assert (
            await manager.dispatch_task_atomic(tenant_id, survivor.task_id, 0) is True
        )

        assert await manager.get_tenant_queue_size(tenant_id) == 0
        limits = await manager.get_tenant_limits(tenant_id)
        assert limits.queued_tasks == 0
        assert limits.active_tasks == 1
        assert await manager.get_tenant_active_task_ids(tenant_id) == [survivor.task_id]

    @pytest.mark.asyncio
    async def test_late_finalize_does_not_overwrite_cancellation(self, manager, token):
        tenant_id = f"cancel72-{token}"
        (task,) = await self._enqueue(manager, tenant_id)
        await manager.cancel_queued_task_atomic(task.task_id)

        terminalization = await manager.finalize_task_success_atomic(
            tenant_id=tenant_id,
            task_id=task.task_id,
            task_size=0,
            result=_fake_result(),
        )

        assert terminalization.status_changed is False
        assert terminalization.final_status is TaskStatus.CANCELLED
        assert terminalization.result_key is None
        metadata = await manager.get_task_metadata_model(task.task_id)
        assert metadata is not None
        assert metadata.status is TaskStatus.CANCELLED
        assert await manager.get_task_outcome(task.task_id) is None
        counters = await manager.get_tenant_task_counters(tenant_id)
        assert counters.tasks_succeeded_total == 0

    @pytest.mark.asyncio
    async def test_timeout_failure_and_user_cancel_first_terminal_wins(
        self, manager, token
    ):
        from docling.datamodel.service.responses import (
            FailureCategory,
            FailurePhase,
            PublicFailureInfo,
        )

        from docling_jobkit.orchestrators.ray.models import TaskCancellationOutcome

        def _timeout_failure() -> PublicFailureInfo:
            return PublicFailureInfo(
                category=FailureCategory.INTERNAL,
                message="converter_request_timeout",
                retryable=False,
                phase=FailurePhase.ORCHESTRATION,
            )

        tenant_id = f"cancel72-{token}"
        # Timeout failure first: terminal FAILURE wins and a late user cancel
        # conflicts instead of resurrecting the task.
        (timed_out,) = await self._enqueue(manager, tenant_id)
        failure = await manager.finalize_task_failure_atomic(
            tenant_id=tenant_id,
            task_id=timed_out.task_id,
            task_size=0,
            error_message="converter_request_timeout",
            failure=_timeout_failure(),
        )
        assert failure.status_changed is True
        assert failure.final_status is TaskStatus.FAILURE
        late_cancel = await manager.cancel_queued_task_atomic(timed_out.task_id)
        assert late_cancel.outcome is TaskCancellationOutcome.CONFLICT
        assert late_cancel.status is TaskStatus.FAILURE
        metadata = await manager.get_task_metadata_model(timed_out.task_id)
        assert metadata is not None
        assert metadata.status is TaskStatus.FAILURE
        assert await manager.get_task_outcome(timed_out.task_id) is not None

        # User cancel first: CANCELLED wins, a late timeout failure cannot
        # overwrite it, and no converter unit may be granted afterwards.
        (cancelled,) = await self._enqueue(manager, tenant_id)
        first = await manager.cancel_queued_task_atomic(cancelled.task_id)
        assert first.outcome is TaskCancellationOutcome.CANCELLED
        late_failure = await manager.finalize_task_failure_atomic(
            tenant_id=tenant_id,
            task_id=cancelled.task_id,
            task_size=0,
            error_message="converter_request_timeout",
            failure=_timeout_failure(),
        )
        assert late_failure.status_changed is False
        assert late_failure.final_status is TaskStatus.CANCELLED
        metadata = await manager.get_task_metadata_model(cancelled.task_id)
        assert metadata is not None
        assert metadata.status is TaskStatus.CANCELLED
        assert await manager.get_task_outcome(cancelled.task_id) is None
        lease_written = await manager.write_task_execution_lease(
            cancelled.task_id, tenant_id, "replica-a"
        )
        granted = await manager.acquire_converter_unit(
            tenant_id, cancelled.task_id, ceiling=2
        )
        assert lease_written is False
        assert granted == -1

    @pytest.mark.asyncio
    async def test_cancellation_is_visible_to_another_instance(self, manager, token):
        tenant_id = f"cancel72-{token}"
        (task,) = await self._enqueue(manager, tenant_id)
        await manager.cancel_queued_task_atomic(task.task_id)

        peer = _ray_state_manager_cls()(
            redis_url=_REDIS_URL,
            results_prefix=f"docling:cancel72:{token}:results",
            sub_channel=f"docling:cancel72:{token}:updates",
        )
        await peer.connect()
        try:
            metadata = await peer.get_task_metadata_model(task.task_id)
            assert metadata is not None
            assert metadata.status is TaskStatus.CANCELLED
        finally:
            await peer.disconnect()


class TestRayOrchestratorCancellation:
    """The Ray orchestrator is a thin adapter over the Redis primitive."""

    @pytest.fixture
    def token(self) -> str:
        return _unique()

    @pytest.fixture
    async def orchestrator(self, token: str, sync_conn):
        pytest.importorskip("ray", reason="Ray extra is required")
        from docling_jobkit.orchestrators._redis_gate import RedisCallerGate
        from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
        from docling_jobkit.orchestrators.ray.orchestrator import RayOrchestrator
        from docling_jobkit.orchestrators.ray.redis_helper import RedisStateManager

        state_manager = RedisStateManager(
            redis_url=_REDIS_URL,
            results_prefix=f"docling:cancel72:{token}:results",
            sub_channel=f"docling:cancel72:{token}:updates",
        )
        config = RayOrchestratorConfig(redis_url=_REDIS_URL)
        with patch.object(RayOrchestrator, "__init__", lambda self, **kw: None):
            orch = object.__new__(RayOrchestrator)
        orch.config = config
        orch.allowed_target_kinds = None
        orch.tasks = {}
        orch.notifier = _RecordingNotifier()
        orch.cm = MagicMock()
        orch.cm.config = MagicMock()
        orch.redis_manager = state_manager
        orch._redis_gate = RedisCallerGate(config.redis_gate_concurrency or 1)
        orch.dispatcher = None
        created: list[str] = []
        yield orch, created
        _purge_redis(sync_conn, token, created)

    async def _enqueue(self, orchestrator, created: list[str], tenant_id: str) -> Task:
        orch, _ = orchestrator
        task = _new_task(str(uuid.uuid4()), tenant_id)
        await orch.redis_manager.set_task_metadata(
            task_id=task.task_id,
            tenant_id=tenant_id,
            task_type=TaskType.CONVERT,
            task_size=0,
            status=TaskStatus.PENDING,
        )
        await orch.redis_manager.enqueue_task(tenant_id, task)
        created.append(task.task_id)
        return task

    @pytest.mark.asyncio
    async def test_cancel_delegates_and_publishes(self, orchestrator, token):
        orch, created = orchestrator
        tenant_id = f"cancel72-{token}"
        task = await self._enqueue(orchestrator, created, tenant_id)

        cancelled = await orch.cancel_queued_task(task.task_id)
        repeated = await orch.cancel_queued_task(task.task_id)

        assert cancelled.task_status == TaskStatus.CANCELLED
        assert cancelled.finished_at is not None
        assert repeated.task_id == cancelled.task_id
        assert task.task_id in orch.notifier.notified
        assert (
            await orch.task_status(task.task_id)
        ).task_status is TaskStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_cancel_conflicts_and_not_found(self, orchestrator, token):
        orch, created = orchestrator
        tenant_id = f"cancel72-{token}"
        started = await self._enqueue(orchestrator, created, tenant_id)
        await orch.redis_manager.mark_task_started(started.task_id, tenant_id)

        with pytest.raises(TaskCancellationConflictError) as excinfo:
            await orch.cancel_queued_task(started.task_id)
        assert excinfo.value.reason is CancellationConflictReason.ALREADY_STARTED

        with pytest.raises(TaskNotFoundError):
            await orch.cancel_queued_task(f"missing-{_unique()}")

    @pytest.mark.asyncio
    async def test_late_pubsub_update_does_not_regress_cancellation(
        self, orchestrator, token
    ):
        from docling_jobkit.orchestrators.ray.models import TaskUpdate

        orch, created = orchestrator
        tenant_id = f"cancel72-{token}"
        task = await self._enqueue(orchestrator, created, tenant_id)
        await orch.cancel_queued_task(task.task_id)

        async def _updates() -> AsyncIterator[TaskUpdate]:
            yield TaskUpdate(task_id=task.task_id, task_status=TaskStatus.STARTED)
            yield TaskUpdate(task_id=task.task_id, task_status=TaskStatus.SUCCESS)

        with patch.object(orch.redis_manager, "subscribe_to_updates", _updates):
            await orch._listen_for_updates()

        assert orch.tasks[task.task_id].task_status is TaskStatus.CANCELLED
        assert orch.tasks[task.task_id].finished_at is not None


class _FakeReplicaCall:
    def result(self, timeout_s=None, _skip_asyncio_check=False):
        return None


class _FakeDeploymentHandle:
    def __init__(self) -> None:
        self.process_task = MagicMock()
        self.process_task.remote = MagicMock(
            side_effect=lambda task: _FakeReplicaCall()
        )


class TestRayDispatcherCancellation:
    """The dispatcher never hands a cancelled task to a replica."""

    @pytest.fixture
    def token(self) -> str:
        return _unique()

    @pytest.fixture
    def dispatcher(self):
        pytest.importorskip("ray", reason="Ray extra is required")
        from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
        from docling_jobkit.orchestrators.ray.dispatcher import RayTaskDispatcher

        config = RayOrchestratorConfig(redis_url=_REDIS_URL)
        return RayTaskDispatcher.__ray_actor_class__(config, _FakeDeploymentHandle())

    async def _enqueued(self, manager, tenant_id: str, created: list[str]) -> Task:
        task = _new_task(str(uuid.uuid4()), tenant_id)
        await manager.set_task_metadata(
            task_id=task.task_id,
            tenant_id=tenant_id,
            task_type=TaskType.CONVERT,
            task_size=0,
            status=TaskStatus.PENDING,
        )
        await manager.enqueue_task(tenant_id, task)
        created.append(task.task_id)
        return task

    @pytest.mark.asyncio
    async def test_dispatched_but_cancelled_task_never_reaches_a_replica(
        self, dispatcher, sync_conn, token
    ):
        from docling_jobkit.orchestrators.ray.models import TaskCancellationOutcome

        manager = _ray_state_manager_cls()(
            redis_url=_REDIS_URL,
            results_prefix=f"docling:cancel72:{token}:results",
            sub_channel=f"docling:cancel72:{token}:updates",
        )
        await manager.connect()
        created: list[str] = []
        try:
            tenant_id = f"cancel72-{token}"
            task = await self._enqueued(manager, tenant_id, created)

            # The reservation is taken first, then the cancellation wins the
            # race: this is the interleaving the pre-launch skip has to catch.
            assert (
                await manager.dispatch_task_atomic(tenant_id, task.task_id, 0) is True
            )
            assert (
                await manager.cancel_queued_task_atomic(task.task_id)
            ).outcome is TaskCancellationOutcome.CANCELLED

            await dispatcher._process_task_async(task, tenant_id)

            dispatcher.deployment_handle.process_task.remote.assert_not_called()
            assert dispatcher._wake_event.is_set()
            metadata = await manager.get_task_metadata_model(task.task_id)
            assert metadata is not None
            assert metadata.status is TaskStatus.CANCELLED
        finally:
            await manager.disconnect()
            _purge_redis(sync_conn, token, created)

    @pytest.mark.asyncio
    async def test_uncancelled_task_is_still_dispatched(
        self, dispatcher, sync_conn, token
    ):
        manager = _ray_state_manager_cls()(
            redis_url=_REDIS_URL,
            results_prefix=f"docling:cancel72:{token}:results",
            sub_channel=f"docling:cancel72:{token}:updates",
        )
        await manager.connect()
        created: list[str] = []
        try:
            tenant_id = f"cancel72-{token}"
            task = await self._enqueued(manager, tenant_id, created)
            assert (
                await manager.dispatch_task_atomic(tenant_id, task.task_id, 0) is True
            )

            await dispatcher._process_task_async(task, tenant_id)

            dispatcher.deployment_handle.process_task.remote.assert_called_once_with(
                task
            )
        finally:
            await manager.disconnect()
            _purge_redis(sync_conn, token, created)

    @pytest.mark.asyncio
    async def test_failed_cancellation_check_still_dispatches(self, dispatcher, token):
        """A Redis read error must not fail a task that may well be valid."""
        dispatcher.redis_manager.get_task_metadata_model = AsyncMock(
            side_effect=ConnectionError("redis unavailable")
        )
        task = _new_task(str(uuid.uuid4()), f"cancel72-{token}")

        await dispatcher._process_task_async(task, f"cancel72-{token}")

        dispatcher.deployment_handle.process_task.remote.assert_called_once_with(task)
