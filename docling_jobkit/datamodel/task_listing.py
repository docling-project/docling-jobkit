# SPDX-License-Identifier: MIT

"""Compact, tenant-scoped listings of active tasks.

Every orchestrator backend returns the same shape, ordering and paging rules
for a tenant's in-flight work, so the filtering/sorting/paging rules live here
instead of being reimplemented per backend. The listing is deliberately
identity-level: an item never carries sources, targets, options or results.
"""

import datetime
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Optional, TypeVar

from pydantic import BaseModel, Field

from docling.datamodel.service.tasks import TaskType

from docling_jobkit.datamodel.task_meta import TaskStatus

if TYPE_CHECKING:
    from docling_jobkit.datamodel.task import Task

# Tasks whose tenant is not recorded in the task metadata belong to this tenant,
# matching how the orchestrators and the serving layer resolve a tenant.
DEFAULT_TENANT_ID = "default"

ACTIVE_TASK_STATUSES = frozenset({TaskStatus.PENDING, TaskStatus.STARTED})

MIN_PAGE_LIMIT = 1
MAX_PAGE_LIMIT = 100

# Queued IDs and task metadata are read in chunks of this size so a deep
# queue never turns into one unbounded Redis read. The active-task set itself
# is read with a single SMEMBERS, so chunking does not bound that one call.
LISTING_CHUNK_SIZE = 100

# Fields of the compact projection an indexed backend stores per task. The
# projection carries the tenant so a reader can filter without reading the
# full task, and never carries sources, targets, options or results.
COMPACT_TASK_FIELDS = (
    "task_id",
    "task_type",
    "task_status",
    "created_at",
    "tenant_id",
)

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


class ActiveTaskItem(BaseModel):
    """Identity-level view of one active task."""

    task_id: str
    task_type: TaskType
    task_status: TaskStatus
    created_at: datetime.datetime


class ActiveTaskPage(BaseModel):
    """One page of active tasks; ``has_more`` replaces a total count or cursor."""

    items: list[ActiveTaskItem] = Field(default_factory=list)
    has_more: bool = False


class CompactTask(BaseModel):
    """Identity-level projection of one task, as an indexed backend stores it.

    The projection is what a listing page reads instead of the full task, so
    it holds the task id, its type, its status, its creation time and the
    tenant it belongs to - and nothing else.
    """

    task_id: str
    task_type: TaskType
    task_status: TaskStatus
    created_at: datetime.datetime
    tenant_id: str = DEFAULT_TENANT_ID

    @classmethod
    def from_task(cls, task: "Task") -> "CompactTask":
        """Project a task into the fields a listing needs."""
        return cls(
            task_id=task.task_id,
            task_type=task.task_type,
            task_status=task.task_status,
            created_at=task.created_at,
            tenant_id=tenant_of(task.metadata),
        )

    def to_redis_mapping(self) -> dict[str, str]:
        """Render the projection as the fields of a Redis hash."""
        return {
            "task_id": self.task_id,
            "task_type": str(self.task_type.value),
            "task_status": str(self.task_status.value),
            "created_at": self.created_at.isoformat(),
            "tenant_id": self.tenant_id,
        }

    @classmethod
    def from_redis_mapping(
        cls, mapping: Optional[Mapping[Any, Any]]
    ) -> Optional["CompactTask"]:
        """Decode a stored projection.

        ``None`` marks a row that is absent or unusable: a listing omits such
        a row instead of reporting a task it cannot describe.
        """
        if not mapping:
            return None
        # A real Redis client returns hash field names as bytes unless the
        # connection decodes responses globally. Normalise the keys here so a
        # compact record looks the same either way, instead of failing the
        # tenant lookup on an otherwise valid row.
        normalized: dict[str, Any] = {}
        for key, value in mapping.items():
            if isinstance(key, (bytes, bytearray)):
                key = key.decode("utf-8", errors="replace")
            normalized[str(key)] = value
        decoded: dict[str, str] = {}
        for field in COMPACT_TASK_FIELDS:
            value = normalized.get(field)
            if value is None:
                return None
            if isinstance(value, (bytes, bytearray)):
                value = value.decode("utf-8", errors="replace")
            decoded[field] = str(value)
        try:
            return cls(
                task_id=decoded["task_id"],
                task_type=TaskType(decoded["task_type"]),
                task_status=TaskStatus(decoded["task_status"]),
                created_at=datetime.datetime.fromisoformat(decoded["created_at"]),
                tenant_id=decoded["tenant_id"] or DEFAULT_TENANT_ID,
            )
        except (TypeError, ValueError):
            return None


def sortable_created_at_score(created_at: datetime.datetime) -> int:
    """Exact score that orders tasks by creation time, newest first.

    The score is the negated count of microseconds since the UTC epoch, so an
    ascending read yields the newest task first while two tasks created in
    the same microsecond keep the ascending task-id tie-break. The conversion
    is integer arithmetic on purpose: a float timestamp collapses two
    distinct microsecond timestamps onto the same boundary.
    """
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=datetime.timezone.utc)
    delta = created_at.astimezone(datetime.timezone.utc) - _EPOCH
    micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    return -micros


def resolve_tenant_id(value: Optional[Any]) -> str:
    """Normalise a requested tenant; a missing or empty value is the default one."""
    if value is None:
        return DEFAULT_TENANT_ID
    tenant = str(value)
    return tenant or DEFAULT_TENANT_ID


def tenant_of(metadata: Optional[Mapping[str, Any]]) -> str:
    """Resolve the tenant a task belongs to from the task metadata."""
    if not metadata:
        return DEFAULT_TENANT_ID
    return resolve_tenant_id(metadata.get("tenant_id"))


def validate_page_window(limit: int, offset: int) -> None:
    """Reject a page window outside the supported bounds."""
    if limit < MIN_PAGE_LIMIT or limit > MAX_PAGE_LIMIT:
        raise ValueError(
            f"limit must be between {MIN_PAGE_LIMIT} and {MAX_PAGE_LIMIT}, got {limit}"
        )
    if offset < 0:
        raise ValueError(f"offset must not be negative, got {offset}")


_T = TypeVar("_T")


def iter_chunks(
    items: Sequence[_T], size: int = LISTING_CHUNK_SIZE
) -> Iterator[Sequence[_T]]:
    """Split *items* into consecutive slices of at most *size* entries."""
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _sort_key(item: ActiveTaskItem) -> tuple[int, str]:
    """Newest first, ties broken by ascending task id for a stable page order.

    Task timestamps are compared as instants: a naive timestamp that survived a
    backend round trip is read as UTC rather than raising on comparison. The
    score is exact, so it agrees with the score an indexed backend stores.
    """
    return (sortable_created_at_score(item.created_at), item.task_id)


def build_active_task_page(
    items: Iterable[ActiveTaskItem], *, limit: int, offset: int
) -> ActiveTaskPage:
    """Order, deduplicate and page already-filtered active task items.

    Callers filter by tenant and active status before calling this, so paging
    never hides an entry behind a filtered-out one.
    """
    validate_page_window(limit, offset)

    ordered = sorted(items, key=_sort_key)
    unique: list[ActiveTaskItem] = []
    seen: set[str] = set()
    for item in ordered:
        if item.task_id in seen:
            continue
        seen.add(item.task_id)
        unique.append(item)

    window = unique[offset : offset + limit]
    return ActiveTaskPage(items=window, has_more=len(unique) > offset + limit)
