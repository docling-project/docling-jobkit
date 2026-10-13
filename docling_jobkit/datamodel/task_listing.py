"""Compact, tenant-scoped listings of active tasks.

Every orchestrator backend returns the same shape, ordering and paging rules
for a tenant's in-flight work, so the filtering/sorting/paging rules live here
instead of being reimplemented per backend. The listing is deliberately
identity-level: an item never carries sources, targets, options or results.
"""

import datetime
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any, Optional, TypeVar

from pydantic import BaseModel, Field

from docling.datamodel.service.tasks import TaskType

from docling_jobkit.datamodel.task_meta import TaskStatus

# Tasks whose tenant is not recorded in the task metadata belong to this tenant,
# matching how the orchestrators and the serving layer resolve a tenant.
DEFAULT_TENANT_ID = "default"

# A listing only reports work that has not reached a terminal state.
ACTIVE_TASK_STATUSES = frozenset({TaskStatus.PENDING, TaskStatus.STARTED})

MIN_PAGE_LIMIT = 1
MAX_PAGE_LIMIT = 100

# Queued IDs and task metadata are read in chunks of this size so a deep
# queue never turns into one unbounded Redis read. The active-task set itself
# is read with a single SMEMBERS, so chunking does not bound that one call.
LISTING_CHUNK_SIZE = 100


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


def _sort_key(item: ActiveTaskItem) -> tuple[float, str]:
    """Newest first, ties broken by ascending task id for a stable page order.

    Task timestamps are compared as instants: a naive timestamp that survived a
    backend round trip is read as UTC rather than raising on comparison.
    """
    created_at = item.created_at
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=datetime.timezone.utc)
    return (-created_at.timestamp(), item.task_id)


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
