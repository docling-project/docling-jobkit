"""Explicitly grant interactive authentication to trusted frontends."""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

_interactive_auth_allowed = ContextVar(
    "docling_jobkit_interactive_auth_allowed",
    default=False,
)


def is_interactive_auth_allowed() -> bool:
    return _interactive_auth_allowed.get()


@contextmanager
def allow_interactive_auth() -> Iterator[None]:
    """Allow browser-based authentication within a trusted interactive frontend."""
    token = _interactive_auth_allowed.set(True)
    try:
        yield
    finally:
        _interactive_auth_allowed.reset(token)
