"""Shared serialization utilities for orchestrators."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, SecretBytes, SecretStr


def _restore_secret_values(raw: Any, dumped: Any) -> Any:
    """Restore secret values into a dumped structure for trusted transports."""
    if isinstance(raw, (SecretStr, SecretBytes)):
        return raw.get_secret_value()

    if isinstance(raw, dict) and isinstance(dumped, dict):
        return {key: _restore_secret_values(raw[key], dumped[key]) for key in dumped}

    if isinstance(raw, list) and isinstance(dumped, list):
        return [
            _restore_secret_values(raw_item, dumped_item)
            for raw_item, dumped_item in zip(raw, dumped)
        ]

    return dumped


def dump_model_with_secrets(
    model: BaseModel,
    *,
    exclude_none: bool = False,
    serialize_as_any: bool = False,
) -> Any:
    """Dump a model for trusted internal transport with secrets restored."""
    dumped = model.model_dump(
        mode="json",
        exclude_none=exclude_none,
        serialize_as_any=serialize_as_any,
    )
    raw = model.model_dump(
        mode="python",
        exclude_none=exclude_none,
        serialize_as_any=serialize_as_any,
    )
    return _restore_secret_values(raw, dumped)


def make_msgpack_safe(obj):
    """Recursively convert any non-msgpack-serializable types to safe types.

    This function handles Pydantic types (like AnyUrl, datetime, Decimal) and
    other Python objects that msgpack cannot serialize directly, keeping bytes unchanged.

    Args:
        obj: Any Python object to make msgpack-safe

    Returns:
        A msgpack-serializable version of the object
    """
    from datetime import datetime
    from decimal import Decimal

    # Types msgpack already supports
    if obj is None or isinstance(obj, (str, int, float, bool, bytes)):
        return obj

    # Handle sequences
    if isinstance(obj, (list, tuple, set)):
        return [make_msgpack_safe(v) for v in obj]

    # Handle mappings
    if isinstance(obj, dict):
        return {make_msgpack_safe(k): make_msgpack_safe(v) for k, v in obj.items()}

    # Known common conversions
    if isinstance(obj, (datetime, Decimal)):
        return str(obj)  # ISO for datetime, str for Decimal

    # Fallback: use string representation
    return str(obj)
