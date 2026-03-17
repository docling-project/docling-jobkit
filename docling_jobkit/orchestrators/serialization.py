"""Shared serialization utilities for orchestrators."""


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
