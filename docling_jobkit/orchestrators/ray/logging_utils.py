"""Logging helpers for Ray actors and Serve replicas."""

import logging
import sys


def configure_ray_actor_logging(level: str) -> None:
    """Ensure actor logs are emitted to stderr for Ray log capture."""
    level = level.upper()
    logging.basicConfig(
        level=level,
        format="%(levelname)s:\t%(asctime)s - %(name)s - %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
        force=True,
    )
    logging.getLogger().setLevel(level)
