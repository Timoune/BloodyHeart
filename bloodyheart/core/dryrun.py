"""
bloodyheart.core.dryrun
───────────────────────
Dry-run / simulation mode support (v1.6).

Allows high-stakes operations (BigArms tool calls, external APIs, state changes with side effects)
to be simulated safely before committing. Used via context manager or global flag.

Kernel exposes:
    kernel.is_dry_run()
    kernel.set_dry_run(True)
    with kernel.dry_run_context():
        ...
"""

from __future__ import annotations

import contextlib
from typing import Iterator

_dry_run_enabled: bool = False


def is_dry_run() -> bool:
    """Return whether the kernel is currently in dry-run / simulation mode."""
    return _dry_run_enabled


def set_dry_run(enabled: bool) -> None:
    """Globally enable or disable dry-run mode."""
    global _dry_run_enabled
    _dry_run_enabled = bool(enabled)


class DryRunContext(contextlib.AbstractContextManager):
    """
    Context manager for temporary dry-run mode.

    Example:
        with DryRunContext():
            # all operations inside are simulated
            ...
    """

    def __init__(self, enabled: bool = True):
        self._enabled = enabled
        self._previous_state: bool = False

    def __enter__(self) -> "DryRunContext":
        self._previous_state = is_dry_run()
        set_dry_run(self._enabled)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        set_dry_run(self._previous_state)
        return False  # do not suppress exceptions