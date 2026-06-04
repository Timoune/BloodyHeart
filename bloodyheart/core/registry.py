"""
bloodyheart.core.registry
─────────────────────────
SchemaRegistry v1.2 — Event schema validation with strict mode support.

Previously (v1.1): unknown schemas always returned True → complete bypass of validation.
v1.2:
- Configurable allow_unknown (default=True for backward compat with existing modules).
- When allow_unknown=False, unknown event_type+version combinations are rejected.
- Integrated into CoreBus.publish path (see bus.py).
- Still lightweight; for full JSON Schema enforcement later, register jsonschema validators.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict

logger = logging.getLogger(__name__)


class SchemaRegistry:
    def __init__(self, allow_unknown: bool = True):
        self._schemas: Dict[str, Callable] = {}
        self.allow_unknown = allow_unknown
        self._logger = logging.getLogger(f"{__name__}.SchemaRegistry")

    def register(self, event_type: str, version: str, validator: Callable[[dict], bool]) -> None:
        key = f"{event_type}.{version}"
        self._schemas[key] = validator
        self._logger.debug("Registered schema validator for %s", key)

    def validate(self, event_type: str, version: str, payload: dict) -> bool:
        key = f"{event_type}.{version}"
        validator = self._schemas.get(key)
        if validator:
            try:
                return bool(validator(payload))
            except Exception as e:
                self._logger.error("Schema validator for %s raised: %s", key, e)
                return False

        if not self.allow_unknown:
            self._logger.warning(
                "Rejecting event with unknown schema: event_type=%s version=%s (strict mode)",
                event_type, version
            )
            return False

        # Permissive mode (default) — log once per unknown type to aid debugging
        self._logger.debug("Allowing unknown schema (permissive): %s.%s", event_type, version)
        return True