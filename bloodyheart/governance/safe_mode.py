"""
bloodyheart.governance.safe_mode
────────────────────────────────
Hierarchical Safe Mode system (L1–L4) from original v1.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Optional

from ..core.event import Event, Priority

if TYPE_CHECKING:
    from ..core.bus import CoreBus

logger = logging.getLogger(__name__)


class SafeMode(Enum):
    NORMAL        = "NORMAL"
    L1_PARTIAL    = "L1_PARTIAL"
    L2_DEGRADED   = "L2_DEGRADED"
    L3_READ_ONLY  = "L3_READ_ONLY"
    L4_EMERGENCY  = "L4_EMERGENCY"

    @property
    def level(self) -> int:
        return {
            SafeMode.NORMAL: 0,
            SafeMode.L1_PARTIAL: 1,
            SafeMode.L2_DEGRADED: 2,
            SafeMode.L3_READ_ONLY: 3,
            SafeMode.L4_EMERGENCY: 4,
        }[self]

    def __lt__(self, other: "SafeMode") -> bool:
        return self.level < other.level

    def __le__(self, other: "SafeMode") -> bool:
        return self.level <= other.level


@dataclass
class SafeModeTransition:
    timestamp: str
    from_mode: SafeMode
    to_mode: SafeMode
    reason: str
    triggered_by: str


class SafeModeManager:
    def __init__(self, bus: Optional["CoreBus"] = None) -> None:
        self._bus: Optional["CoreBus"] = bus
        self._current_mode: SafeMode = SafeMode.NORMAL
        self._history: List[SafeModeTransition] = []
        self._logger = logging.getLogger(f"{__name__}.SafeModeManager")

    @property
    def current_mode(self) -> SafeMode:
        return self._current_mode

    def is_write_allowed(self) -> bool:
        return self._current_mode.level < SafeMode.L3_READ_ONLY.level

    def is_autonomous_allowed(self) -> bool:
        return self._current_mode.level < SafeMode.L2_DEGRADED.level

    def is_diagnostics_only(self) -> bool:
        return self._current_mode == SafeMode.L4_EMERGENCY

    async def escalate(self, target_mode: SafeMode, reason: str, triggered_by: str = "Unknown") -> bool:
        if target_mode <= self._current_mode:
            return False
        old_mode = self._current_mode
        self._current_mode = target_mode
        transition = SafeModeTransition(
            timestamp=datetime.now(timezone.utc).isoformat(),
            from_mode=old_mode,
            to_mode=target_mode,
            reason=reason,
            triggered_by=triggered_by,
        )
        self._history.append(transition)
        self._logger.warning("SAFE MODE ESCALATED: %s → %s | reason=%s | by=%s", old_mode.value, target_mode.value, reason, triggered_by)
        if self._bus:
            await self._bus.publish(Event(
                source="BloodyHeart.SafeModeManager",
                destination="*",
                event_type="system.safe_mode_changed",
                version="v1",
                payload={
                    "from_mode": old_mode.value,
                    "to_mode": target_mode.value,
                    "reason": reason,
                    "triggered_by": triggered_by,
                    "level": target_mode.level,
                },
                priority=Priority.P0_SECURITY,
            ))
        return True

    async def de_escalate(self, reason: str = "Manual de-escalation", triggered_by: str = "HumanOperator") -> bool:
        if self._current_mode == SafeMode.NORMAL:
            return False
        level_map = {m.level: m for m in SafeMode}
        target_level = max(0, self._current_mode.level - 1)
        target = level_map[target_level]
        old_mode = self._current_mode
        self._current_mode = target
        transition = SafeModeTransition(
            timestamp=datetime.now(timezone.utc).isoformat(),
            from_mode=old_mode,
            to_mode=target,
            reason=reason,
            triggered_by=triggered_by,
        )
        self._history.append(transition)
        self._logger.info("SAFE MODE DE-ESCALATED: %s → %s | reason=%s", old_mode.value, target.value, reason)
        if self._bus:
            await self._bus.publish(Event(
                source="BloodyHeart.SafeModeManager",
                destination="*",
                event_type="system.safe_mode_changed",
                version="v1",
                payload={
                    "from_mode": old_mode.value,
                    "to_mode": target.value,
                    "reason": reason,
                    "triggered_by": triggered_by,
                    "level": target.level,
                },
                priority=Priority.P1_HUMAN,
            ))
        return True

    def get_history(self, limit: int = 50) -> List[SafeModeTransition]:
        return self._history[-limit:]

    def reset_to_normal(self, reason: str = "Emergency reset") -> None:
        self._current_mode = SafeMode.NORMAL
        self._logger.critical("SAFE MODE FORCE RESET TO NORMAL: %s", reason)
