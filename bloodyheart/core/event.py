"""
bloodyheart.core.event
──────────────────────
Core event and priority definitions.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional
from datetime import datetime, timezone


class Priority(Enum):
    P0_SECURITY    = "P0_SECURITY"
    P1_HUMAN       = "P1_HUMAN"
    P2_AUTONOMOUS  = "P2_AUTONOMOUS"
    P3_COGNITIVE   = "P3_COGNITIVE"
    P4_MAINTENANCE = "P4_MAINTENANCE"


@dataclass
class Event:
    source: str
    destination: str
    event_type: str
    version: str = "v1"
    payload: Dict[str, Any] = field(default_factory=dict)
    priority: Priority = Priority.P2_AUTONOMOUS
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    correlation_id: Optional[str] = None