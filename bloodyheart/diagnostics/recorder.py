"""
bloodyheart.diagnostics.recorder
───────────────────
FlightRecorder (v1.5) — Structured JSONL black-box / flight data recorder for CoreBus.

Subscribes to events (optionally filtered by fnmatch patterns) and appends them
to a JSONL file for post-mortem analysis, debugging, and HITL review.

Can be created via:
    recorder = kernel.create_flight_recorder(
        output_path="bloodyheart_flight_recorder.jsonl",
        patterns=["*"],           # or ["security.*", "state.*"]
        max_events=10000
    )
"""

from __future__ import annotations

import asyncio
import fnmatch
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, List, Optional

if TYPE_CHECKING:
    from ..core.bus import CoreBus
    from ..core.event import Event

logger = logging.getLogger(__name__)


class FlightRecorder:
    """
    Black-box recorder that listens to CoreBus events and writes them to JSONL.
    """

    def __init__(
        self,
        bus: "CoreBus",
        output_path: str = "bloodyheart_flight_recorder.jsonl",
        patterns: Optional[List[str]] = None,
        max_events: Optional[int] = None,
    ):
        self.bus = bus
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.patterns = patterns or ["*"]  # match everything by default
        self.max_events = max_events
        self._event_count = 0
        self._logger = logging.getLogger(f"{__name__}.FlightRecorder")

        # Subscribe to all events (we filter inside)
        self.bus.subscribe("*", self._handle_event)  # wildcard handled by our logic
        # Note: CoreBus subscribe uses exact event_type; we do broad matching here.

        self._logger.info(
            "FlightRecorder initialized → %s (patterns=%s, max_events=%s)",
            self.output_path, self.patterns, self.max_events
        )

    def _matches(self, event_type: str) -> bool:
        for pat in self.patterns:
            if fnmatch.fnmatch(event_type, pat):
                return True
        return False

    async def _handle_event(self, event: "Event") -> None:
        if not self._matches(event.event_type):
            return

        if self.max_events is not None and self._event_count >= self.max_events:
            return

        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": event.event_type,
            "source": event.source,
            "destination": event.destination,
            "priority": event.priority.value,
            "version": event.version,
            "correlation_id": event.correlation_id,
            "payload": event.payload,
        }

        try:
            with open(self.output_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, default=str) + "\n")
            self._event_count += 1
        except Exception as e:
            self._logger.error("Failed to write flight record: %s", e)

    def get_stats(self) -> dict:
        return {
            "output_path": str(self.output_path),
            "events_recorded": self._event_count,
            "patterns": self.patterns,
            "max_events": self.max_events,
        }
