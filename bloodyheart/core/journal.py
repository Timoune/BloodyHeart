"""
bloodyheart.core.journal
────────────────────────
EventJournal — Immutable append-only log.
"""

import json
import logging
from pathlib import Path
from typing import Any

from .event import Event

logger = logging.getLogger(__name__)


class EventJournal:
    def __init__(self, path: str = "bloodyheart_journal.jsonl"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    async def append(self, event: Event) -> None:
        try:
            with open(self.path, "a") as f:
                f.write(json.dumps({
                    "timestamp": event.timestamp,
                    "source": event.source,
                    "destination": event.destination,
                    "event_type": event.event_type,
                    "version": event.version,
                    "payload": event.payload,
                    "priority": event.priority.value,
                }) + "\n")
        except Exception as e:
            logger.error("Failed to write to journal: %s", e)