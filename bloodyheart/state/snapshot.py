"""
bloodyheart.state.snapshot
──────────────────────────
SnapshotManager (from original).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from .store import MVCCStateStore

logger = logging.getLogger(__name__)


@dataclass
class StateSnapshot:
    snapshot_id: str
    created_at: str
    store_name: str
    data: Dict[str, Dict[str, Any]]


class SnapshotManager:
    def __init__(self, store: "MVCCStateStore", snapshot_dir: str = "bloodyheart_snapshots"):
        self._store = store
        self._snapshot_dir = Path(snapshot_dir)
        self._snapshot_dir.mkdir(parents=True, exist_ok=True)
        self._logger = logging.getLogger(f"{__name__}.SnapshotManager")

    async def create_snapshot(self, snapshot_id: Optional[str] = None) -> StateSnapshot:
        if snapshot_id is None:
            snapshot_id = f"snapshot_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        current_state = self._store.snapshot_state()
        snapshot_data = {}
        for key, vv in current_state.items():
            snapshot_data[key] = {
                "value": vv.value,
                "version": vv.version,
                "updated_at": vv.updated_at,
                "updated_by": vv.updated_by,
            }
        snapshot = StateSnapshot(
            snapshot_id=snapshot_id,
            created_at=datetime.now(timezone.utc).isoformat(),
            store_name=getattr(self._store, "_name", "default"),
            data=snapshot_data,
        )
        filepath = self._snapshot_dir / f"{snapshot_id}.json"
        with open(filepath, "w") as f:
            json.dump({
                "snapshot_id": snapshot.snapshot_id,
                "created_at": snapshot.created_at,
                "store_name": snapshot.store_name,
                "data": snapshot.data,
            }, f, indent=2, default=str)
        self._logger.info("Created snapshot: %s (%d keys)", snapshot_id, len(snapshot_data))
        return snapshot

    def load_snapshot(self, snapshot_id: str) -> Optional[StateSnapshot]:
        filepath = self._snapshot_dir / f"{snapshot_id}.json"
        if not filepath.exists():
            return None
        with open(filepath) as f:
            raw = json.load(f)
        return StateSnapshot(
            snapshot_id=raw["snapshot_id"],
            created_at=raw["created_at"],
            store_name=raw["store_name"],
            data=raw["data"],
        )

    async def restore_snapshot(self, snapshot: StateSnapshot) -> int:
        restored = 0
        for key, item in snapshot.data.items():
            try:
                await self._store.put(key, item["value"], expected_version=None, updated_by="SnapshotRestore")
                restored += 1
            except Exception as e:
                self._logger.error("Failed to restore key %s: %s", key, e)
        self._logger.warning("Restored %d keys from snapshot %s", restored, snapshot.snapshot_id)
        return restored