"""
bloodyheart.core.compensation
─────────────────────────────
CompensationRegistry — v1.7 (Enhanced)

Kernel-level registry for tracking external side-effecting operations that
require compensation / rollback.

v1.7 Enhancements:
- Failure tracking with reasons
- Recovery API for pending compensations after restart/crash
- Timeout awareness for long-running external operations
- Statistics and health reporting
- Better querying by state and module
- Foundation for future automatic compensation triggering (BigArms integration)

This is still cooperative — the kernel tracks and surfaces what needs to be
undone. Full enforcement will come with process isolation in BigArms.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class CompensationRecord:
    """Record of an external operation that may need compensation."""
    token: str
    module: str
    operation: str
    started_at: str
    state: str = "PENDING"          # PENDING | COMPLETED | COMPENSATING | COMPENSATED | FAILED
    compensation_action: Optional[Dict[str, Any]] = None
    result: Optional[Dict[str, Any]] = None
    completed_at: Optional[str] = None
    failed_at: Optional[str] = None
    failure_reason: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    timeout_seconds: Optional[int] = None


class CompensationRegistry:
    """
    v1.7 Enhanced CompensationRegistry

    Use this to register any external operation that has side effects outside
    the kernel (API calls, file system changes via BigArms, database writes, etc.).

    The registry is persistent and can be used during recovery to find
    operations that were left in PENDING or COMPENSATING state.
    """

    def __init__(
        self,
        path: str = "bloodyheart_compensation.jsonl",
        default_timeout_seconds: Optional[int] = 300
    ):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.default_timeout_seconds = default_timeout_seconds
        self._records: Dict[str, CompensationRecord] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            with open(self.path, "r") as f:
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        rec = CompensationRecord(**data)
                        self._records[rec.token] = rec
        except Exception as e:
            logger.error("Failed to load compensation registry: %s", e)

    def _append(self, record: CompensationRecord) -> None:
        try:
            with open(self.path, "a") as f:
                f.write(json.dumps(asdict(record), default=str) + "\n")
        except Exception as e:
            logger.error("Failed to persist compensation record: %s", e)

    # === Core API ===

    def register(
        self,
        module: str,
        operation: str,
        compensation_action: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None
    ) -> str:
        """Register a new external operation that may require compensation."""
        token = f"comp_{uuid.uuid4().hex[:12]}"
        record = CompensationRecord(
            token=token,
            module=module,
            operation=operation,
            started_at=datetime.now(timezone.utc).isoformat(),
            compensation_action=compensation_action,
            metadata=metadata or {},
            timeout_seconds=timeout_seconds or self.default_timeout_seconds
        )
        self._records[token] = record
        self._append(record)
        logger.info("Registered compensation token=%s module=%s op=%s", token, module, operation)
        return token

    def mark_completed(self, token: str, result: Optional[Dict[str, Any]] = None) -> bool:
        if token not in self._records:
            logger.warning("mark_completed called on unknown token: %s", token)
            return False
        rec = self._records[token]
        rec.state = "COMPLETED"
        rec.result = result
        rec.completed_at = datetime.now(timezone.utc).isoformat()
        self._append(rec)
        return True

    def begin_compensation(self, token: str) -> bool:
        if token not in self._records:
            return False
        rec = self._records[token]
        rec.state = "COMPENSATING"
        self._append(rec)
        return True

    def mark_compensated(self, token: str) -> bool:
        if token not in self._records:
            return False
        rec = self._records[token]
        rec.state = "COMPENSATED"
        rec.completed_at = datetime.now(timezone.utc).isoformat()
        self._append(rec)
        return True

    def mark_failed(self, token: str, reason: str, auto_request_compensation: bool = True) -> bool:
        """
        Mark an operation as failed.
        If auto_request_compensation=True, the compensation action is surfaced
        for immediate or later execution.
        """
        if token not in self._records:
            return False
        rec = self._records[token]
        rec.state = "FAILED"
        rec.failure_reason = reason
        rec.failed_at = datetime.now(timezone.utc).isoformat()
        self._append(rec)
        logger.warning("Compensation token %s marked FAILED: %s", token, reason)

        if auto_request_compensation and rec.compensation_action:
            logger.info("Auto-requesting compensation for failed token %s", token)
            # The caller can use request_compensation(token) to get the action
        return True

    def request_compensation(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Request the compensation action for a given token.
        Returns the compensation_action dict if available, otherwise None.
        This is the main hook for triggering compensation logic.
        """
        rec = self._records.get(token)
        if not rec or not rec.compensation_action:
            return None
        if rec.state not in ("FAILED", "PENDING", "COMPENSATING"):
            logger.warning("request_compensation called on token %s in state %s", token, rec.state)
        return rec.compensation_action

    # === Recovery & Querying ===

    def get_pending(self) -> List[CompensationRecord]:
        """Return all operations that still need attention."""
        return [r for r in self._records.values() if r.state in ("PENDING", "COMPENSATING")]

    def get_by_token(self, token: str) -> Optional[CompensationRecord]:
        return self._records.get(token)

    def get_by_module(self, module: str) -> List[CompensationRecord]:
        return [r for r in self._records.values() if r.module == module]

    def list_by_state(self, state: str) -> List[CompensationRecord]:
        return [r for r in self._records.values() if r.state == state]

    def recover_pending(self) -> List[Dict[str, Any]]:
        """
        Recovery helper for use after restart or crash.
        Returns structured data ready for compensation execution.
        """
        pending = self.get_pending()
        return [
            {
                "token": r.token,
                "module": r.module,
                "operation": r.operation,
                "compensation_action": r.compensation_action,
                "started_at": r.started_at,
                "state": r.state,
                "metadata": r.metadata
            }
            for r in pending
        ]

    def get_overdue(self) -> List[CompensationRecord]:
        """Return operations that have exceeded their timeout."""
        now = datetime.now(timezone.utc)
        overdue = []
        for r in self._records.values():
            if r.state not in ("PENDING", "COMPENSATING"):
                continue
            if r.timeout_seconds is None:
                continue
            started = datetime.fromisoformat(r.started_at)
            if (now - started) > timedelta(seconds=r.timeout_seconds):
                overdue.append(r)
        return overdue

    # === Statistics ===

    def get_statistics(self) -> Dict[str, Any]:
        states = {}
        for r in self._records.values():
            states[r.state] = states.get(r.state, 0) + 1

        return {
            "total_records": len(self._records),
            "by_state": states,
            "pending_count": len(self.get_pending()),
            "overdue_count": len(self.get_overdue()),
        }

    def health_check(self) -> Dict[str, Any]:
        stats = self.get_statistics()
        overdue = self.get_overdue()
        return {
            "status": "degraded" if overdue else "healthy",
            "pending": stats["pending_count"],
            "overdue": stats["overdue_count"],
            "recommendation": "Run recovery for overdue items" if overdue else "All good"
        }