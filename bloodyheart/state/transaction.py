"""
bloodyheart.state.transaction
─────────────────────────────
TransactionManager v1.5 — MVCC transactions with compensation awareness.

v1.5 Emphasis:
- Internal state rollback is clean and atomic.
- **External side-effects** (network calls, API mutations, file writes outside the KV store,
  hardware commands, etc.) are **explicitly NOT rolled back** by this manager.
- All compensation for external actions **MUST** be handled by BigArms-registered undo tools.
- **Critical Rule**: Every compensation/undo tool you register **MUST be strictly idempotent**.
  If a rollback chain is interrupted and later re-executed, non-idempotent undos will cause
  duplicate effects, data corruption, or cascading failures.
- Recommended pattern: Use unique operation IDs + "already undone" guards in your undo tools.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, List, Optional

from ..core.event import Event, Priority

if TYPE_CHECKING:
    from ..core.bus import CoreBus
    from .store import MVCCStateStore

logger = logging.getLogger(__name__)


class TxStatus(Enum):
    ACTIVE = "ACTIVE"
    COMMITTED = "COMMITTED"
    ROLLED_BACK = "ROLLED_BACK"
    FAILED = "FAILED"


@dataclass
class TxOperation:
    op_type: str
    key: str
    old_value: Any = None
    old_version: Optional[int] = None
    new_value: Any = None


@dataclass
class Transaction:
    tx_id: str
    status: TxStatus = TxStatus.ACTIVE
    operations: List[TxOperation] = field(default_factory=list)
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    completed_at: Optional[str] = None


class TransactionManager:
    def __init__(self, store: "MVCCStateStore", bus: Optional["CoreBus"] = None):
        self._store = store
        self._bus = bus
        self._active_transactions: Dict[str, Transaction] = {}
        self._logger = logging.getLogger(f"{__name__}.TransactionManager")

    async def begin(self, tx_id: Optional[str] = None) -> Transaction:
        if tx_id is None:
            tx_id = f"tx_{uuid.uuid4().hex[:12]}"
        tx = Transaction(tx_id=tx_id)
        self._active_transactions[tx_id] = tx
        return tx

    async def put(self, tx: Transaction, key: str, value: Any, expected_version: Optional[int] = None) -> None:
        if tx.status != TxStatus.ACTIVE:
            raise RuntimeError(f"Transaction {tx.tx_id} is not active")
        current = self._store.get(key)
        op = TxOperation("put", key, current.value if current else None, current.version if current else None, value)
        tx.operations.append(op)
        await self._store.put(key, value, expected_version=expected_version)

    async def delete(self, tx: Transaction, key: str, expected_version: Optional[int] = None) -> None:
        if tx.status != TxStatus.ACTIVE:
            raise RuntimeError(f"Transaction {tx.tx_id} is not active")
        current = self._store.get(key)
        if current is None:
            return
        op = TxOperation("delete", key, current.value, current.version)
        tx.operations.append(op)
        await self._store.delete(key, expected_version=expected_version)

    async def commit(self, tx: Transaction) -> bool:
        if tx.status != TxStatus.ACTIVE:
            return False
        tx.status = TxStatus.COMMITTED
        tx.completed_at = datetime.now(timezone.utc).isoformat()
        if self._bus:
            await self._bus.publish(Event(
                source="BloodyHeart.TransactionManager",
                destination="*",
                event_type="tx.committed",
                payload={"tx_id": tx.tx_id, "operations": len(tx.operations)},
                priority=Priority.P2_AUTONOMOUS,
            ))
        self._active_transactions.pop(tx.tx_id, None)
        return True

    async def rollback(self, tx: Transaction) -> bool:
        if tx.status != TxStatus.ACTIVE:
            return False
        tx.status = TxStatus.ROLLED_BACK
        tx.completed_at = datetime.now(timezone.utc).isoformat()

        for op in reversed(tx.operations):
            try:
                if op.op_type == "put":
                    if op.old_value is not None:
                        await self._store.put(op.key, op.old_value, expected_version=None)
                    else:
                        await self._store.delete(op.key, expected_version=None)
                elif op.op_type == "delete":
                    if op.old_value is not None:
                        await self._store.put(op.key, op.old_value, expected_version=None)
            except Exception as e:
                self._logger.error("Compensation failed for key=%s: %s", op.key, e)

        if self._bus:
            await self._bus.publish(Event(
                source="BloodyHeart.TransactionManager",
                destination="*",
                event_type="tx.rolled_back",
                payload={"tx_id": tx.tx_id},
                priority=Priority.P0_SECURITY,
            ))
        self._active_transactions.pop(tx.tx_id, None)
        return True