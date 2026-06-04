"""
bloodyheart.compensator
───────────────────────
RobustCompensator — Production-grade compensation handler for BloodyHeart v1.7+

Features:
- Subscribes to `compensation.requested` events on the CoreBus
- Retry with exponential backoff + jitter
- Per-attempt timeout protection
- Transient vs Permanent error classification
- Dead-Letter Queue (DLQ) for permanently failed compensations
- Rich metrics (success rate, latency, retries, DLQ count)
- Simple circuit breaker
- Clean integration with CompensationRegistry

Usage:
    from bloodyheart.compensator import RobustCompensator

    compensator = RobustCompensator(kernel)
    kernel.bus.subscribe("compensation.requested", compensator.handle)
"""

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .core.event import Event
from .kernel import BloodyHeart

logger = logging.getLogger(__name__)


# =============================================================================
# Exceptions
# =============================================================================
class CompensationError(Exception):
    """Base exception for compensation failures."""
    pass


class TransientCompensationError(CompensationError):
    """Temporary failure — safe to retry."""
    pass


class PermanentCompensationError(CompensationError):
    """Permanent failure — send to DLQ, do not retry."""
    pass


# =============================================================================
# Metrics
# =============================================================================
@dataclass
class CompensationMetrics:
    total_requested: int = 0
    total_succeeded: int = 0
    total_permanent_failed: int = 0
    total_transient_failed: int = 0
    total_retries: int = 0
    total_dlq_entries: int = 0
    latencies_ms: List[float] = field(default_factory=list)

    def record_success(self, latency_ms: float):
        self.total_succeeded += 1
        self.latencies_ms.append(latency_ms)

    def record_permanent_failure(self):
        self.total_permanent_failed += 1

    def record_transient_failure(self):
        self.total_transient_failed += 1

    def record_retry(self):
        self.total_retries += 1

    def record_dlq(self):
        self.total_dlq_entries += 1

    def avg_latency_ms(self) -> float:
        if not self.latencies_ms:
            return 0.0
        return sum(self.latencies_ms) / len(self.latencies_ms)

    def to_dict(self) -> Dict[str, Any]:
        total = max(1, self.total_requested)
        return {
            "total_requested": self.total_requested,
            "succeeded": self.total_succeeded,
            "permanent_failed": self.total_permanent_failed,
            "transient_failed": self.total_transient_failed,
            "retries": self.total_retries,
            "dlq_entries": self.total_dlq_entries,
            "avg_latency_ms": round(self.avg_latency_ms(), 2),
            "success_rate": round(self.total_succeeded / total, 3),
        }


# =============================================================================
# Dead Letter Queue
# =============================================================================
class DeadLetterQueue:
    """Persistent storage for compensations that have permanently failed."""

    def __init__(self, path: str = "bloodyheart_compensation_dlq.jsonl"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def add(self, token: str, action: Dict[str, Any], reason: str):
        entry = {
            "token": token,
            "compensation_action": action,
            "failure_reason": reason,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            with open(self.path, "a") as f:
                f.write(json.dumps(entry, default=str) + "\n")
            logger.warning(f"Compensation moved to DLQ | token={token}")
        except Exception as e:
            logger.error(f"Failed to write to DLQ: {e}")

    def get_all(self) -> List[Dict[str, Any]]:
        if not self.path.exists():
            return []
        try:
            with open(self.path, "r") as f:
                return [json.loads(line) for line in f if line.strip()]
        except Exception as e:
            logger.error(f"Failed to read DLQ: {e}")
            return []


# =============================================================================
# Robust Compensator
# =============================================================================
class RobustCompensator:
    """
    Production-ready compensator for BloodyHeart.

    Handles compensation.requested events with retries, DLQ, metrics,
    and circuit breaker protection.
    """

    def __init__(
        self,
        kernel: BloodyHeart,
        max_retries: int = 3,
        base_backoff_seconds: float = 0.5,
        timeout_seconds: float = 25.0,
        dlq_path: str = "bloodyheart_compensation_dlq.jsonl"
    ):
        self.kernel = kernel
        self.dlq = DeadLetterQueue(dlq_path)
        self.metrics = CompensationMetrics()

        self.max_retries = max_retries
        self.base_backoff = base_backoff_seconds
        self.timeout = timeout_seconds

        # Circuit breaker state
        self.consecutive_permanent_failures = 0
        self.circuit_open = False

    async def handle(self, event: Event):
        """Main handler for compensation.requested events."""
        payload = event.payload or {}
        token = payload.get("token")
        action = payload.get("compensation_action")

        if not token or not action:
            logger.error("Received invalid compensation.requested event")
            return

        self.metrics.total_requested += 1
        start_time = time.monotonic()

        self.kernel.compensation_registry.begin_compensation(token)

        success = await self._execute_with_retries(token, action)
        latency_ms = (time.monotonic() - start_time) * 1000

        if success:
            self.metrics.record_success(latency_ms)
            self.consecutive_permanent_failures = 0
            self.circuit_open = False

    async def _execute_with_retries(self, token: str, action: Dict[str, Any]) -> bool:
        for attempt in range(1, self.max_retries + 1):
            try:
                if self.circuit_open:
                    logger.warning("Circuit breaker is open — skipping compensation")
                    return False

                logger.info(f"Compensation attempt {attempt}/{self.max_retries} | token={token}")

                await asyncio.wait_for(
                    self._run_compensation_action(action, token),
                    timeout=self.timeout
                )

                self.kernel.compensation_registry.mark_compensated(token)
                logger.info(f"Compensation succeeded | token={token}")
                return True

            except PermanentCompensationError as e:
                logger.error(f"Permanent compensation failure | token={token} | {e}")
                self.metrics.record_permanent_failure()
                self.kernel.compensation_registry.mark_failed(token, reason=str(e))
                self.dlq.add(token, action, str(e))
                self.metrics.record_dlq()

                self.consecutive_permanent_failures += 1
                if self.consecutive_permanent_failures >= 5:
                    self.circuit_open = True
                    logger.warning("Circuit breaker OPENED due to repeated permanent failures")

                return False

            except (TransientCompensationError, asyncio.TimeoutError) as e:
                self.metrics.record_transient_failure()
                self.metrics.record_retry()
                logger.warning(f"Transient failure on attempt {attempt} | token={token} | {e}")

                if attempt == self.max_retries:
                    reason = f"Failed after {self.max_retries} retries: {e}"
                    self.kernel.compensation_registry.mark_failed(token, reason=reason)
                    self.dlq.add(token, action, reason)
                    self.metrics.record_dlq()
                    return False

                # Exponential backoff + jitter
                delay = self.base_backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.3)
                await asyncio.sleep(delay)

            except Exception as e:
                logger.exception(f"Unexpected error during compensation | token={token}")
                self.kernel.compensation_registry.mark_failed(token, reason=f"Unexpected: {e}")
                self.dlq.add(token, action, f"Unexpected: {e}")
                self.metrics.record_dlq()
                return False

        return False

    async def _run_compensation_action(self, action: Dict[str, Any], token: str):
        """Execute the actual compensation logic. Override or extend in subclasses."""
        comp_type = action.get("type", "unknown")

        # Simulated implementations — replace with real BigArms calls in production
        if comp_type == "undo_api_call":
            await asyncio.sleep(0.12)
        elif comp_type == "delete_blob":
            await asyncio.sleep(0.07)
        elif comp_type == "rollback_transaction":
            await asyncio.sleep(0.22)
        elif comp_type == "delete_file":
            await asyncio.sleep(0.05)
        else:
            raise PermanentCompensationError(f"Unsupported compensation type: {comp_type}")

    def get_status(self) -> Dict[str, Any]:
        """Return current metrics and health information."""
        return {
            "metrics": self.metrics.to_dict(),
            "circuit_breaker_open": self.circuit_open,
            "consecutive_permanent_failures": self.consecutive_permanent_failures,
            "dlq_size": len(self.dlq.get_all()),
        }
