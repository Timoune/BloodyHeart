"""
bloodyheart.core.bus
────────────────────
CoreBus v1.4 — Central event backbone with priority scheduling + governance + preemption.

v1.4:
- Passes `authenticated_emitter` to BlobManager so per-module blob quotas are enforced.
- Triggers scheduler preemption on P0_SECURITY events.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from .event import Event, Priority

if TYPE_CHECKING:
    from .journal import EventJournal
    from .blob import BlobManager
    from .registry import SchemaRegistry
    from .scheduler import PriorityScheduler
    from ..governance.trust import TrustEnforcer

logger = logging.getLogger(__name__)


class CoreBus:
    def __init__(
        self,
        journal: Optional["EventJournal"] = None,
        registry: Optional["SchemaRegistry"] = None,
        blob_manager: Optional["BlobManager"] = None,
        scheduler: Optional["PriorityScheduler"] = None,
        trust_enforcer: Optional["TrustEnforcer"] = None,
    ):
        self.journal = journal
        self.registry = registry
        self.blob_manager = blob_manager
        self.scheduler = scheduler
        self.trust_enforcer = trust_enforcer

        self._subscribers: Dict[str, List[Callable]] = {}
        self._running = False
        self._logger = logging.getLogger(f"{__name__}.CoreBus")

        if self.scheduler:
            self.scheduler.set_delivery_callback(self._deliver_to_subscribers)

    async def start(self):
        self._running = True
        if self.scheduler:
            self.scheduler.start_background_processor()
        self._logger.info("CoreBus started (priority scheduling + preemption enabled).")

    async def stop(self):
        self._running = False
        if self.scheduler:
            await self.scheduler.stop()
        self._logger.info("CoreBus stopped.")

    async def publish(
        self,
        event: Event,
        authenticated_emitter: Optional[str] = None,
    ) -> None:
        trust_source = authenticated_emitter or event.source

        if self.journal:
            await self.journal.append(event)

        if self.registry:
            if not self.registry.validate(event.event_type, event.version, event.payload or {}):
                self._logger.warning("Schema validation failed for %s from %s", event.event_type, trust_source)
                if self.trust_enforcer:
                    await self.trust_enforcer._emit_violation(
                        source=trust_source,
                        violation_type="schema_violation",
                        reason=f"Event '{event.event_type}' failed schema validation",
                        details={"event_type": event.event_type},
                    )
                return

        if self.trust_enforcer:
            allowed = await self.trust_enforcer.validate_event(event, authenticated_emitter=authenticated_emitter)
            if not allowed:
                return

        # v1.4: Pass emitter to BlobManager for per-module quota enforcement
        if self.blob_manager and event.payload:
            try:
                event.payload = self.blob_manager.replace_large_payloads(event.payload, emitter=authenticated_emitter)
            except PermissionError as e:
                self._logger.warning("Blob quota violation from %s: %s", trust_source, e)
                if self.trust_enforcer:
                    await self.trust_enforcer._emit_violation(
                        source=trust_source,
                        violation_type="blob_quota_exceeded",
                        reason=str(e),
                        details={"emitter": authenticated_emitter},
                    )
                return

        if self.scheduler:
            await self.scheduler.enqueue(event)
        else:
            await self._deliver_to_subscribers(event)

        self._logger.debug("Published event: %s from %s (emitter=%s)", event.event_type, event.source, trust_source)

    def subscribe(self, event_type: str, callback: Callable[[Event], Any]):
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(callback)

    async def _deliver_to_subscribers(self, event: Event):
        if event.event_type in self._subscribers:
            for callback in self._subscribers[event.event_type]:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(event)
                    else:
                        callback(event)
                except Exception as e:
                    self._logger.error("Subscriber error for %s: %s", event.event_type, e)

    async def request(self, event: Event, timeout: float = 5.0) -> Optional[Event]:
        await self.publish(event)
        return None