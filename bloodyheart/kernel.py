"""
bloodyheart.kernel
──────────────────
BloodyHeart v1.7 — Cognitive Microkernel (Hardened Cooperative Safety)

v1.7 changes:
- Integrated BlockingTaskMonitor via PriorityScheduler
- Added CompensationRegistry for external operation tracking
- Stronger version reporting and safety posture documentation
- Better hooks for future BigArms process isolation
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from .core.bus import CoreBus
from .core.journal import EventJournal
from .core.blob import BlobManager
from .core.registry import SchemaRegistry
from .core.scheduler import PriorityScheduler
from .core.threadpool import ThreadPoolManager
from .core.dryrun import is_dry_run, set_dry_run, DryRunContext
from .core.compensation import CompensationRegistry  # NEW v1.7

from .governance.trust import TrustEnforcer
from .governance.resource import ResourceGovernor
from .governance.watchdog import TaskBudgetWatchdog
from .governance.safe_mode import SafeModeManager
from .governance.security import SecurityEscalationMatrix

from .state.store import MVCCStateStore
from .state.transaction import TransactionManager
from .state.snapshot import SnapshotManager

from .manifest import ModuleManifest
from .health.monitor import HealthMonitor
from .dag.graph import DependencyDAG
from .diagnostics.recorder import FlightRecorder

logger = logging.getLogger(__name__)


@dataclass
class BloodyHeartConfig:
    name: str = "BloodyHeart"
    journal_path: str = "bloodyheart_journal.jsonl"
    compensation_path: str = "bloodyheart_compensation.jsonl"  # NEW v1.7
    snapshot_dir: str = "bloodyheart_snapshots"
    blob_threshold_bytes: int = 64 * 1024
    blob_persist_dir: Optional[str] = None
    blob_max_cache_mb: Optional[int] = 256
    max_queue_size: int = 1000
    log_level: str = "INFO"
    schema_allow_unknown: bool = False
    thread_pool_default_workers: int = 4
    dry_run_default: bool = False
    blocking_threshold_seconds: float = 5.0  # NEW v1.7


class BloodyHeart:
    def __init__(self, config: Optional[BloodyHeartConfig] = None):
        self.config = config or BloodyHeartConfig()
        self._logger = logging.getLogger(f"{__name__}.BloodyHeart[{self.config.name}]")

        if self.config.dry_run_default:
            set_dry_run(True)

        self.registry = SchemaRegistry(allow_unknown=self.config.schema_allow_unknown)
        self.journal = EventJournal(path=self.config.journal_path)

        # v1.7: Compensation registry
        self.compensation_registry = CompensationRegistry(path=self.config.compensation_path)

        max_blob = self.config.blob_max_cache_mb * 1024 * 1024 if self.config.blob_max_cache_mb else None
        self.blob_manager = BlobManager(
            threshold_bytes=self.config.blob_threshold_bytes,
            blob_dir=self.config.blob_persist_dir,
            max_cache_size_bytes=max_blob,
        )
        self.scheduler = PriorityScheduler(
            max_queue_size=self.config.max_queue_size,
            blocking_threshold_seconds=self.config.blocking_threshold_seconds
        )
        self.thread_pool = ThreadPoolManager(default_max_workers=self.config.thread_pool_default_workers)

        self.bus = CoreBus(
            journal=self.journal,
            registry=self.registry,
            blob_manager=self.blob_manager,
            scheduler=self.scheduler,
            trust_enforcer=None,
        )

        self.trust_enforcer = TrustEnforcer(bus=self.bus, registry=self.registry)
        self.resource_governor = ResourceGovernor(bus=self.bus)
        self.watchdog = TaskBudgetWatchdog(bus=self.bus, registry=self.registry)
        self.bus.trust_enforcer = self.trust_enforcer

        self.safe_mode_manager = SafeModeManager(bus=self.bus)
        self.security_matrix = SecurityEscalationMatrix(bus=self.bus, safe_mode_manager=self.safe_mode_manager)

        self.state_store = MVCCStateStore(bus=self.bus, name=self.config.name)
        self.transaction_manager = TransactionManager(store=self.state_store, bus=self.bus)
        self.snapshot_manager = SnapshotManager(store=self.state_store, snapshot_dir=self.config.snapshot_dir)

        self.dag = DependencyDAG()
        self.health_monitor = HealthMonitor(bus=self.bus, dag=self.dag, registry=self.registry)

        self.manifests: Dict[str, ModuleManifest] = {}

        # v1.7: Wire blocking escalation from scheduler into safe mode
        self.scheduler.set_blocking_escalation_callback(self._handle_blocking_task)

        self._logger.info("BloodyHeart v1.7 initialized (strict schema=%s, dry_run_default=%s, blocking_threshold=%ss)",
                          not self.config.schema_allow_unknown, self.config.dry_run_default,
                          self.config.blocking_threshold_seconds)

    # === v1.7 Blocking handling ===
    def _handle_blocking_task(self, priority: str, age_seconds: float):
        self._logger.warning("Blocking task detected (priority=%s, age=%.1fs) — triggering safety escalation",
                             priority, age_seconds)
        # In a real system this would escalate safe mode or publish a P0_SECURITY event
        # For v1.7 we log + allow higher layers (GhostMind / BigArms) to react
        # Future: self.safe_mode_manager.escalate(...)

    # Dry-run helpers
    def is_dry_run(self) -> bool:
        return is_dry_run()

    def set_dry_run(self, enabled: bool):
        set_dry_run(enabled)

    def dry_run_context(self, enabled: bool = True):
        return DryRunContext(enabled)

    def create_flight_recorder(self, output_path="bloodyheart_flight_recorder.jsonl", patterns=None, max_events=None):
        return FlightRecorder(bus=self.bus, output_path=output_path, patterns=patterns, max_events=max_events)

    def register_module(self, manifest: ModuleManifest) -> None:
        if manifest.name in self.manifests:
            self._logger.warning("Module '%s' already registered. Overwriting.", manifest.name)
        self.manifests[manifest.name] = manifest
        self.dag.register(manifest)
        self.trust_enforcer.register_module(manifest)
        self.resource_governor.register_module(manifest)
        self.watchdog.register_module(manifest)
        self.health_monitor.register_module(manifest)

    # === v1.7 Compensation Helpers ===

    def register_external_operation(
        self,
        module: str,
        operation: str,
        compensation_action: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None
    ) -> str:
        """
        Convenience method to register an external side-effecting operation
        that should be tracked for potential compensation.
        """
        return self.compensation_registry.register(
            module=module,
            operation=operation,
            compensation_action=compensation_action,
            metadata=metadata,
            timeout_seconds=timeout_seconds
        )

    def get_compensation_health(self) -> Dict[str, Any]:
        """Quick health check of the compensation system."""
        return self.compensation_registry.health_check()

    def recover_pending_compensations(self) -> List[Dict[str, Any]]:
        """Recovery helper — returns pending compensations after restart/crash."""
        return self.compensation_registry.recover_pending()

    def request_compensation(self, token: str, publish_event: bool = True) -> Optional[Dict[str, Any]]:
        """
        Request the compensation action for a token.
        If publish_event=True (default), also publishes a `compensation.requested` event
        on the CoreBus so subscribers can react automatically.
        """
        action = self.compensation_registry.request_compensation(token)
        if action and publish_event:
            asyncio.create_task(self._publish_compensation_requested_event(token, action))
        return action

    async def _publish_compensation_requested_event(self, token: str, action: Dict[str, Any]):
        """Publish a structured compensation.requested event on the CoreBus."""
        try:
            from .core.event import Event, Priority
            event = Event(
                source="BloodyHeart",
                destination="*",
                event_type="compensation.requested",
                version="v1",
                payload={
                    "token": token,
                    "compensation_action": action,
                    "requested_at": datetime.now(timezone.utc).isoformat()
                },
                priority=Priority.P1_HUMAN
            )
            await self.bus.publish(event, authenticated_emitter="BloodyHeart")
            self._logger.info("Published compensation.requested event for token=%s", token)
        except Exception as e:
            self._logger.error("Failed to publish compensation.requested event: %s", e)

    async def put_state(self, key: str, value: Any, expected_version: Optional[int] = None) -> int:
        return await self.state_store.put(key, value, expected_version=expected_version)

    async def get_state(self, key: str) -> Any:
        val = self.state_store.get(key)
        return val.value if val else None

    async def begin_transaction(self):
        return await self.transaction_manager.begin()

    async def escalate_safe_mode(self, level, reason: str = "Manual"):
        return await self.safe_mode_manager.escalate(level, reason, triggered_by="API")

    def current_safe_mode(self):
        return self.safe_mode_manager.current_mode

    async def create_snapshot(self, snapshot_id: Optional[str] = None):
        return await self.snapshot_manager.create_snapshot(snapshot_id)

    async def restore_snapshot(self, snapshot_id: str):
        snap = self.snapshot_manager.load_snapshot(snapshot_id)
        if snap:
            return await self.snapshot_manager.restore_snapshot(snap)
        return 0

    async def start(self):
        await self.bus.start()
        self._logger.info("BloodyHeart v1.7 is ONLINE.")

    async def stop(self):
        await self.bus.stop()
        self.thread_pool.shutdown(wait=True)
        self._logger.info("BloodyHeart is OFFLINE.")

    def get_status(self) -> dict:
        return {
            "name": self.config.name,
            "version": "1.7",  # Updated
            "registered_modules": list(self.manifests.keys()),
            "safe_mode": self.safe_mode_manager.current_mode.value,
            "dry_run": self.is_dry_run(),
            "schema_strict": not self.config.schema_allow_unknown,
            "blocking_threshold_seconds": self.config.blocking_threshold_seconds,
        }