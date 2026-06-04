"""
bloodyheart.governance.resource
───────────────────────────────
ResourceGovernor v1.2 — Tracks declared resource limits vs actual usage.

v1.2: Clarified CPU budget math and added early-warning at 80% for CPU.
The original threshold (cpu_budget hours → seconds) is intentional for "daily aggregate"
accounting. Tight per-task / infinite-loop protection should use TaskBudgetWatchdog
or BigArms OS-level limits (cgroups, CPU quota).
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Optional

from ..core.event import Event, Priority
from ..manifest import ModuleManifest, ResourceLimits

if TYPE_CHECKING:
    from ..core.bus import CoreBus

logger = logging.getLogger(__name__)


@dataclass
class ModuleResourceUsage:
    cpu_time_ms: float = 0.0
    memory_mb: float = 0.0
    tokens_used: int = 0
    reasoning_cycles: int = 0
    last_updated: float = field(default_factory=time.time)


class ResourceGovernor:
    """
    Lightweight resource tracking and enforcement.
    Emits watchdog.budget_exceeded on violation (consumed by SecurityEscalationMatrix).
    """

    def __init__(self, bus: Optional["CoreBus"] = None):
        self._bus = bus
        self._limits: Dict[str, ResourceLimits] = {}
        self._usage: Dict[str, ModuleResourceUsage] = {}
        self._logger = logging.getLogger(f"{__name__}.ResourceGovernor")

    def register_module(self, manifest: ModuleManifest) -> None:
        self._limits[manifest.name] = manifest.resource_limits
        if manifest.name not in self._usage:
            self._usage[manifest.name] = ModuleResourceUsage()
        self._logger.debug("Resource limits registered for %s: %s", manifest.name, manifest.resource_limits)

    def get_limits(self, module_name: str) -> Optional[ResourceLimits]:
        return self._limits.get(module_name)

    def record_usage(
        self,
        module_name: str,
        cpu_ms: float = 0.0,
        memory_mb: float = 0.0,
        tokens: int = 0,
        reasoning_cycles: int = 0,
    ) -> None:
        if module_name not in self._usage:
            self._usage[module_name] = ModuleResourceUsage()

        u = self._usage[module_name]
        u.cpu_time_ms += cpu_ms
        u.memory_mb = max(u.memory_mb, memory_mb)
        u.tokens_used += tokens
        u.reasoning_cycles += reasoning_cycles
        u.last_updated = time.time()

        self._check_budgets(module_name)

    def _check_budgets(self, module_name: str) -> None:
        limits = self._limits.get(module_name)
        usage = self._usage.get(module_name)
        if not limits or not usage:
            return

        violations = []
        cpu_seconds_used = usage.cpu_time_ms / 1000.0
        allowed_cpu_seconds = (limits.cpu_budget or 1.0) * 3600.0

        if limits.cpu_budget and cpu_seconds_used > allowed_cpu_seconds:
            violations.append(("cpu", cpu_seconds_used, allowed_cpu_seconds))

        if limits.token_budget and usage.tokens_used > limits.token_budget:
            violations.append(("tokens", usage.tokens_used, limits.token_budget))

        if limits.reasoning_budget and usage.reasoning_cycles > limits.reasoning_budget:
            violations.append(("reasoning", usage.reasoning_cycles, limits.reasoning_budget))

        for budget_type, used, limit in violations:
            self._logger.warning(
                "Budget exceeded for %s: %s used=%.1f limit=%.1f",
                module_name, budget_type, used, limit
            )
            if self._bus:
                asyncio.create_task(
                    self._emit_budget_exceeded(module_name, budget_type, min(used / max(limit, 1), 1.5))
                )

        # Early warning for CPU (80%)
        if limits.cpu_budget and cpu_seconds_used > 0.8 * allowed_cpu_seconds:
            self._logger.info(
                "CPU budget 80%% warning for %s: %.1fs / %.1fs",
                module_name, cpu_seconds_used, allowed_cpu_seconds
            )

    async def _emit_budget_exceeded(self, module_name: str, budget_type: str, usage_pct: float):
        await self._bus.publish(
            Event(
                source="BloodyHeart.ResourceGovernor",
                destination=module_name,
                event_type="watchdog.budget_exceeded",
                version="v1",
                payload={
                    "module": module_name,
                    "budget_type": budget_type,
                    "usage_pct": usage_pct,
                },
                priority=Priority.P0_SECURITY,
            ),
            authenticated_emitter="BloodyHeart.ResourceGovernor",
        )

    def get_usage(self, module_name: str) -> Optional[ModuleResourceUsage]:
        return self._usage.get(module_name)

    def reset_usage(self, module_name: str):
        if module_name in self._usage:
            self._usage[module_name] = ModuleResourceUsage()