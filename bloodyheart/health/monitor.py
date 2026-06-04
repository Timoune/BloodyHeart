"""
bloodyheart.health.monitor
──────────────────────────
HealthMonitor (from original v1, lightly enhanced for recovery_policy awareness).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Any, Callable

from .states import HealthState

logger = logging.getLogger(__name__)


@dataclass
class ModuleHealth:
    state: HealthState = HealthState.HEALTHY
    last_check: float = field(default_factory=time.time)
    latency_ms: float = 0.0
    error_count: int = 0
    capabilities_lost: List[str] = field(default_factory=list)


class HealthMonitor:
    def __init__(self, bus: Optional[Any] = None, dag: Optional[Any] = None, registry: Optional[Any] = None):
        self.bus = bus
        self.dag = dag
        self.registry = registry
        self._modules: Dict[str, ModuleHealth] = {}
        self._on_state_change: List[Callable[[str, HealthState, HealthState], None]] = []
        self._logger = logging.getLogger(f"{__name__}.HealthMonitor")

    def register_module(self, manifest: Any) -> None:
        name = getattr(manifest, "name", str(manifest))
        if name not in self._modules:
            self._modules[name] = ModuleHealth()
            self._logger.debug("Registered module '%s' in HealthMonitor", name)

    def set_state(self, module_name: str, new_state: HealthState, reason: str = "") -> None:
        if module_name not in self._modules:
            self._modules[module_name] = ModuleHealth()
        current = self._modules[module_name]
        old_state = current.state
        if old_state != new_state:
            current.state = new_state
            current.last_check = time.time()
            self._logger.warning("Module '%s' health changed: %s → %s (%s)", module_name, old_state.value, new_state.value, reason)
            for callback in self._on_state_change:
                try:
                    callback(module_name, old_state, new_state)
                except Exception as e:
                    self._logger.error("Error in health state change callback: %s", e)
            if self.dag and new_state in (HealthState.UNHEALTHY, HealthState.FAILED):
                affected = self.dag.propagate_failure(module_name)
                for affected_module in affected:
                    self._logger.info("Health degradation propagated to '%s' due to '%s'", affected_module, module_name)

    def record_latency(self, module_name: str, latency_ms: float) -> None:
        if module_name in self._modules:
            self._modules[module_name].latency_ms = latency_ms

    def record_error(self, module_name: str) -> None:
        if module_name in self._modules:
            self._modules[module_name].error_count += 1

    def get_state(self, module_name: str) -> Optional[HealthState]:
        health = self._modules.get(module_name)
        return health.state if health else None

    def get_module_health(self, module_name: str) -> Optional[ModuleHealth]:
        return self._modules.get(module_name)

    def get_all_states(self) -> Dict[str, HealthState]:
        return {name: h.state for name, h in self._modules.items()}

    def add_state_change_listener(self, callback: Callable[[str, HealthState, HealthState], None]) -> None:
        self._on_state_change.append(callback)

    def check_thresholds(self, module_name: str, degraded_ms: int, unhealthy_ms: int) -> None:
        health = self._modules.get(module_name)
        if not health:
            return
        if health.latency_ms > unhealthy_ms:
            self.set_state(module_name, HealthState.UNHEALTHY, "High latency")
        elif health.latency_ms > degraded_ms:
            self.set_state(module_name, HealthState.DEGRADED, "Elevated latency")