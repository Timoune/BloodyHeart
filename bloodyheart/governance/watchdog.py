"""
bloodyheart.governance.watchdog
───────────────────────────────
TaskBudgetWatchdog — Per-task cognitive budget tracking.

Modules (especially GhostMind) report token usage, reasoning cycles,
and approximate CPU time for individual tasks/tokens.

When a task exceeds its declared budget (from manifest), the watchdog
emits `watchdog.budget_exceeded` which SecurityEscalationMatrix can act on.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Optional

from ..core.event import Event, Priority
from ..manifest import ResourceLimits

if TYPE_CHECKING:
    from ..core.bus import CoreBus
    from ..core.registry import SchemaRegistry

logger = logging.getLogger(__name__)


@dataclass
class TaskBudget:
    task_id: str
    module_name: str
    cpu_budget_s: float
    token_budget: int
    reasoning_budget: int
    started_at: float = field(default_factory=time.time)
    cpu_used_s: float = 0.0
    tokens_used: int = 0
    reasoning_used: int = 0


class TaskBudgetWatchdog:
    """
    Tracks per-task budgets and raises violations on exhaustion.
    """

    def __init__(self, bus: Optional["CoreBus"] = None, registry: Optional["SchemaRegistry"] = None):
        self._bus = bus
        self._registry = registry
        self._active_tasks: Dict[str, TaskBudget] = {}
        self._module_limits: Dict[str, ResourceLimits] = {}
        self._logger = logging.getLogger(f"{__name__}.TaskBudgetWatchdog")

    def register_module(self, manifest: Any) -> None:
        if hasattr(manifest, "resource_limits"):
            self._module_limits[manifest.name] = manifest.resource_limits

    def start_task(
        self,
        task_id: str,
        module_name: str,
        cpu_budget_s: Optional[float] = None,
        token_budget: Optional[int] = None,
        reasoning_budget: Optional[int] = None,
    ) -> TaskBudget:
        limits = self._module_limits.get(module_name, ResourceLimits())
        budget = TaskBudget(
            task_id=task_id,
            module_name=module_name,
            cpu_budget_s=cpu_budget_s or (limits.cpu_budget * 3600 if limits.cpu_budget else 300),
            token_budget=token_budget or limits.token_budget or 100_000,
            reasoning_budget=reasoning_budget or limits.reasoning_budget or 50,
        )
        self._active_tasks[task_id] = budget
        return budget

    def report_usage(
        self,
        task_id: str,
        cpu_s: float = 0.0,
        tokens: int = 0,
        reasoning_cycles: int = 0,
    ) -> Optional[float]:
        """Report incremental usage. Returns remaining token fraction or None."""
        budget = self._active_tasks.get(task_id)
        if not budget:
            return None

        budget.cpu_used_s += cpu_s
        budget.tokens_used += tokens
        budget.reasoning_used += reasoning_cycles

        # Check thresholds
        token_pct = budget.tokens_used / max(budget.token_budget, 1)
        reasoning_pct = budget.reasoning_used / max(budget.reasoning_budget, 1)
        cpu_pct = budget.cpu_used_s / max(budget.cpu_budget_s, 1)

        max_pct = max(token_pct, reasoning_pct, cpu_pct)

        if max_pct >= 0.9:
            self._logger.warning(
                "Task %s approaching budget limit (%.1f%%)", task_id, max_pct * 100
            )

        if max_pct >= 1.0:
            self._emit_exceeded(budget, max_pct)
            # Optionally auto-pause or terminate the task here in future

        return max(0.0, 1.0 - max_pct)

    def end_task(self, task_id: str) -> Optional[TaskBudget]:
        return self._active_tasks.pop(task_id, None)

    def _emit_exceeded(self, budget: TaskBudget, usage_pct: float):
        if self._bus:
            asyncio.create_task(
                self._bus.publish(
                    Event(
                        source="BloodyHeart.TaskBudgetWatchdog",
                        destination=budget.module_name,
                        event_type="watchdog.budget_exceeded",
                        version="v1",
                        payload={
                            "task_id": budget.task_id,
                            "module": budget.module_name,
                            "budget_type": "combined",
                            "usage_pct": usage_pct,
                            "tokens_used": budget.tokens_used,
                            "reasoning_used": budget.reasoning_used,
                        },
                        priority=Priority.P0_SECURITY,
                    )
                )
            )
        self._logger.error(
            "BUDGET EXCEEDED task=%s module=%s tokens=%d/%d reasoning=%d/%d",
            budget.task_id, budget.module_name,
            budget.tokens_used, budget.token_budget,
            budget.reasoning_used, budget.reasoning_budget
        )

    def get_active_tasks(self) -> Dict[str, TaskBudget]:
        return self._active_tasks.copy()