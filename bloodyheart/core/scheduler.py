"""
bloodyheart.core.scheduler
──────────────────────────
PriorityScheduler v1.7 — Five-level priority queue with deterministic preemption
and BlockingTaskMonitor for non-cooperative modules.

v1.7 improvements:
- BlockingTaskMonitor: detects tasks that have not yielded for too long
- Automatic escalation when cooperative cancellation is insufficient
- Better statistics and observability for safety decisions
"""

from __future__ import annotations

import asyncio
import logging
import time
import weakref
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Optional, Set

from .event import Event, Priority

logger = logging.getLogger(__name__)


class PriorityScheduler:
    """
    v1.7: Added BlockingTaskMonitor for detection of modules that refuse to yield.
    This is a critical hardening step while we move toward process isolation (BigArms).
    """

    def __init__(self, max_queue_size: int = 1000, blocking_threshold_seconds: float = 5.0):
        self.max_queue_size = max_queue_size
        self.blocking_threshold = blocking_threshold_seconds

        self._queues: Dict[Priority, asyncio.Queue] = {
            Priority.P0_SECURITY:    asyncio.Queue(maxsize=max_queue_size),
            Priority.P1_HUMAN:       asyncio.Queue(maxsize=max_queue_size),
            Priority.P2_AUTONOMOUS:  asyncio.Queue(maxsize=max_queue_size),
            Priority.P3_COGNITIVE:   asyncio.Queue(maxsize=max_queue_size),
            Priority.P4_MAINTENANCE: asyncio.Queue(maxsize=max_queue_size),
        }
        self._stats = {p: {"enqueued": 0, "dequeued": 0, "dropped": 0, "blocked": 0} for p in Priority}
        self._running = False
        self._processor_task: Optional[asyncio.Task] = None
        self._delivery_callback: Optional[Callable[[Event], Any]] = None
        self._logger = logging.getLogger(f"{__name__}.PriorityScheduler")

        # v1.4: Running task registry for preemption
        self._running_tasks: Dict[Priority, Set[weakref.ref]] = {p: set() for p in Priority}

        # v1.7: Blocking task monitoring
        self._task_start_times: Dict[weakref.ref, float] = {}
        self._blocking_escalation_callback: Optional[Callable[[str, float], Any]] = None

    def set_delivery_callback(self, callback: Callable[[Event], Any]):
        self._delivery_callback = callback

    def set_blocking_escalation_callback(self, callback: Callable[[str, float], Any]):
        """Register a callback that will be called when a task is detected as blocking."""
        self._blocking_escalation_callback = callback

    def register_running_task(self, priority: Priority, task: asyncio.Task) -> None:
        """Register a long-running task so it can be preempted and monitored for blocking."""
        if priority in self._running_tasks:
            ref = weakref.ref(task)
            self._running_tasks[priority].add(ref)
            self._task_start_times[ref] = time.monotonic()

    def cancel_tasks_below(self, priority: Priority, reason: str = "P0 preemption") -> int:
        """
        Cancel all currently registered tasks with priority lower than the given one.
        Used for deterministic emergency preemption on S4/L4 or P0_SECURITY events.
        """
        cancelled = 0
        for prio in list(Priority):
            if prio.value >= priority.value:
                continue
            for task_ref in list(self._running_tasks.get(prio, set())):
                task = task_ref()
                if task and not task.done():
                    try:
                        task.cancel()
                        cancelled += 1
                        self._logger.warning("Preempted task (prio=%s) due to %s", prio.value, reason)
                    except Exception as e:
                        self._logger.error("Failed to cancel task: %s", e)
                if not task or task.done():
                    self._running_tasks[prio].discard(task_ref)
                    self._task_start_times.pop(task_ref, None)
        return cancelled

    async def enqueue(self, event: Event) -> bool:
        q = self._queues.get(event.priority)
        if q is None:
            q = self._queues[Priority.P2_AUTONOMOUS]

        try:
            await q.put(event)
            self._stats[event.priority]["enqueued"] += 1

            if event.priority == Priority.P0_SECURITY:
                self.cancel_tasks_below(Priority.P0_SECURITY, reason="P0_SECURITY override")

            return True
        except asyncio.QueueFull:
            self._stats[event.priority]["dropped"] += 1
            self._logger.warning("Queue full for priority %s — event dropped", event.priority.value)
            return False

    async def get_next(self, timeout: Optional[float] = None) -> Optional[Event]:
        for prio in [Priority.P0_SECURITY, Priority.P1_HUMAN, Priority.P2_AUTONOMOUS,
                     Priority.P3_COGNITIVE, Priority.P4_MAINTENANCE]:
            q = self._queues[prio]
            if not q.empty():
                try:
                    if timeout is None:
                        event = q.get_nowait()
                    else:
                        event = await asyncio.wait_for(q.get(), timeout=timeout)
                    self._stats[prio]["dequeued"] += 1
                    return event
                except asyncio.QueueEmpty:
                    continue
        return None

    async def run(self):
        self._running = True
        self._logger.info("PriorityScheduler background processor started (v1.7 with blocking monitor).")

        last_check = time.monotonic()

        while self._running:
            try:
                event = await self.get_next(timeout=0.05)
                if event and self._delivery_callback:
                    try:
                        if asyncio.iscoroutinefunction(self._delivery_callback):
                            await self._delivery_callback(event)
                        else:
                            self._delivery_callback(event)
                    except Exception as e:
                        self._logger.error("Delivery callback error for %s: %s", event.event_type, e)
                else:
                    await asyncio.sleep(0.01)

                # v1.7: Periodic blocking check
                now = time.monotonic()
                if now - last_check > 1.0:
                    self._check_for_blocking_tasks()
                    last_check = now

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.exception("Scheduler loop error: %s", e)
                await asyncio.sleep(0.1)

    def _check_for_blocking_tasks(self):
        """v1.7: Detect tasks that have been running without yielding for too long."""
        now = time.monotonic()
        blocked = []

        for prio, task_refs in self._running_tasks.items():
            for ref in list(task_refs):
                start = self._task_start_times.get(ref)
                if start is None:
                    continue
                age = now - start
                if age > self.blocking_threshold:
                    task = ref()
                    if task and not task.done():
                        blocked.append((prio, task, age))
                        self._stats[prio]["blocked"] += 1
                    else:
                        # Clean up dead task
                        task_refs.discard(ref)
                        self._task_start_times.pop(ref, None)

        if blocked and self._blocking_escalation_callback:
            for prio, task, age in blocked:
                try:
                    self._blocking_escalation_callback(str(prio), age)
                except Exception as e:
                    self._logger.error("Blocking escalation callback failed: %s", e)

    def start_background_processor(self):
        if self._processor_task is None or self._processor_task.done():
            self._processor_task = asyncio.create_task(self.run())

    async def stop(self):
        self._running = False
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
        self._logger.info("PriorityScheduler stopped.")

    def get_stats(self) -> Dict[str, Any]:
        return {p.value: self._stats[p] for p in Priority}

    def qsize(self) -> Dict[str, int]:
        return {p.value: q.qsize() for p, q in self._queues.items()}