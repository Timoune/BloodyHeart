"""
bloodyheart.core.scheduler
──────────────────────────
PriorityScheduler v1.4 — Five-level priority queue with deterministic preemption for P0 emergencies.

v1.4 Security / Safety Feature:
- When a P0_SECURITY event is published (especially during S4/L4 escalations),
  the scheduler can actively cancel lower-priority running tasks using asyncio.Task.cancel().
- This guarantees that life/safety critical overrides get CPU immediately, even if
  heavy cognitive loops (long planning, simulation, reflection) are running.
- Modules doing long-running work should register their tasks via `register_running_task()`.
"""

from __future__ import annotations

import asyncio
import logging
import weakref
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Optional, Set

from .event import Event, Priority

logger = logging.getLogger(__name__)


class PriorityScheduler:
    """
    v1.4: Added deterministic preemption support for P0_SECURITY events.
    """

    def __init__(self, max_queue_size: int = 1000):
        self.max_queue_size = max_queue_size
        self._queues: Dict[Priority, asyncio.Queue] = {
            Priority.P0_SECURITY:    asyncio.Queue(maxsize=max_queue_size),
            Priority.P1_HUMAN:       asyncio.Queue(maxsize=max_queue_size),
            Priority.P2_AUTONOMOUS:  asyncio.Queue(maxsize=max_queue_size),
            Priority.P3_COGNITIVE:   asyncio.Queue(maxsize=max_queue_size),
            Priority.P4_MAINTENANCE: asyncio.Queue(maxsize=max_queue_size),
        }
        self._stats = {p: {"enqueued": 0, "dequeued": 0, "dropped": 0} for p in Priority}
        self._running = False
        self._processor_task: Optional[asyncio.Task] = None
        self._delivery_callback: Optional[Callable[[Event], Any]] = None
        self._logger = logging.getLogger(f"{__name__}.PriorityScheduler")

        # v1.4: Running task registry for preemption (weakrefs to avoid leaks)
        self._running_tasks: Dict[Priority, Set[weakref.ref]] = {p: set() for p in Priority}

    def set_delivery_callback(self, callback: Callable[[Event], Any]):
        self._delivery_callback = callback

    def register_running_task(self, priority: Priority, task: asyncio.Task) -> None:
        """Register a long-running task so it can be preempted on P0 emergencies."""
        if priority in self._running_tasks:
            self._running_tasks[priority].add(weakref.ref(task))

    def cancel_tasks_below(self, priority: Priority, reason: str = "P0 preemption") -> int:
        """
        Cancel all currently registered tasks with priority lower than the given one.
        Used for deterministic emergency preemption on S4/L4 or P0_SECURITY events.
        """
        cancelled = 0
        for prio in list(Priority):
            if prio.value >= priority.value:   # keep same or higher priority
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
                # Clean up dead refs
                if not task or task.done():
                    self._running_tasks[prio].discard(task_ref)
        return cancelled

    async def enqueue(self, event: Event) -> bool:
        q = self._queues.get(event.priority)
        if q is None:
            q = self._queues[Priority.P2_AUTONOMOUS]

        try:
            await q.put(event)
            self._stats[event.priority]["enqueued"] += 1

            # v1.4: If this is a P0_SECURITY event, trigger immediate preemption of lower work
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
        self._logger.info("PriorityScheduler background processor started.")
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
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.exception("Scheduler loop error: %s", e)
                await asyncio.sleep(0.1)

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