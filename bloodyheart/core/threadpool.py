"""
bloodyheart.core.threadpool
───────────────────
ThreadPoolManager (v1.5) — Safe offloading of blocking / CPU-heavy synchronous work.

Used by modules that need to call sync libraries (e.g. heavy vision models, file I/O sinks,
Voicy/MightyEyes style processors) without blocking the async CoreBus event loop.

Features:
- Isolated ThreadPoolExecutor per "sink" type (or shared default)
- Future tracking + graceful shutdown
- Compatible with PriorityScheduler preemption (tasks can be cancelled)
"""

from __future__ import annotations

import atexit
import concurrent.futures
import logging
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)


class ThreadPoolManager:
    """
    Manages one or more ThreadPoolExecutors for offloading sync work.

    Default pool is used for general offloading.
    Named pools can be created for specific heavy sinks (e.g. "vision", "audio").
    """

    def __init__(self, default_max_workers: int = 4):
        self._default_workers = default_max_workers
        self._pools: Dict[str, concurrent.futures.ThreadPoolExecutor] = {}
        self._default_pool: Optional[concurrent.futures.ThreadPoolExecutor] = None
        self._logger = logging.getLogger(f"{__name__}.ThreadPoolManager")

        atexit.register(self.shutdown)

    def _get_default_pool(self) -> concurrent.futures.ThreadPoolExecutor:
        if self._default_pool is None or self._default_pool._shutdown:
            self._default_pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=self._default_workers,
                thread_name_prefix="BloodyHeart-Default"
            )
            self._logger.debug("Created default ThreadPoolExecutor with %d workers", self._default_workers)
        return self._default_pool

    def submit(self, fn: Callable, *args, pool_name: Optional[str] = None, **kwargs) -> concurrent.futures.Future:
        """
        Submit work to a thread pool.

        If pool_name is given, uses (or creates) a dedicated pool for that name.
        Otherwise uses the shared default pool.
        """
        if pool_name:
            if pool_name not in self._pools:
                self._pools[pool_name] = concurrent.futures.ThreadPoolExecutor(
                    max_workers=max(2, self._default_workers // 2),
                    thread_name_prefix=f"BloodyHeart-{pool_name}"
                )
                self._logger.debug("Created dedicated pool '%s'", pool_name)
            executor = self._pools[pool_name]
        else:
            executor = self._get_default_pool()

        future = executor.submit(fn, *args, **kwargs)
        return future

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown all pools. Called automatically at exit."""
        if self._default_pool:
            self._default_pool.shutdown(wait=wait)
        for name, pool in self._pools.items():
            pool.shutdown(wait=wait)
            self._logger.debug("Shutdown pool '%s'", name)
        self._pools.clear()
        self._default_pool = None
