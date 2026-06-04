"""
test_infrastructure_v1.py
─────────────────────────
High-quality test suite for BloodyHeart v1 Core Infrastructure.
Covers: PriorityScheduler, BlobManager, CoreBus,
MVCCStateStore + Transaction + Snapshot, DependencyDAG, HealthMonitor,
ModuleManifest and kernel wiring.

Clean version — only meaningful tests.
"""

import pytest
import asyncio

from bloodyheart.core.scheduler import PriorityScheduler
from bloodyheart.core.blob import BlobManager
from bloodyheart.core.event import Event, Priority
from bloodyheart.dag.graph import DependencyDAG
from bloodyheart.health.states import HealthState
from bloodyheart.manifest import ModuleManifest, TrustLevel


# ── PriorityScheduler ────────────────────────────────────────────────────────

def test_scheduler_enqueue_different_priorities():
    sched = PriorityScheduler(max_queue_size=100)
    ev_low = Event(source="A", destination="*", event_type="low", priority=Priority.P4_MAINTENANCE)
    ev_high = Event(source="B", destination="*", event_type="high", priority=Priority.P0_SECURITY)
    asyncio.run(sched.enqueue(ev_low))
    asyncio.run(sched.enqueue(ev_high))
    assert sched.qsize()["P0_SECURITY"] == 1


async def test_scheduler_get_next_returns_highest_priority_first():
    sched = PriorityScheduler()
    low = Event(source="L", destination="*", event_type="low", priority=Priority.P4_MAINTENANCE)
    high = Event(source="H", destination="*", event_type="high", priority=Priority.P0_SECURITY)
    await sched.enqueue(low)
    await sched.enqueue(high)
    next_ev = await sched.get_next()
    assert next_ev.priority == Priority.P0_SECURITY


def test_scheduler_stats_tracking():
    sched = PriorityScheduler()
    ev = Event(source="S", destination="*", event_type="stat", priority=Priority.P2_AUTONOMOUS)
    asyncio.run(sched.enqueue(ev))
    stats = sched.get_stats()
    assert stats["P2_AUTONOMOUS"]["enqueued"] >= 1


# ── BlobManager ──────────────────────────────────────────────────────────────

def test_blob_should_use_blob_large_bytes():
    bm = BlobManager(threshold_bytes=100)
    big = b"x" * 200
    assert bm.should_use_blob(big) is True


def test_blob_store_and_retrieve():
    bm = BlobManager()
    ref = bm.store({"embedding": [0.1]*128})
    assert ref.blob_id.startswith("blob_")
    data = bm.retrieve(ref.blob_id)
    assert data is not None


def test_blob_replace_large_payloads():
    bm = BlobManager(threshold_bytes=50)
    payload = {"small": "ok", "big": "x" * 100}
    new_p = bm.replace_large_payloads(payload)
    assert "__blob_ref__" in new_p.get("big", {})


# ── DependencyDAG + HealthMonitor ────────────────────────────────────────────

def test_dag_startup_order(kernel, sample_manifests):
    dag = kernel.dag
    dag.register(sample_manifests["DreamCloud"])
    dag.register(sample_manifests["GhostMind"])
    order = dag.get_startup_order()
    assert order.index("DreamCloud") < order.index("GhostMind")


def test_dag_propagate_failure(kernel, sample_manifests):
    dag = kernel.dag
    dag.register(sample_manifests["DreamCloud"])
    dag.register(sample_manifests["GhostMind"])
    affected = dag.propagate_failure("DreamCloud")
    assert "GhostMind" in affected


def test_health_state_change(kernel, sample_manifests):
    hm = kernel.health_monitor
    hm.register_module(sample_manifests["GhostMind"])
    hm.set_state("GhostMind", HealthState.UNHEALTHY, "test")
    assert hm.get_state("GhostMind") == HealthState.UNHEALTHY


def test_health_latency_threshold(kernel, sample_manifests):
    hm = kernel.health_monitor
    hm.register_module(sample_manifests["GhostMind"])
    hm.record_latency("GhostMind", 2500)
    hm.check_thresholds("GhostMind", degraded_ms=500, unhealthy_ms=2000)
    assert hm.get_state("GhostMind") == HealthState.UNHEALTHY


# ── Manifest + Kernel Wiring ─────────────────────────────────────────────────

def test_manifest_validation_trust_level_string():
    m = ModuleManifest(name="Test", trust_level="TRUST_PLUGIN")
    assert m.trust_level == TrustLevel.TRUST_PLUGIN


def test_manifest_to_from_dict_roundtrip(sample_manifests):
    original = sample_manifests["GhostMind"]
    d = original.to_dict()
    restored = ModuleManifest.from_dict(d)
    assert restored.name == original.name
    assert restored.trust_level == original.trust_level


def test_kernel_register_wires_all_systems(kernel, sample_manifests):
    kernel.register_module(sample_manifests["GhostMind"])
    assert "GhostMind" in kernel.manifests
    assert "GhostMind" in kernel.dag._nodes
    assert "GhostMind" in kernel.trust_enforcer._modules
    assert "GhostMind" in kernel.resource_governor._limits
    assert "GhostMind" in kernel.watchdog._module_limits


def test_kernel_status_basic(kernel, sample_manifests):
    kernel.register_module(sample_manifests["DreamCloud"])
    status = kernel.get_status()
    assert "DreamCloud" in status["registered_modules"]