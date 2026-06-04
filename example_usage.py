"""
example_usage.py
────────────────
Demonstration of the COMPLETE BloodyHeart v1 kernel with all governance layers.
"""

import asyncio
from bloodyheart.kernel import BloodyHeart, BloodyHeartConfig
from bloodyheart.manifest import ModuleManifest, TrustLevel, ResourceLimits, RecoveryPolicy

async def main():
    print("=== BloodyHeart v1 COMPLETE Demo ===")

    config = BloodyHeartConfig(name="MiniVonKernel", log_level="INFO")
    kernel = BloodyHeart(config=config)

    await kernel.start()

    # Register modules with rich manifests (now fully enforced)
    ghostmind = ModuleManifest(
        name="GhostMind",
        version="4.4",
        trust_level=TrustLevel.TRUST_CORE,
        dependencies=["DreamCloud"],
        resource_limits=ResourceLimits(cpu_budget=2.0, memory_limit_mb=2048, token_budget=50000),
        permissions=["read_state", "write_state", "emit_events"],
        recovery_policy=RecoveryPolicy(restart_attempts=3, escalation_level="L2"),
    )

    dreamcloud = ModuleManifest(
        name="DreamCloud",
        version="16",
        trust_level=TrustLevel.TRUST_CORE,
        dependencies=[],
        resource_limits=ResourceLimits(memory_limit_mb=4096),
        permissions=["read_state", "write_state"],
    )

    kernel.register_module(ghostmind)
    kernel.register_module(dreamcloud)

    print("Registered modules:", list(kernel.manifests.keys()))

    # State operations (MVCC + optimistic locking)
    version = await kernel.put_state("system.mode", "operational")
    print(f"State written at version {version}")

    value = await kernel.get_state("system.mode")
    print(f"Current system mode: {value}")

    # Transaction example
    tx = await kernel.begin_transaction()
    await kernel.transaction_manager.put(tx, "critical.config", {"safe": True})
    await kernel.transaction_manager.commit(tx)
    print("Transaction committed successfully")

    # Snapshot
    snap = await kernel.create_snapshot("demo-snapshot")
    print(f"Created snapshot: {snap.snapshot_id}")

    # Safety status
    print(f"Current Safe Mode: {kernel.current_safe_mode()}")

    # Simulate a budget report (would normally come from GhostMind)
    kernel.watchdog.start_task("task-001", "GhostMind", token_budget=1000)
    remaining = kernel.watchdog.report_usage("task-001", tokens=1200)
    print(f"Budget remaining after over-use: {remaining}")

    # Status
    status = kernel.get_status()
    print("Kernel Status:", status)

    await kernel.stop()
    print("=== Demo complete ===")


if __name__ == "__main__":
    asyncio.run(main())