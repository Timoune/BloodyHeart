"""
example_compensator.py
──────────────────────
Thin demo showing how to use the official RobustCompensator module.

Run with:
    python example_compensator.py
"""

import asyncio
import json

from bloodyheart import BloodyHeart, BloodyHeartConfig
from bloodyheart.compensator import RobustCompensator


async def main():
    print("=== BloodyHeart v1.7 RobustCompensator Demo ===\n")

    kernel = BloodyHeart(BloodyHeartConfig(name="CompensatorDemo", log_level="INFO"))
    await kernel.start()

    # Create the official compensator
    compensator = RobustCompensator(kernel=kernel, max_retries=3)

    # Subscribe it to compensation events
    kernel.bus.subscribe("compensation.requested", compensator.handle)
    print("[Demo] RobustCompensator subscribed to compensation.requested events\n")

    # Demo 1: Successful compensation
    print("--- Demo 1: Successful compensation ---")
    token1 = kernel.register_external_operation(
        module="GhostMind",
        operation="call_payment_api",
        compensation_action={"type": "undo_api_call", "details": {"endpoint": "/payments/reverse"}}
    )
    kernel.compensation_registry.mark_failed(token1, reason="Service unavailable")
    kernel.request_compensation(token1)
    await asyncio.sleep(1.2)

    # Demo 2: Permanent failure (goes to DLQ)
    print("\n--- Demo 2: Permanent failure → DLQ ---")
    token2 = kernel.register_external_operation(
        module="DreamCloud",
        operation="persist_state",
        compensation_action={"type": "delete_blob", "blob_id": "blob_42"}
    )
    kernel.compensation_registry.mark_failed(token2, reason="Invalid blob state")
    kernel.request_compensation(token2)
    await asyncio.sleep(0.8)

    # Show status
    print("\n=== Compensator Status ===")
    status = compensator.get_status()
    print(json.dumps(status, indent=2))

    await kernel.stop()
    print("\n=== Demo Complete ===")


if __name__ == "__main__":
    asyncio.run(main())
