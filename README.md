# BloodyHeart v1.7.1 — BigArms Integration Update

This package contains the **BigArms integration module** for **BloodyHeart v1.7.1**.

## What's New in v1.7.1 Integration

- Deep integration with `CompensationRegistry`
- `BlockingTaskMonitor` awareness via scheduler callback
- `TaskBudgetWatchdog` integration for tool executions
- Built-in **circuit breaker** for BigArms connectivity resilience
- Automatic **Safe Mode escalation** + `SecurityEscalationMatrix` reporting on repeated failures
- Rich event payloads (`tool.execute.started`, detailed result/failed events)
- Full governance enforcement + HITL approval flow for ELEVATED capabilities
- Dry-run awareness from the kernel
- Operational metrics via `get_metrics()`

## How to Install

1. Copy `bloodyheart/bigarms_integration.py` into your existing `bloodyheart/` package.
2. Make sure you have `BigArms` (v0.7+) installed and the Named Pipe server running.
3. Wire it during BloodyHeart startup (see example below).

## Wiring Example

```python
from bloodyheart.kernel import BloodyHeart, BloodyHeartConfig
from bloodyheart.bigarms_integration import wire_bigarms_to_bloodyheart

kernel = BloodyHeart(BloodyHeartConfig(name="MiniVonKernel"))

# Wire BigArms integration (v1.7.1)
bigarms_executor = wire_bigarms_to_bloodyheart(kernel)

await kernel.start()
```

## Key Events

- `tool.execute.request` (from GhostMind)
- `tool.execute.started`
- `tool.execute.approval_required` (for ELEVATED)
- `tool.execute.result`
- `tool.execute.failed`

## Metrics

```python
metrics = bigarms_executor.get_metrics()
print(metrics)
```

## Notes

- This integration maintains **loose coupling** via `BigArmsNamedPipeClient`.
- All governance layers (Trust, SafeMode, Resource, Security, Watchdog) are enforced.
- Compensation is automatically registered and triggered on failure when applicable.

For questions or further customization, refer to the source or contact the developer.
