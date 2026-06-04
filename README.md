# BloodyHeart v1.6.1 — Cognitive Microkernel for Mini Von

**Complete hardened release** (June 2026)

This is the full, production-ready BloodyHeart v1.6.1 microkernel with all cumulative hardening from the development roadmap:

- **v1.2**: TrustLevel as IntEnum (correct ordering), authenticated_emitter + system impersonation protection, MVCC asyncio.Lock, LRU BlobManager, Schema strict mode
- **v1.3**: Single massive blob handling, MVCC exponential backoff retry on contention
- **v1.4**: Per-emitter blob quota isolation (DoS prevention), deterministic P0_SECURITY preemption via Task.cancel()
- **v1.5**: ThreadPoolManager for sync offloading, FlightRecorder (black-box JSONL), strong idempotency requirements for compensation
- **v1.6**: ViolationType enum (structured classification), DryRunContext for safe simulation, strict schema default in config
- **v1.6.1**: `handle_budget_exceeded` now consistently routes through `ViolationType.BUDGET_EXHAUSTION`

## Key Features
- Priority-scheduled CoreBus with preemption
- MVCC state store + transactions (external side-effects explicitly not rolled back — use BigArms compensation)
- Hierarchical safe modes (L1–L4) + S1–S4 security escalation
- TrustEnforcer + ResourceGovernor + TaskBudgetWatchdog
- BlobManager with per-emitter quotas + LRU hot cache + disk persistence
- HealthMonitor + DependencyDAG
- Dry-run / simulation mode
- Flight data recorder for observability

## Usage
```python
from bloodyheart.kernel import BloodyHeart, BloodyHeartConfig

kernel = BloodyHeart(BloodyHeartConfig(
    name="MiniVonKernel",
    dry_run_default=False,
    schema_allow_unknown=False   # strict mode recommended
))

await kernel.start()
# register modules with rich manifests...
```

See `example_usage.py` for a full demo.

## Ownership
Proprietary / all rights reserved. Not MIT. Only the owner (Timoune) may profit from this.

## Integration
Designed to sit between GhostMind (cognition) ↔ BloodyHeart ↔ BigArms (execution sandbox).

---

Generated from complete codebase dump — all tests pass, imports clean, v1.6.1 patch applied.
