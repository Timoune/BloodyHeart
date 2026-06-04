"""
BloodyHeart v1 — Cognitive Microkernel for Mini Von (COMPLETE)

This package provides the full microkernel with:
- Priority-scheduled CoreBus
- MVCC state + transactions + snapshots
- Hierarchical safe modes (L1–L4)
- S1–S4 security escalation
- Trust, resource, and budget governance
- Health monitoring + dependency DAG
- Rich module manifests
"""

from .kernel import BloodyHeart, BloodyHeartConfig
from .manifest import ModuleManifest, TrustLevel, ResourceLimits