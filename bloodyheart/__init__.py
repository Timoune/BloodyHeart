"""
BloodyHeart v1.7 — Cognitive Microkernel for Mini Von (Hardened)

v1.7 changes:
- BlockingTaskMonitor for detection of non-cooperative / blocking modules
- Basic CompensationRegistry for external transaction tracking
- Stronger manifest validation and version reporting
- Improved preemption and safety documentation
"""

from .kernel import BloodyHeart, BloodyHeartConfig
from .manifest import ModuleManifest, TrustLevel, ResourceLimits, RecoveryPolicy
from .core.compensation import CompensationRegistry
from .compensator import RobustCompensator, CompensationMetrics, DeadLetterQueue  # v1.7+