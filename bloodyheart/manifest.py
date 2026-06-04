"""
bloodyheart.manifest
────────────────────
Module Manifest Schema for BloodyHeart v1.2 (Hardened)

Changes in v1.2:
- TrustLevel is now IntEnum with numeric privilege ordering (0=UNTRUSTED < ... < 4=SYSTEM)
  This fixes the critical string-comparison ordering bug that allowed privilege escalation
  via alphabetical sort (e.g. "TRUST_CORE" < "TRUST_MODULE" was True incorrectly).
- from_dict / to_dict remain backward-compatible with string trust_level in JSON manifests.
- Rich validation + documentation for all governance fields.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from typing import List, Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class TrustLevel(IntEnum):
    """
    Privilege levels for modules. Higher numeric value = more trust / capabilities.

    Comparisons now work correctly:
        TrustLevel.TRUST_CORE (3) > TrustLevel.TRUST_MODULE (2)  → True
        TrustLevel.TRUST_UNTRUSTED (0) < anything                 → True

    Never compare .value as strings again.
    """
    TRUST_UNTRUSTED = 0
    TRUST_PLUGIN    = 1
    TRUST_MODULE    = 2
    TRUST_CORE      = 3
    TRUST_SYSTEM    = 4


@dataclass
class ResourceLimits:
    """Resource budgets declared by a module. Enforced by ResourceGovernor + TaskBudgetWatchdog."""
    cpu_budget: float = 1.0          # CPU-hours per day (rough). See ResourceGovernor for enforcement.
    memory_limit_mb: int = 512
    token_budget: Optional[int] = None
    reasoning_budget: Optional[int] = None


@dataclass
class RecoveryPolicy:
    """How BloodyHeart should react to module failure / repeated violations."""
    restart_attempts: int = 3
    restart_backoff: str = "exponential"
    escalation_level: str = "L2"
    cooldown_seconds: int = 60


@dataclass
class HealthThresholds:
    """Latency / error thresholds that trigger HealthState changes."""
    degraded_latency_ms: int = 500
    unhealthy_latency_ms: int = 2000
    max_error_rate: float = 0.1


@dataclass
class ModuleManifest:
    """
    Declarative contract for every module loaded into BloodyHeart.

    The kernel wires this into:
    - DependencyDAG (startup/shutdown order + failure propagation)
    - TrustEnforcer (permission + privilege checks)
    - ResourceGovernor + TaskBudgetWatchdog
    - HealthMonitor
    - SafeModeManager / SecurityEscalationMatrix (recovery hints)
    """
    name: str
    version: str = "1.0"
    trust_level: TrustLevel = TrustLevel.TRUST_MODULE
    dependencies: List[str] = field(default_factory=list)
    resource_limits: ResourceLimits = field(default_factory=ResourceLimits)
    permissions: List[str] = field(default_factory=list)
    recovery_policy: RecoveryPolicy = field(default_factory=RecoveryPolicy)
    health_thresholds: HealthThresholds = field(default_factory=HealthThresholds)
    supported_events: List[str] = field(default_factory=list)
    description: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self._validate()

    def _validate(self):
        if not self.name or not isinstance(self.name, str):
            raise ValueError("Module name must be a non-empty string")
        if not self.version or not isinstance(self.version, str):
            raise ValueError("Module version must be a non-empty string")

        # Accept str (from JSON/manifests) or int or already IntEnum
        if isinstance(self.trust_level, str):
            try:
                self.trust_level = TrustLevel[self.trust_level]
            except KeyError:
                logger.warning("Unknown trust_level '%s' for %s — defaulting to TRUST_MODULE",
                               self.trust_level, self.name)
                self.trust_level = TrustLevel.TRUST_MODULE
        elif isinstance(self.trust_level, int):
            self.trust_level = TrustLevel(self.trust_level)
        elif not isinstance(self.trust_level, TrustLevel):
            raise ValueError(f"Invalid trust_level: {self.trust_level}")

        if not isinstance(self.dependencies, list):
            raise ValueError("dependencies must be a list of strings")
        if not isinstance(self.permissions, list):
            raise ValueError("permissions must be a list of strings")

    def to_dict(self) -> dict:
        """Serialize for manifests, snapshots, or API. trust_level uses .name for human readability."""
        return {
            "name": self.name,
            "version": self.version,
            "trust_level": self.trust_level.name,   # e.g. "TRUST_CORE" (compatible with v1.1 JSON)
            "dependencies": self.dependencies,
            "resource_limits": {
                "cpu_budget": self.resource_limits.cpu_budget,
                "memory_limit_mb": self.resource_limits.memory_limit_mb,
                "token_budget": self.resource_limits.token_budget,
                "reasoning_budget": self.resource_limits.reasoning_budget,
            },
            "permissions": self.permissions,
            "recovery_policy": {
                "restart_attempts": self.recovery_policy.restart_attempts,
                "restart_backoff": self.recovery_policy.restart_backoff,
                "escalation_level": self.recovery_policy.escalation_level,
                "cooldown_seconds": self.recovery_policy.cooldown_seconds,
            },
            "health_thresholds": {
                "degraded_latency_ms": self.health_thresholds.degraded_latency_ms,
                "unhealthy_latency_ms": self.health_thresholds.unhealthy_latency_ms,
                "max_error_rate": self.health_thresholds.max_error_rate,
            },
            "supported_events": self.supported_events,
            "description": self.description,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ModuleManifest":
        resource_limits = ResourceLimits(**data.get("resource_limits", {}))
        recovery_policy = RecoveryPolicy(**data.get("recovery_policy", {}))
        health_thresholds = HealthThresholds(**data.get("health_thresholds", {}))
        return cls(
            name=data["name"],
            version=data.get("version", "1.0"),
            trust_level=data.get("trust_level", TrustLevel.TRUST_MODULE),
            dependencies=data.get("dependencies", []),
            resource_limits=resource_limits,
            permissions=data.get("permissions", []),
            recovery_policy=recovery_policy,
            health_thresholds=health_thresholds,
            supported_events=data.get("supported_events", []),
            description=data.get("description"),
            metadata=data.get("metadata", {}),
        )