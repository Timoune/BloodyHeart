"""
bloodyheart.governance.trust
────────────────────────────
TrustEnforcer v1.2 — Enforces trust levels and permission checks.

v1.2 changes:
- validate_event now accepts optional authenticated_emitter.
  All privilege decisions are made against the *real* emitter's TrustLevel.
- Added explicit check to prevent low-trust modules from emitting events
  with BloodyHeart.* (system) sources — closes impersonation of kernel components.
- _emit_violation kept as internal but now used by CoreBus schema integration too.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

from ..core.event import Event, Priority
from ..manifest import ModuleManifest, TrustLevel

if TYPE_CHECKING:
    from ..core.bus import CoreBus
    from ..core.registry import SchemaRegistry

logger = logging.getLogger(__name__)


@dataclass
class ModuleTrustInfo:
    manifest: ModuleManifest
    permissions: Set[str] = field(default_factory=set)


class TrustEnforcer:
    """
    Runtime trust and permission enforcement for BloodyHeart.
    """

    def __init__(self, bus: Optional["CoreBus"] = None, registry: Optional["SchemaRegistry"] = None):
        self._bus = bus
        self._registry = registry
        self._modules: Dict[str, ModuleTrustInfo] = {}
        self._logger = logging.getLogger(f"{__name__}.TrustEnforcer")

        self._privileged_actions = {
            "state.write": TrustLevel.TRUST_CORE,
            "state.delete": TrustLevel.TRUST_CORE,
            "module.register": TrustLevel.TRUST_SYSTEM,
            "safe_mode.escalate": TrustLevel.TRUST_SYSTEM,
            "security.violation": TrustLevel.TRUST_CORE,
        }

    def register_module(self, manifest: ModuleManifest) -> None:
        info = ModuleTrustInfo(
            manifest=manifest,
            permissions=set(manifest.permissions),
        )
        self._modules[manifest.name] = info
        self._logger.debug("Registered trust info for %s (level=%s)", manifest.name, manifest.trust_level.name)

    def get_trust_level(self, module_name: str) -> TrustLevel:
        info = self._modules.get(module_name)
        return info.manifest.trust_level if info else TrustLevel.TRUST_UNTRUSTED

    def has_permission(self, module_name: str, permission: str) -> bool:
        info = self._modules.get(module_name)
        if not info:
            return False
        return (permission in info.permissions or
                info.manifest.trust_level in (TrustLevel.TRUST_SYSTEM, TrustLevel.TRUST_CORE))

    async def check_action(
        self,
        source: str,
        action: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Check if source module is allowed to perform the action."""
        trust = self.get_trust_level(source)
        required = self._privileged_actions.get(action, TrustLevel.TRUST_MODULE)

        if trust < required:  # Now correct numeric comparison thanks to IntEnum
            await self._emit_violation(
                source=source,
                violation_type="insufficient_trust",
                reason=f"Action '{action}' requires {required.name}, module has {trust.name}",
                details=details or {},
            )
            return False

        if action in ("state.write", "state.delete") and not self.has_permission(source, "write_state"):
            await self._emit_violation(
                source=source,
                violation_type="permission_denied",
                reason=f"Module '{source}' lacks 'write_state' permission",
                details=details or {},
            )
            return False

        return True

    async def validate_event(
        self,
        event: Event,
        authenticated_emitter: Optional[str] = None,
    ) -> bool:
        """
        Event-level trust validation (v1.2 hardened).

        Uses authenticated_emitter (real caller) when provided; falls back to
        event.source for backward compatibility.
        """
        effective_source = authenticated_emitter or event.source
        trust = self.get_trust_level(effective_source)

        # ── NEW: Block low-trust modules from impersonating BloodyHeart.* system components ──
        if event.source.startswith("BloodyHeart.") and authenticated_emitter is not None:
            emitter_trust = self.get_trust_level(authenticated_emitter)
            if emitter_trust < TrustLevel.TRUST_SYSTEM:
                await self._emit_violation(
                    source=authenticated_emitter,
                    violation_type="system_impersonation",
                    reason=f"Module '{authenticated_emitter}' (trust={emitter_trust.name}) "
                           f"attempted to emit system-reserved event source '{event.source}'",
                    details={"claimed_source": event.source, "event_type": event.event_type},
                )
                return False

        # Untrusted modules restricted to low-priority non-privileged events
        if trust == TrustLevel.TRUST_UNTRUSTED:
            if event.priority in (Priority.P0_SECURITY, Priority.P1_HUMAN):
                await self._emit_violation(
                    source=effective_source,
                    violation_type="untrusted_high_priority",
                    reason="Untrusted module attempted high-priority event",
                    details={"event_type": event.event_type, "claimed_source": event.source},
                )
                return False

        return True

    async def _emit_violation(
        self,
        source: str,
        violation_type: str,
        reason: str,
        details: Dict[str, Any],
    ) -> None:
        self._logger.warning("[TRUST VIOLATION] %s from %s: %s", violation_type, source, reason)
        if self._bus:
            await self._bus.publish(
                Event(
                    source="BloodyHeart.TrustEnforcer",
                    destination=source,
                    event_type="security.violation",
                    version="v1",
                    payload={
                        "module": source,
                        "violation_type": violation_type,
                        "reason": reason,
                        "details": details,
                        "trust_level": self.get_trust_level(source).name,
                    },
                    priority=Priority.P0_SECURITY,
                ),
                authenticated_emitter="BloodyHeart.TrustEnforcer",  # internal, trusted
            )