"""
bloodyheart.governance.security
───────────────────────────────
SecurityEscalationMatrix v1.6.1 — Structured violation classification (patch).

v1.6.1 alignment fix:
- `handle_budget_exceeded` now routes through the main `handle_violation` path
  using `ViolationType.BUDGET_EXHAUSTION` for full consistency with the enum system.
  Behavior is unchanged, but classification and logging are now unified.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Dict, Optional

from ..core.event import Event, Priority
from .safe_mode import SafeMode, SafeModeManager

if TYPE_CHECKING:
    from ..core.bus import CoreBus

logger = logging.getLogger(__name__)


class ViolationType(str, Enum):
    """Structured violation categories."""
    SANDBOX_ESCAPE = "SANDBOX_ESCAPE"
    KERNEL_TAMPER = "KERNEL_TAMPER"
    STATESTORE_TAMPER = "STATESTORE_TAMPER"
    JOURNAL_MANIPULATION = "JOURNAL_MANIPULATION"
    TRUST_BOUNDARY_BREACH = "TRUST_BOUNDARY_BREACH"
    UNAUTHORIZED_IPC = "UNAUTHORIZED_IPC"
    PRIVILEGE_ESCALATION = "PRIVILEGE_ESCALATION"
    PERMISSION_MISUSE = "PERMISSION_MISUSE"
    REPEATED_SECURITY_FAILURE = "REPEATED_SECURITY_FAILURE"
    BUDGET_EXHAUSTION = "BUDGET_EXHAUSTION"
    SCHEMA_VIOLATION = "SCHEMA_VIOLATION"
    BLOB_QUOTA_EXCEEDED = "BLOB_QUOTA_EXCEEDED"
    SYSTEM_IMPERSONATION = "SYSTEM_IMPERSONATION"
    UNKNOWN = "UNKNOWN"


class SecurityLevel(Enum):
    S1_LOCAL = "S1_LOCAL"
    S2_TRUSTED_MODULE = "S2_TRUSTED_MODULE"
    S3_TRUST_BOUNDARY = "S3_TRUST_BOUNDARY"
    S4_CRITICAL = "S4_CRITICAL"


@dataclass
class SecurityAction:
    level: SecurityLevel
    safe_mode_target: Optional["SafeMode"] = None
    terminate_module: bool = False
    revoke_permissions: bool = False
    isolate_module: bool = False
    create_audit_snapshot: bool = False
    reason: str = ""


class SecurityEscalationMatrix:
    def __init__(self, bus: Optional["CoreBus"] = None, safe_mode_manager: Optional["SafeModeManager"] = None) -> None:
        self._bus = bus
        self._safe_mode = safe_mode_manager
        self._violation_counts: Dict[str, int] = {}
        self.terminated_modules: set[str] = set()
        self._logger = logging.getLogger(f"{__name__}.SecurityEscalationMatrix")

    async def handle_violation(
        self,
        module_name: str,
        violation_type: str | ViolationType,
        reason: str,
        trust_level: str = "UNKNOWN",
    ) -> SecurityAction:
        if isinstance(violation_type, str):
            try:
                vtype = ViolationType(violation_type.upper())
            except ValueError:
                vtype = ViolationType.UNKNOWN
        else:
            vtype = violation_type

        action = self._classify_violation(module_name, vtype, reason, trust_level)

        if action.terminate_module:
            await self._terminate_module(module_name, reason)
        if action.isolate_module and self._safe_mode:
            await self._safe_mode.escalate(
                target_mode=action.safe_mode_target or SafeMode.L2_DEGRADED,
                reason=f"S{action.level.value} violation: {reason}",
                triggered_by="SecurityEscalationMatrix",
            )
        if action.create_audit_snapshot:
            self._logger.warning("AUDIT SNAPSHOT requested for module %s", module_name)
        if action.safe_mode_target and self._safe_mode:
            await self._safe_mode.escalate(
                target_mode=action.safe_mode_target,
                reason=f"S{action.level.value}: {reason}",
                triggered_by="SecurityEscalationMatrix",
            )

        self._logger.warning("[SECURITY ESCALATION] %s | module=%s | type=%s", action.level.value, module_name, vtype.value)
        return action

    def _classify_violation(
        self, module_name: str, vtype: ViolationType, reason: str, trust_level: str
    ) -> SecurityAction:
        if vtype in (ViolationType.SANDBOX_ESCAPE, ViolationType.KERNEL_TAMPER,
                     ViolationType.STATESTORE_TAMPER, ViolationType.JOURNAL_MANIPULATION):
            return SecurityAction(
                level=SecurityLevel.S4_CRITICAL,
                safe_mode_target=SafeMode.L4_EMERGENCY,
                create_audit_snapshot=True,
                reason=f"Critical system compromise: {vtype.value}",
            )

        if vtype in (ViolationType.TRUST_BOUNDARY_BREACH, ViolationType.UNAUTHORIZED_IPC,
                     ViolationType.PRIVILEGE_ESCALATION, ViolationType.SYSTEM_IMPERSONATION):
            return SecurityAction(
                level=SecurityLevel.S3_TRUST_BOUNDARY,
                safe_mode_target=SafeMode.L3_READ_ONLY,
                isolate_module=True,
                create_audit_snapshot=True,
                reason=f"Trust boundary breach: {vtype.value}",
            )

        count = self._violation_counts.get(module_name, 0) + 1
        self._violation_counts[module_name] = count

        if count >= 3 or vtype in (ViolationType.PERMISSION_MISUSE, ViolationType.REPEATED_SECURITY_FAILURE):
            return SecurityAction(
                level=SecurityLevel.S2_TRUSTED_MODULE,
                safe_mode_target=SafeMode.L2_DEGRADED,
                isolate_module=True,
                reason=f"Repeated violation (count={count}) type={vtype.value}",
            )

        if vtype == ViolationType.BUDGET_EXHAUSTION:
            return SecurityAction(
                level=SecurityLevel.S2_TRUSTED_MODULE,
                safe_mode_target=SafeMode.L2_DEGRADED,
                reason=f"Budget exhaustion in {module_name}",
            )

        return SecurityAction(
            level=SecurityLevel.S1_LOCAL,
            terminate_module=True,
            revoke_permissions=True,
            reason=f"Local violation: {vtype.value}",
        )

    async def handle_budget_exceeded(self, module_name: str, budget_type: str, usage_pct: float) -> Optional[SecurityAction]:
        """
        v1.6.1: Now routes through handle_violation using ViolationType.BUDGET_EXHAUSTION
        for full consistency with the structured classification system.
        """
        if usage_pct >= 1.0:
            # Route through the main path for consistency
            action = await self.handle_violation(
                module_name=module_name,
                violation_type=ViolationType.BUDGET_EXHAUSTION,
                reason=f"Critical {budget_type} budget exhaustion (usage={usage_pct:.2f})",
            )
            return action
        return None

    async def _terminate_module(self, module_name: str, reason: str) -> None:
        self._logger.error("TERMINATE MODULE requested: %s — %s", module_name, reason)
        self.terminated_modules.add(module_name)
        if self._bus:
            await self._bus.publish(Event(
                source="BloodyHeart.SecurityEscalationMatrix",
                destination=module_name,
                event_type="module.terminate",
                version="v1",
                payload={"module_name": module_name, "reason": reason, "enforced": True},
                priority=Priority.P0_SECURITY,
            ))