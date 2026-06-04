"""
test_governance_v1.py
─────────────────────
High-quality test suite for BloodyHeart v1 Governance layer.
Covers: TrustEnforcer, ResourceGovernor, TaskBudgetWatchdog,
SecurityEscalationMatrix, SafeModeManager and integrations.

Clean version — only meaningful tests.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock

from bloodyheart.manifest import TrustLevel, ResourceLimits
from bloodyheart.governance.trust import TrustEnforcer
from bloodyheart.governance.resource import ResourceGovernor
from bloodyheart.governance.watchdog import TaskBudgetWatchdog
from bloodyheart.governance.security import SecurityLevel
from bloodyheart.governance.safe_mode import SafeMode


# ── TrustEnforcer ────────────────────────────────────────────────────────────

def test_trust_enforcer_register(kernel, sample_manifests):
    te = kernel.trust_enforcer
    te.register_module(sample_manifests["GhostMind"])
    assert "GhostMind" in te._modules
    assert te.get_trust_level("GhostMind") == TrustLevel.TRUST_CORE


def test_trust_enforcer_unregistered_defaults_to_untrusted(kernel):
    te = kernel.trust_enforcer
    assert te.get_trust_level("UnknownModule") == TrustLevel.TRUST_UNTRUSTED


@pytest.mark.parametrize("action", ["state.write", "state.delete", "module.register"])
async def test_trust_check_action_insufficient_trust(kernel, sample_manifests, action):
    te = kernel.trust_enforcer
    te.register_module(sample_manifests["UntrustedPlugin"])
    allowed = await te.check_action("UntrustedPlugin", action)
    assert allowed is False


async def test_trust_check_action_sufficient_trust(kernel, sample_manifests):
    te = kernel.trust_enforcer
    te.register_module(sample_manifests["GhostMind"])
    allowed = await te.check_action("GhostMind", "state.write")
    assert allowed is True


async def test_trust_permission_denied_write_state(kernel, sample_manifests):
    te = kernel.trust_enforcer
    m = sample_manifests["GhostMind"]
    m.permissions = ["read_state"]
    te.register_module(m)
    allowed = await te.check_action("GhostMind", "state.write")
    assert allowed is False


# ── ResourceGovernor ─────────────────────────────────────────────────────────

def test_resource_governor_register(kernel, sample_manifests):
    rg = kernel.resource_governor
    rg.register_module(sample_manifests["GhostMind"])
    limits = rg.get_limits("GhostMind")
    assert limits.cpu_budget == 2.0


def test_resource_governor_record_usage(kernel, sample_manifests):
    rg = kernel.resource_governor
    rg.register_module(sample_manifests["GhostMind"])
    rg.record_usage("GhostMind", cpu_ms=1500, tokens=1200)
    usage = rg.get_usage("GhostMind")
    assert usage.tokens_used == 1200


def test_resource_governor_reset_usage(kernel, sample_manifests):
    rg = kernel.resource_governor
    rg.register_module(sample_manifests["GhostMind"])
    rg.record_usage("GhostMind", tokens=1000)
    rg.reset_usage("GhostMind")
    assert rg.get_usage("GhostMind").tokens_used == 0


# ── TaskBudgetWatchdog ───────────────────────────────────────────────────────

def test_watchdog_start_and_end_task(kernel, sample_manifests):
    wd = kernel.watchdog
    wd.register_module(sample_manifests["GhostMind"])
    budget = wd.start_task("t1", "GhostMind", token_budget=1000)
    assert budget.task_id == "t1"
    ended = wd.end_task("t1")
    assert ended is not None


def test_watchdog_report_usage_under_budget(kernel, sample_manifests):
    wd = kernel.watchdog
    wd.register_module(sample_manifests["GhostMind"])
    wd.start_task("t2", "GhostMind", token_budget=1000)
    remaining = wd.report_usage("t2", tokens=300)
    assert remaining is not None and remaining > 0.5


# ── SecurityEscalationMatrix + SafeMode ──────────────────────────────────────

async def test_security_s1_local_violation(kernel):
    sm = kernel.security_matrix
    action = await sm.handle_violation("BadPlugin", "permission_denied", "reason")
    assert action.level == SecurityLevel.S1_LOCAL
    assert action.terminate_module is True


async def test_security_s2_repeated_violations(kernel):
    sm = kernel.security_matrix
    for _ in range(3):
        await sm.handle_violation("RepeatOffender", "permission_misuse", "x")
    action = await sm.handle_violation("RepeatOffender", "permission_misuse", "x")
    assert action.level == SecurityLevel.S2_TRUSTED_MODULE


async def test_security_s3_privilege_escalation(kernel):
    sm = kernel.security_matrix
    action = await sm.handle_violation("Evil", "privilege_escalation", "tried root")
    assert action.level == SecurityLevel.S3_TRUST_BOUNDARY


async def test_security_s4_critical_compromise(kernel):
    sm = kernel.security_matrix
    action = await sm.handle_violation("Rootkit", "sandbox_escape", "broke out")
    assert action.level == SecurityLevel.S4_CRITICAL
    assert action.safe_mode_target == SafeMode.L4_EMERGENCY


async def test_security_budget_exceeded_escalates_safe_mode(kernel):
    await kernel.start()
    try:
        await kernel.security_matrix.handle_budget_exceeded("GhostMind", "tokens", 1.05)
        assert kernel.current_safe_mode() == SafeMode.L2_DEGRADED
    finally:
        await kernel.stop()


async def test_security_critical_violation_forces_l4_from_l2(kernel):
    await kernel.start()
    try:
        await kernel.escalate_safe_mode(SafeMode.L2_DEGRADED, "previous")
        await kernel.security_matrix.handle_violation("bad", "sandbox_escape", "break")
        assert kernel.current_safe_mode() == SafeMode.L4_EMERGENCY
    finally:
        await kernel.stop()


# ── Integration ──────────────────────────────────────────────────────────────

def test_manifest_recovery_policy_is_stored(kernel, sample_manifests):
    kernel.register_module(sample_manifests["GhostMind"])
    manifest = kernel.manifests["GhostMind"]
    assert manifest.recovery_policy.escalation_level == "L2"