"""
Pytest configuration and shared fixtures for BloodyHeart v1 test suites.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

# Enable asyncio support
pytest_plugins = ["pytest_asyncio"]


from bloodyheart.kernel import BloodyHeart, BloodyHeartConfig
from bloodyheart.manifest import ModuleManifest, TrustLevel, ResourceLimits, RecoveryPolicy
from bloodyheart.core.event import Event, Priority
from bloodyheart.governance.safe_mode import SafeMode
from bloodyheart.governance.security import SecurityLevel


@pytest.fixture(scope="function")
def kernel():
    """Fresh kernel instance for each test."""
    config = BloodyHeartConfig(name="TestKernel", journal_path="/tmp/test_journal.jsonl")
    k = BloodyHeart(config=config)
    return k


@pytest.fixture
async def running_kernel(kernel):
    """Kernel that is started (and will be stopped after test)."""
    await kernel.start()
    yield kernel
    await kernel.stop()


@pytest.fixture
def mock_bus():
    bus = MagicMock()
    bus.publish = AsyncMock()
    return bus


@pytest.fixture
def sample_manifests():
    return {
        "GhostMind": ModuleManifest(
            name="GhostMind",
            version="4.4",
            trust_level=TrustLevel.TRUST_CORE,
            dependencies=["DreamCloud"],
            resource_limits=ResourceLimits(cpu_budget=2.0, memory_limit_mb=2048, token_budget=50000, reasoning_budget=100),
            permissions=["read_state", "write_state", "emit_events"],
            recovery_policy=RecoveryPolicy(restart_attempts=3, escalation_level="L2"),
        ),
        "DreamCloud": ModuleManifest(
            name="DreamCloud",
            version="16",
            trust_level=TrustLevel.TRUST_CORE,
            dependencies=[],
            resource_limits=ResourceLimits(memory_limit_mb=4096),
            permissions=["read_state", "write_state"],
        ),
        "UntrustedPlugin": ModuleManifest(
            name="UntrustedPlugin",
            version="1.0",
            trust_level=TrustLevel.TRUST_UNTRUSTED,
            dependencies=[],
            permissions=[],
        ),
    }


@pytest.fixture
def security_matrix(kernel):
    return kernel.security_matrix


@pytest.fixture
def safe_mode_manager(kernel):
    return kernel.safe_mode_manager


# Helper to create events
@pytest.fixture
def make_event():
    def _factory(source="Test", dest="*", etype="test.event", prio=Priority.P2_AUTONOMOUS, payload=None):
        return Event(source=source, destination=dest, event_type=etype, priority=prio, payload=payload or {})
    return _factory