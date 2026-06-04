"""
bloodyheart.health.states
─────────────────────────
Health state definitions.
"""

from enum import Enum


class HealthState(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNHEALTHY = "UNHEALTHY"
    RECOVERING = "RECOVERING"
    FAILED = "FAILED"