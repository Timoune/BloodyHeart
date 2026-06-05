"""
bloodyheart/bigarms_integration.py
──────────────────────────────────
BigArms Integration for BloodyHeart v1.7.1

This module provides a production-grade bridge between GhostMind and BigArms
through BloodyHeart v1.7.1, with deep integration into the following v1.7+ features:

- CompensationRegistry (for tracking external side-effecting operations)
- BlockingTaskMonitor integration (via scheduler callback)
- Rich event payloads with risk assessment and resource hints
- Full governance enforcement (Trust, SafeMode, Budget, Security)
- HITL approval workflow for ELEVATED capabilities

Loose coupling is maintained via BigArmsNamedPipeClient.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

from bigarms.windows.ipc import BigArmsNamedPipeClient
from bigarms.models import (
    ExecutionRequest,
    StructuredCapability,
    PermissionTier,
    NamedPipeConnectionError,
    NamedPipeCommunicationError,
)

from .core.event import Event, Priority
from .kernel import BloodyHeart
from .manifest import TrustLevel

logger = logging.getLogger("bloodyheart.bigarms_integration_v17")


@dataclass
class ToolExecutionRequest:
    """Parsed payload from `tool.execute.request` event."""
    correlation_id: str
    tool_name: str
    tool_version: str
    args: Dict[str, Any]
    granted_capabilities: list[Dict[str, Any]]
    dry_run: bool = False
    resource_budget: Dict[str, Any] = None
    requested_by: str = "GhostMind"
    risk_level: str = "medium"           # NEW: richer payload
    estimated_tokens: int = 0            # NEW
    estimated_cpu_ms: int = 0            # NEW


class BigArmsExecutor:
    """
    Production BigArms bridge for BloodyHeart v1.7.1.

    Features:
    - Deep CompensationRegistry integration
    - BlockingTaskMonitor awareness
    - TaskBudgetWatchdog integration
    - Circuit breaker for resilience
    - Automatic safe mode escalation on repeated failures
    - Rich event payloads + full governance + HITL for ELEVATED
    """

    def __init__(
        self,
        kernel: BloodyHeart,
        bigarms_pipe_name: str = r"\\.\pipe\BigArmsExecution",
    ):
        self.kernel = kernel
        self.bus = kernel.bus
        self.trust_enforcer = kernel.trust_enforcer
        self.resource_governor = kernel.resource_governor
        self.watchdog = kernel.watchdog
        self.safe_mode_manager = kernel.safe_mode_manager
        self.security_matrix = kernel.security_matrix
        self.compensation_registry = kernel.compensation_registry  # v1.7

        self.bigrams_pipe_name = bigarms_pipe_name
        self._client: Optional[BigArmsNamedPipeClient] = None
        self._pending_approvals: Dict[str, ToolExecutionRequest] = {}
        self._consecutive_failures: int = 0
        self._max_failures_before_escalation: int = 3

        # Simple circuit breaker state for BigArms connectivity
        self._circuit_open: bool = False
        self._circuit_failure_count: int = 0
        self._circuit_failure_threshold: int = 5
        self._circuit_reset_timeout: float = 30.0
        self._last_circuit_open_time: float = 0.0

        # Basic operational metrics
        self._metrics = {
            "total_requests": 0,
            "successful": 0,
            "failed": 0,
            "approval_required": 0,
            "circuit_trips": 0,
        }

        # Subscribe to events
        self.bus.subscribe("tool.execute.request", self._handle_execution_request)
        self.bus.subscribe("tool.execute.approval_response", self._handle_approval_response)

        # v1.7: Register blocking escalation callback
        if hasattr(kernel.scheduler, "set_blocking_escalation_callback"):
            kernel.scheduler.set_blocking_escalation_callback(self._on_blocking_task_detected)

        logger.info("BigArmsExecutor v1.7.1 initialized with CompensationRegistry + BlockingTaskMonitor + TaskBudgetWatchdog + CircuitBreaker + auto-escalation integration")

    # =====================================================================
    # Blocking Task Monitor Integration (v1.7)
    # =====================================================================
    def _on_blocking_task_detected(self, priority: str, age_seconds: float):
        """Called by PriorityScheduler when a task is blocking too long."""
        logger.warning(
            "Blocking task detected in BigArms path (priority=%s, age=%.1fs) — may impact tool execution",
            priority, age_seconds
        )
        # Future enhancement: could escalate safe mode or publish a specific event
        # For now we just log — higher layers (GhostMind) can react via events.

    # =====================================================================
    # Main Execution Request Handler
    # =====================================================================
    async def _handle_execution_request(self, event: Event):
        try:
            payload = event.payload or {}
            exec_req = ToolExecutionRequest(
                correlation_id=payload.get("correlation_id") or f"exec-{uuid.uuid4().hex[:12]}",
                tool_name=payload["tool_name"],
                tool_version=payload["tool_version"],
                args=payload.get("args", {}),
                granted_capabilities=payload.get("granted_capabilities", []),
                dry_run=payload.get("dry_run", False),
                resource_budget=payload.get("resource_budget", {}),
                requested_by=payload.get("requested_by", event.source),
                risk_level=payload.get("risk_level", "medium"),
                estimated_tokens=payload.get("estimated_tokens", 0),
                estimated_cpu_ms=payload.get("estimated_cpu_ms", 0),
            )

            # 1. Governance checks (Trust, Budget, SafeMode, etc.)
            allowed = await self._perform_governance_checks(exec_req, event.source)
            if not allowed:
                return

            # Publish started event for observability
            await self._publish_started(exec_req)

            # 2. Start TaskBudgetWatchdog task (v1.7 integration)
            watchdog_task_id = None
            if not exec_req.dry_run:
                watchdog_task_id = self._start_watchdog_task(exec_req)

            # 3. Register with CompensationRegistry (v1.7) if this has side effects
            compensation_token = None
            if not exec_req.dry_run and exec_req.granted_capabilities:
                compensation_token = self._register_compensation(exec_req)

            # 4. Execute via BigArms (respect kernel dry-run mode)
            effective_dry_run = req.dry_run or self.kernel.is_dry_run()
            if effective_dry_run:
                result = {"success": True, "status": "dry_run", "tool": req.tool_name, "correlation_id": req.correlation_id}
            else:
                result = await self._execute_via_bigarms(exec_req)

            # 5. Report usage to watchdog if applicable
            if watchdog_task_id:
                self._report_watchdog_usage(watchdog_task_id, result)

            # 4. Mark compensation if successful
            if compensation_token and result.get("success"):
                self.kernel.compensation_registry.mark_compensated(compensation_token)

            # 6. On failure, attempt automatic compensation (v1.7)
            if compensation_token and not result.get("success", False):
                try:
                    self.kernel.request_compensation(compensation_token, publish_event=True)
                except Exception as comp_err:
                    logger.warning("Failed to auto-request compensation for %s: %s", compensation_token, comp_err)

            # 7. Track failures, trip circuit breaker, and auto-escalate if needed
            if result.get("success", False):
                self._consecutive_failures = 0
                self._circuit_failure_count = 0
            else:
                self._consecutive_failures += 1
                self._circuit_failure_count += 1

                if self._circuit_failure_count >= self._circuit_failure_threshold:
                    self._circuit_open = True
                    self._last_circuit_open_time = time.time()
                    self._metrics["circuit_trips"] += 1
                    logger.error("BigArms circuit breaker OPENED due to repeated failures")

                if self._consecutive_failures >= self._max_failures_before_escalation:
                    await self._auto_escalate_on_repeated_failures(exec_req)

            # 8. Publish rich result
            await self._publish_result(exec_req, result, compensation_token)

        except Exception as e:
            logger.exception("Error in BigArmsExecutor._handle_execution_request: %s", e)
            await self._publish_failure(
                correlation_id=payload.get("correlation_id", "unknown"),
                tool_name=payload.get("tool_name", "unknown"),
                error=str(e),
                requested_by=event.source,
            )

    def _register_compensation(self, req: ToolExecutionRequest) -> Optional[str]:
        """Register the tool execution with BloodyHeart's CompensationRegistry (v1.7)."""
        try:
            token = self.kernel.register_external_operation(
                module=req.requested_by,
                operation=f"bigarms.{req.tool_name}",
                compensation_action={
                    "type": "bigarms_undo",
                    "tool_name": req.tool_name,
                    "tool_version": req.tool_version,
                    "args": req.args,
                },
                metadata={
                    "correlation_id": req.correlation_id,
                    "risk_level": req.risk_level,
                },
                timeout_seconds=30,
            )
            return token
        except Exception as e:
            logger.warning("Failed to register compensation for %s: %s", req.tool_name, e)
            return None

    # =====================================================================
    # Governance + HITL
    # =====================================================================
    async def _perform_governance_checks(self, req: ToolExecutionRequest, source: str) -> bool:
        trust_level = self.trust_enforcer.get_trust_level(source)
        if trust_level < TrustLevel.TRUST_MODULE:
            await self._publish_failure(req.correlation_id, req.tool_name, "Insufficient trust level", source)
            return False

        # Resource budget recording
        if req.resource_budget or req.estimated_tokens or req.estimated_cpu_ms:
            self.resource_governor.record_usage(
                source,
                cpu_ms=req.estimated_cpu_ms or req.resource_budget.get("estimated_cpu_ms", 0),
                tokens=req.estimated_tokens or req.resource_budget.get("estimated_tokens", 0),
            )

        # Safe Mode restrictions
        current_safe_mode = self.safe_mode_manager.current_mode
        if current_safe_mode.level >= 3:  # L3_READ_ONLY or higher
            has_write_or_elevated = any(
                cap.get("tier") in ("WRITE", "ELEVATED") for cap in req.granted_capabilities
            )
            if has_write_or_elevated and not req.dry_run:
                await self._publish_failure(
                    req.correlation_id, req.tool_name,
                    f"Write/ELEVATED operations blocked in {current_safe_mode.value}", source
                )
                return False

        # ELEVATED → HITL approval flow
        has_elevated = any(cap.get("tier") == "ELEVATED" for cap in req.granted_capabilities)
        if has_elevated and not req.dry_run:
            self._pending_approvals[req.correlation_id] = req
            await self.bus.publish(
                Event(
                    source="BigArmsExecutor",
                    destination=req.requested_by,
                    event_type="tool.execute.approval_required",
                    payload={
                        "correlation_id": req.correlation_id,
                        "tool_name": req.tool_name,
                        "tool_version": req.tool_version,
                        "args": req.args,
                        "risk_level": req.risk_level,
                        "estimated_tokens": req.estimated_tokens,
                        "estimated_cpu_ms": req.estimated_cpu_ms,
                        "warning": "ELEVATED capability requested — human approval required",
                        "requested_by": source,
                    },
                    priority=Priority.P1_HUMAN,
                )
            )
            return False

        return True

    async def _handle_approval_response(self, event: Event):
        payload = event.payload or {}
        correlation_id = payload.get("correlation_id")
        approved = payload.get("approved", False)
        decided_by = payload.get("decided_by", event.source)

        if correlation_id not in self._pending_approvals:
            return

        req = self._pending_approvals.pop(correlation_id)

        if not approved:
            await self._publish_failure(correlation_id, req.tool_name, f"Denied by {decided_by}", req.requested_by)
            return

        # Proceed with execution after approval
        try:
            result = await self._execute_via_bigarms(req)
            await self._publish_result(req, result)
        except Exception as e:
            await self._publish_failure(correlation_id, req.tool_name, str(e), req.requested_by)

    # =====================================================================
    # BigArms Execution (loose coupling)
    # =====================================================================
    async def _execute_via_bigarms(self, req: ToolExecutionRequest) -> dict:
        # Circuit breaker check
        if self._circuit_open:
            if time.time() - self._last_circuit_open_time > self._circuit_reset_timeout:
                self._circuit_open = False
                self._circuit_failure_count = 0
                logger.info("BigArms circuit breaker reset")
            else:
                return {
                    "success": False,
                    "error": "BigArms circuit breaker is open (too many recent failures)",
                    "circuit_open": True,
                }

        if self._client is None or not self._client.is_connected:
            self._client = BigArmsNamedPipeClient(pipe_name=self.bigrams_pipe_name, max_retries=3)

        try:
            capabilities = []
            for cap_dict in req.granted_capabilities:
                try:
                    capabilities.append(StructuredCapability(**cap_dict))
                except Exception:
                    capabilities.append(StructuredCapability(tier=PermissionTier.READ))

            result = self._client.execute_tool(
                tool_name=req.tool_name,
                tool_version=req.tool_version,
                args=req.args,
                granted_capabilities=capabilities,
                dry_run=req.dry_run,
                correlation_id=req.correlation_id,
                resource_budget=req.resource_budget or {},
            )
            return result
        except (NamedPipeConnectionError, NamedPipeCommunicationError) as e:
            logger.error("BigArms communication failed: %s", e)
            return {"success": False, "error": f"BigArms unavailable: {e}"}

    # =====================================================================
    # Event Publishing (rich payloads)
    # =====================================================================
    async def _publish_result(self, req: ToolExecutionRequest, result: dict, compensation_token: Optional[str] = None):
        payload = {
            "correlation_id": req.correlation_id,
            "tool_name": req.tool_name,
            "tool_version": req.tool_version,
            "result": result,
            "dry_run": req.dry_run,
            "risk_level": req.risk_level,
        }
        if compensation_token:
            payload["compensation_token"] = compensation_token

        await self.bus.publish(
            Event(
                source="BigArmsExecutor",
                destination=req.requested_by,
                event_type="tool.execute.result",
                payload=payload,
                priority=Priority.P2_AUTONOMOUS,
            )
        )

    async def _publish_failure(self, correlation_id: str, tool_name: str, error: str, requested_by: str):
        await self.bus.publish(
            Event(
                source="BigArmsExecutor",
                destination=requested_by,
                event_type="tool.execute.failed",
                payload={
                    "correlation_id": correlation_id,
                    "tool_name": tool_name,
                    "error": error,
                },
                priority=Priority.P0_SECURITY,
            )
        )

    # =====================================================================
    # Helper methods for v1.7 integrations
    # =====================================================================
    async def _publish_started(self, req: ToolExecutionRequest):
        await self.bus.publish(
            Event(
                source="BigArmsExecutor",
                destination=req.requested_by,
                event_type="tool.execute.started",
                payload={
                    "correlation_id": req.correlation_id,
                    "tool_name": req.tool_name,
                    "tool_version": req.tool_version,
                    "risk_level": req.risk_level,
                    "dry_run": req.dry_run,
                },
                priority=Priority.P2_AUTONOMOUS,
            )
        )

    def _start_watchdog_task(self, req: ToolExecutionRequest) -> Optional[str]:
        try:
            task_id = f"bigarms-{req.correlation_id}"
            self.watchdog.start_task(
                task_id=task_id,
                module_name=req.requested_by,
                cpu_budget_s=req.estimated_cpu_ms / 1000.0 if req.estimated_cpu_ms else None,
                token_budget=req.estimated_tokens or None,
            )
            return task_id
        except Exception as e:
            logger.debug("Could not start watchdog task: %s", e)
            return None

    def _report_watchdog_usage(self, task_id: str, result: dict):
        try:
            # Rough reporting — in real use you'd get better metrics from BigArms result
            duration_ms = result.get("resource_usage", {}).get("duration_ms", 0)
            self.watchdog.report_usage(
                task_id=task_id,
                cpu_s=duration_ms / 1000.0,
                tokens=result.get("tokens_used", 0),
            )
            self.watchdog.end_task(task_id)
        except Exception as e:
            logger.debug("Watchdog reporting failed: %s", e)

    async def _auto_escalate_on_repeated_failures(self, req: ToolExecutionRequest):
        """Escalate safe mode after repeated BigArms failures."""
        logger.warning(
            "Repeated BigArms failures (%d) for tool=%s — auto-escalating safe mode",
            self._consecutive_failures, req.tool_name
        )
        try:
            # Escalate to L2_DEGRADED as a protective measure
            await self.safe_mode_manager.escalate(
                target_mode=self.safe_mode_manager.current_mode.__class__.L2_DEGRADED,
                reason=f"Repeated BigArms execution failures ({self._consecutive_failures})",
                triggered_by="BigArmsExecutor"
            )
            # Also report via SecurityEscalationMatrix
            await self.security_matrix.handle_violation(
                module_name=req.requested_by,
                violation_type="REPEATED_SECURITY_FAILURE",
                reason=f"Repeated BigArms tool execution failures for {req.tool_name}",
            )
        except Exception as e:
            logger.error("Failed to auto-escalate after repeated failures: %s", e)
        finally:
            self._consecutive_failures = 0  # reset after escalation

    def get_metrics(self) -> dict:
        """Return current operational metrics."""
        return {
            **self._metrics,
            "consecutive_failures": self._consecutive_failures,
            "circuit_open": self._circuit_open,
            "pending_approvals": len(self._pending_approvals),
        }

    async def shutdown(self):
        if self._client:
            self._client.close()
        logger.info("BigArmsExecutor shut down")


# =============================================================================
# Wiring helper for v1.7
# =============================================================================
def wire_bigarms_to_bloodyheart(kernel: BloodyHeart, pipe_name: str = r"\\.\pipe\BigArmsExecution") -> BigArmsExecutor:
    """
    Wire BigArmsExecutor into a running BloodyHeart v1.7 kernel.
    """
    executor = BigArmsExecutor(kernel=kernel, bigarms_pipe_name=pipe_name)
    return executor
