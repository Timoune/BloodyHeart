"""
bloodyheart.core.blob
─────────────────────
BlobManager v1.4 — Large payload offloading with bounded LRU hot cache + per-module isolation.

v1.4 Security Hardening:
- Added optional `emitter` (authenticated_emitter) parameter to `store()`.
- Per-emitter byte quota tracking to prevent low-trust or compromised modules from
  spamming large blobs (embeddings, vision frames, etc.) to exhaust the shared cache.
- Quotas are enforced per trust tier when a TrustEnforcer is provided, or via explicit
  `max_bytes_per_emitter` map.
- Low-trust modules get very small (or zero) blob allowances by default.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Set

from ..manifest import TrustLevel

logger = logging.getLogger(__name__)


@dataclass
class BlobRef:
    blob_id: str
    size_bytes: int
    metadata: Dict[str, Any]


class BlobManager:
    """
    v1.4: Blob storage is now isolated per authenticated emitter / module tier.
    This stops a single low-trust module from DoS-ing the entire hot cache with
    massive payloads.
    """

    def __init__(
        self,
        threshold_bytes: int = 64 * 1024,
        blob_dir: Optional[str] = None,
        max_cache_size_bytes: Optional[int] = None,
        max_bytes_per_emitter: Optional[Dict[str, int]] = None,
        trust_enforcer: Optional[Any] = None,   # optional, for tier-based defaults
    ):
        self.threshold_bytes = threshold_bytes
        self.blob_dir = Path(blob_dir) if blob_dir else None
        self.max_cache_size_bytes = max_cache_size_bytes
        self._blobs: OrderedDict[str, bytes] = OrderedDict()
        self._metadata: Dict[str, Dict[str, Any]] = {}
        self._current_cache_size: int = 0
        self._per_emitter_usage: Dict[str, int] = defaultdict(int)
        self._max_bytes_per_emitter = max_bytes_per_emitter or {}
        self._trust_enforcer = trust_enforcer
        self._logger = logging.getLogger(f"{__name__}.BlobManager")

        if self.blob_dir:
            self.blob_dir.mkdir(parents=True, exist_ok=True)
            self._load_persisted_blobs()
            self._logger.info("BlobManager using persistent storage at %s", self.blob_dir)
        if self.max_cache_size_bytes:
            self._logger.info("BlobManager hot cache limited to %d bytes (LRU)", self.max_cache_size_bytes)

    def _get_emitter_quota(self, emitter: Optional[str]) -> int:
        if not emitter:
            return self.max_cache_size_bytes or (2 ** 30)  # very large default
        if emitter in self._max_bytes_per_emitter:
            return self._max_bytes_per_emitter[emitter]
        # Tier-based defaults if TrustEnforcer is available
        if self._trust_enforcer:
            trust = self._trust_enforcer.get_trust_level(emitter)
            if trust >= TrustLevel.TRUST_CORE:
                return self.max_cache_size_bytes or (512 * 1024 * 1024)
            if trust >= TrustLevel.TRUST_MODULE:
                return 64 * 1024 * 1024
            return 8 * 1024 * 1024   # low trust gets small allowance
        return 32 * 1024 * 1024      # safe default

    def should_use_blob(self, data: Any) -> bool:
        """Heuristic: use blob for bytes, large strings, or dicts with embeddings (from v1.1/v1.3)."""
        if isinstance(data, (bytes, bytearray)):
            return len(data) > self.threshold_bytes
        if isinstance(data, str):
            return len(data.encode("utf-8")) > self.threshold_bytes
        if isinstance(data, dict):
            try:
                size = len(json.dumps(data, default=str).encode("utf-8"))
                return size > self.threshold_bytes
            except Exception:
                return False
        return False

    def store(
        self,
        data: Any,
        metadata: Optional[Dict[str, Any]] = None,
        emitter: Optional[str] = None,          # NEW in v1.4
    ) -> BlobRef:
        blob_id = f"blob_{uuid.uuid4().hex[:16]}"
        if isinstance(data, (bytes, bytearray)):
            raw = bytes(data)
        elif isinstance(data, str):
            raw = data.encode("utf-8")
        else:
            raw = json.dumps(data, default=str).encode("utf-8")

        blob_size = len(raw)

        # v1.4: Per-emitter quota check
        if emitter:
            quota = self._get_emitter_quota(emitter)
            current_usage = self._per_emitter_usage[emitter]
            if current_usage + blob_size > quota:
                self._logger.warning(
                    "Blob quota exceeded for emitter=%s (used=%d, adding=%d, quota=%d). Rejecting store.",
                    emitter, current_usage, blob_size, quota
                )
                raise PermissionError(f"Blob quota exceeded for module '{emitter}'")

        # Single massive blob warning (from v1.3)
        if self.max_cache_size_bytes and blob_size > self.max_cache_size_bytes:
            self._logger.warning(
                "Single blob %s (%d bytes) exceeds entire cache limit. Storing anyway with warning.",
                blob_id, blob_size
            )

        self._blobs[blob_id] = raw
        self._blobs.move_to_end(blob_id)
        meta = metadata or {}
        if emitter:
            meta["emitter"] = emitter
        self._metadata[blob_id] = meta
        self._current_cache_size += blob_size
        if emitter:
            self._per_emitter_usage[emitter] += blob_size

        self._logger.debug("Stored blob %s (%d bytes) emitter=%s", blob_id, blob_size, emitter)

        # LRU eviction (v1.3 logic)
        if self.max_cache_size_bytes:
            while self._current_cache_size > self.max_cache_size_bytes and self._blobs:
                oldest_id, oldest_raw = self._blobs.popitem(last=False)
                self._current_cache_size -= len(oldest_raw)
                old_meta = self._metadata.pop(oldest_id, {})
                old_emitter = old_meta.get("emitter")
                if old_emitter:
                    self._per_emitter_usage[old_emitter] = max(0, self._per_emitter_usage[old_emitter] - len(oldest_raw))
                self._logger.debug("Evicted LRU blob %s", oldest_id)

        if self.blob_dir:
            self._persist_blob(blob_id, raw, meta)

        return BlobRef(blob_id=blob_id, size_bytes=blob_size, metadata=meta)

    # ... (retrieve, delete, replace_large_payloads, restore_large_payloads, persistence helpers remain the same as v1.3)
    def retrieve(self, blob_id: str) -> Optional[bytes]:
        if blob_id in self._blobs:
            self._blobs.move_to_end(blob_id)
            return self._blobs[blob_id]
        return None

    def retrieve_as(self, blob_id: str, as_type: str = "bytes") -> Any:
        raw = self.retrieve(blob_id)
        if raw is None:
            return None
        if as_type == "str":
            return raw.decode("utf-8", errors="replace")
        if as_type == "json":
            try:
                return json.loads(raw)
            except Exception:
                return None
        return raw

    def delete(self, blob_id: str) -> bool:
        if blob_id in self._blobs:
            size = len(self._blobs[blob_id])
            meta = self._metadata.get(blob_id, {})
            emitter = meta.get("emitter")
            self._blobs.pop(blob_id, None)
            self._metadata.pop(blob_id, None)
            self._current_cache_size = max(0, self._current_cache_size - size)
            if emitter:
                self._per_emitter_usage[emitter] = max(0, self._per_emitter_usage[emitter] - size)
        if self.blob_dir:
            try:
                (self.blob_dir / f"{blob_id}.bin").unlink(missing_ok=True)
                (self.blob_dir / f"{blob_id}.meta.json").unlink(missing_ok=True)
            except Exception:
                pass
        return True

    def _persist_blob(self, blob_id: str, raw: bytes, meta: Dict[str, Any]) -> None:
        if not self.blob_dir:
            return
        try:
            (self.blob_dir / f"{blob_id}.bin").write_bytes(raw)
            (self.blob_dir / f"{blob_id}.meta.json").write_text(json.dumps(meta, default=str), encoding="utf-8")
        except Exception as e:
            self._logger.error("Failed to persist blob %s: %s", blob_id, e)

    def _load_persisted_blobs(self) -> None:
        if not self.blob_dir or not self.blob_dir.exists():
            return
        loaded = 0
        for bin_file in self.blob_dir.glob("*.bin"):
            blob_id = bin_file.stem
            try:
                raw = bin_file.read_bytes()
                self._blobs[blob_id] = raw
                self._blobs.move_to_end(blob_id)
                self._current_cache_size += len(raw)
                meta_file = self.blob_dir / f"{blob_id}.meta.json"
                meta = {}
                if meta_file.exists():
                    meta = json.loads(meta_file.read_text(encoding="utf-8"))
                self._metadata[blob_id] = meta
                emitter = meta.get("emitter")
                if emitter:
                    self._per_emitter_usage[emitter] += len(raw)
                loaded += 1
            except Exception as e:
                self._logger.warning("Failed to load persisted blob %s: %s", blob_id, e)
        if loaded:
            self._logger.info("Loaded %d persisted blobs from %s", loaded, self.blob_dir)

    def replace_large_payloads(self, payload: Dict[str, Any], emitter: Optional[str] = None) -> Dict[str, Any]:
        new_payload = {}
        for k, v in payload.items():
            if self.should_use_blob(v):
                ref = self.store(v, emitter=emitter)
                new_payload[k] = {"__blob_ref__": True, "blob_id": ref.blob_id, "size_bytes": ref.size_bytes}
            else:
                new_payload[k] = v
        return new_payload

    def restore_large_payloads(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        restored = {}
        for k, v in payload.items():
            if isinstance(v, dict) and v.get("__blob_ref__"):
                blob_id = v.get("blob_id")
                restored[k] = self.retrieve_as(blob_id, "json") or self.retrieve(blob_id) if blob_id else v
            else:
                restored[k] = v
        return restored