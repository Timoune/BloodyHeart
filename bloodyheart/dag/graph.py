"""
bloodyheart.dag.graph
─────────────────────
DependencyDAG — Full-featured directed acyclic graph (from original).
"""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Set, Optional, Any

logger = logging.getLogger(__name__)


class DependencyType(str, Enum):
    HARD = "HARD"
    SOFT = "SOFT"


@dataclass
class DependencyEdge:
    target: str
    dep_type: DependencyType = DependencyType.HARD
    capabilities: List[str] = field(default_factory=list)


class DependencyDAG:
    def __init__(self):
        self._nodes: Set[str] = set()
        self._edges: Dict[str, List[DependencyEdge]] = defaultdict(list)
        self._reverse_edges: Dict[str, List[str]] = defaultdict(list)
        self._logger = logging.getLogger(f"{__name__}.DependencyDAG")

    def register(self, manifest: Any) -> None:
        name = getattr(manifest, "name", None)
        if not name:
            raise ValueError("Manifest must have a 'name' attribute")
        deps = getattr(manifest, "dependencies", []) or []
        self._nodes.add(name)
        if name not in self._edges:
            self._edges[name] = []
        for dep in deps:
            self._nodes.add(dep)
            edge = DependencyEdge(target=dep, dep_type=DependencyType.HARD)
            self._edges[name].append(edge)
            self._reverse_edges[dep].append(name)
        self._logger.debug("Registered module '%s' with %d dependencies", name, len(deps))

    def get_startup_order(self) -> List[str]:
        in_degree = {node: 0 for node in self._nodes}
        for module, edges in self._edges.items():
            for edge in edges:
                in_degree[module] += 1
        queue = deque([node for node, degree in in_degree.items() if degree == 0])
        order = []
        while queue:
            node = queue.popleft()
            order.append(node)
            for dependent in self._reverse_edges.get(node, []):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        if len(order) != len(self._nodes):
            raise ValueError("Circular dependency detected in module graph")
        return order

    def get_shutdown_order(self) -> List[str]:
        return list(reversed(self.get_startup_order()))

    def get_dependents(self, module_name: str) -> List[str]:
        if module_name not in self._nodes:
            return []
        visited = set()
        result = []
        def dfs(node):
            for dependent in self._reverse_edges.get(node, []):
                if dependent not in visited:
                    visited.add(dependent)
                    result.append(dependent)
                    dfs(dependent)
        dfs(module_name)
        return result

    def get_dependencies(self, module_name: str) -> List[str]:
        return [edge.target for edge in self._edges.get(module_name, [])]

    def has_cycle(self) -> bool:
        try:
            self.get_startup_order()
            return False
        except ValueError:
            return True

    def propagate_failure(self, failed_module: str) -> List[str]:
        return self.get_dependents(failed_module)