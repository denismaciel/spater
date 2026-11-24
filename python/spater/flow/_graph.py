"""Project a validated DAG onto nodes and edges for Mermaid, Graphviz and the ops UI."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from html import escape
from typing import Any

from ._nodes import (
    Job,
    _BranchNode,
    _CallNode,
    _FailNode,
    _MapNode,
    _Scope,
    _SleepNode,
    _StepNode,
)
from ._types import NoneType, assert_never, describe, normalize

START = "start"
DONE = "done"


@dataclass(frozen=True)
class GraphNode:
    id: str
    kind: str  # start | done | step | sleep | decide | map | fail
    label: str


@dataclass(frozen=True)
class GraphEdge:
    src: str
    dst: str
    label: str | None = None


@dataclass(frozen=True)
class Graph:
    job: str
    nodes: tuple[GraphNode, ...]
    edges: tuple[GraphEdge, ...]

    def to_mermaid(self, *, current: str | None = None) -> str:
        """Mermaid flowchart. `current` highlights the node a run is at."""
        ids = {node.id: f"n{i}" for i, node in enumerate(self.nodes)}
        lines = ["flowchart TD"]
        for node in self.nodes:
            opening, closing = _MERMAID_SHAPES[node.kind]
            lines.append(f'    {ids[node.id]}{opening}"{escape(node.label)}"{closing}')
        for edge in self.edges:
            arrow = f"-->|{edge.label}|" if edge.label else "-->"
            lines.append(f"    {ids[edge.src]} {arrow} {ids[edge.dst]}")
        if current is not None:
            lines.append(f"    style {ids[current]} fill:#fde68a,stroke:#b45309,stroke-width:3px,color:#000")
        return "\n".join(lines)

    def to_dot(self, *, current: str | None = None) -> str:
        """Graphviz source."""
        lines = [
            f"digraph {json.dumps(self.job, ensure_ascii=False)} {{",
            '    node [fontname="Helvetica"]; edge [fontname="Helvetica", fontsize=10];',
        ]
        for node in self.nodes:
            attrs = f"label={json.dumps(node.label, ensure_ascii=False)}, {_DOT_SHAPES[node.kind]}"
            if node.id == current:
                attrs += ', style=filled, fillcolor="#fde68a", penwidth=3'
            lines.append(f"    {json.dumps(node.id)} [{attrs}];")
        for edge in self.edges:
            label = f" [label={json.dumps(edge.label, ensure_ascii=False)}]" if edge.label else ""
            lines.append(f"    {json.dumps(edge.src)} -> {json.dumps(edge.dst)}{label};")
        lines.append("}")
        return "\n".join(lines)


_MERMAID_SHAPES = {
    "start": ("([", "])"),
    "done": ("([", "])"),
    "step": ("[", "]"),
    "sleep": ("{{", "}}"),
    "decide": ("{", "}"),
    "map": ("[[", "]]"),
    "call": ("[[", "]]"),
    "fail": ("([", "])"),
}

_DOT_SHAPES = {
    "start": "shape=oval",
    "done": "shape=oval",
    "step": "shape=box",
    "sleep": "shape=hexagon",
    "decide": "shape=diamond",
    "map": "shape=box3d",
    "call": "shape=box3d",
    "fail": "shape=oval, color=red, fontcolor=red",
}

_Exit = tuple[str, str | None]


def project(job: Job[Any, Any]) -> Graph:
    nodes: list[GraphNode] = [GraphNode(START, "start", f"start: {_cls(job.input)}")]
    edges: list[GraphEdge] = []
    seen_edges: set[GraphEdge] = set()

    def add_edge(src: str, dst: str, label: str | None = None) -> None:
        edge = GraphEdge(src, dst, label)
        if edge not in seen_edges:
            seen_edges.add(edge)
            edges.append(edge)

    def emit_scope(scope: _Scope, entries: list[_Exit]) -> list[_Exit]:
        if not scope.nodes:
            return list(entries)
        exits_by_id: dict[str, list[_Exit]] = {}
        consumed: set[str] = set()
        for node in scope.nodes:
            consumed.update(node.deps)

        for node in scope.nodes:
            if node.deps:
                incoming = [ex for dep in node.deps for ex in exits_by_id[dep]]
            else:
                incoming = entries

            match node:
                case _StepNode():
                    nodes.append(GraphNode(node.id, "step", _step_label(node)))
                    for src, lbl in incoming:
                        add_edge(src, node.id, lbl)
                    exits_by_id[node.id] = [(node.id, None)]
                case _SleepNode():
                    nodes.append(GraphNode(node.id, "sleep", f"sleep {_duration(node.duration)}"))
                    for src, lbl in incoming:
                        add_edge(src, node.id, lbl)
                    exits_by_id[node.id] = [(node.id, None)]
                case _MapNode():
                    nodes.append(GraphNode(node.id, "map", f"{node.id} ⇉ {node.job.name}"))
                    for src, lbl in incoming:
                        add_edge(src, node.id, lbl)
                    exits_by_id[node.id] = [(node.id, None)]
                case _CallNode():
                    nodes.append(GraphNode(node.id, "call", f"{node.id} → {node.job.name}"))
                    for src, lbl in incoming:
                        add_edge(src, node.id, lbl)
                    exits_by_id[node.id] = [(node.id, None)]
                case _FailNode():
                    nodes.append(GraphNode(node.id, "fail", f"fail: {node.reason}"))
                    for src, lbl in incoming:
                        add_edge(src, node.id, lbl)
                    exits_by_id[node.id] = []
                case _BranchNode():
                    nodes.append(GraphNode(node.id, "decide", f"{node.id}?"))
                    for src, lbl in incoming:
                        add_edge(src, node.id, lbl)
                    arm_exits: list[_Exit] = []
                    for case_cls, arm_scope in node.arms.items():
                        case_label = _case_label(case_cls)
                        arm_exits.extend(emit_scope(arm_scope, [(node.id, case_label)]))
                    exits_by_id[node.id] = arm_exits
                case _:
                    assert_never(node)

        surviving: list[_Exit] = []
        for node in scope.nodes:
            if node.id not in consumed:
                surviving.extend(exits_by_id[node.id])
        return surviving

    leaves = emit_scope(job._root, [(START, None)])
    done_label = f"done: {_cls(job.output)}" if job.output is not None else "done"
    nodes.append(GraphNode(DONE, "done", done_label))
    for src, lbl in leaves:
        add_edge(src, DONE, lbl)

    return Graph(job.name, tuple(nodes), tuple(edges))


def _step_label(node: _StepNode) -> str:
    label = node.id
    if node.retry is not None:
        targets = " | ".join(_cls(t) for t in node.retry.targets())
        label += f" (retry ×{node.retry.max} on {targets})"
    return label


def _cls(obj: Any) -> str:
    return "nothing" if obj is None else normalize(obj).text()


def _case_label(key: type[Any]) -> str:
    return "None" if key is NoneType else _cls(key)


def _duration(value: timedelta | Callable[..., timedelta]) -> str:
    if not isinstance(value, timedelta):
        return f"{describe(value)}(...)"
    seconds = value.total_seconds()
    for unit, size in (("d", 86400), ("h", 3600), ("min", 60)):
        if seconds >= size and seconds % size == 0:
            return f"{int(seconds // size)}{unit}"
    return f"{seconds:g}s"
