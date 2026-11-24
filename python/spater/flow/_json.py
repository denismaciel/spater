"""Read-only JSON snapshot of a validated DAG definition for hashing and inspection."""

from __future__ import annotations

import inspect
from datetime import timedelta
from typing import Any

from ._nodes import (
    Job,
    PerWorker,
    _BranchNode,
    _CallNode,
    _FailNode,
    _MapNode,
    _Node,
    _Scope,
    _SleepNode,
    _StepNode,
)
from ._types import NoneType, Signatures, Ty, assert_never, normalize, ref

FORMAT = 3


def job_to_dict(job: Job[Any, Any]) -> dict[str, Any]:
    sigs = job._signatures
    data: dict[str, Any] = {
        "spater_flow": FORMAT,
        "name": job.name,
        "input": _boundary(job.input),
        "output": _boundary(job.output),
        "provides": [
            {"per_worker" if isinstance(p, PerWorker) else "per_step": _fn(p.fn, sigs, provider=True)}
            for p in job.provides
        ],
        "scope": _scope(job._root, sigs),
    }
    if job._on_fail is not None:
        data["on_fail"] = _node(job._on_fail, sigs)
    return data


def _boundary(annotation: Any) -> Any:
    if annotation is None:
        return None
    ty = normalize(annotation)
    if len(ty.alts) == 1:
        atom = next(iter(ty.alts))
        return "none" if atom is NoneType else Ty.of(atom).text(full=True)
    return {"union": sorted("none" if a is NoneType else Ty.of(a).text(full=True) for a in ty.alts)}


def _fn(fn: Any, sigs: Signatures, *, provider: bool = False) -> dict[str, Any]:
    sig = sigs.get(fn, provider=provider)
    out: dict[str, Any] = {"fn": sig.reference, "sig": sig.text(), "mode": sig.mode.value}
    parameters = {
        p.name: [p.kind.name, p.optional]
        for p in sig.params
        if p.kind is not inspect.Parameter.POSITIONAL_OR_KEYWORD or p.optional
    }
    if parameters:
        out["parameters"] = parameters
    return out


def _duration(value: timedelta | None) -> Any:
    if value is None:
        return None
    us = value // timedelta(microseconds=1)
    if us % 1_000_000 == 0:
        return us // 1_000_000
    seconds = value.total_seconds()
    if abs(us) < 2**52 and timedelta(seconds=seconds) == value:
        return seconds
    return {"microseconds": us}


def _scope(scope: _Scope, sigs: Signatures) -> dict[str, Any]:
    return {
        "nodes": [_node(n, sigs) for n in scope.nodes],
        "output": None if scope.output is None else scope.output._source,
    }


def _node(node: _Node, sigs: Signatures) -> dict[str, Any]:
    match node:
        case _StepNode():
            retry = node.retry
            retry_dict: dict[str, Any] | None = None
            if retry is not None:
                retry_dict = {
                    "on": [ref(t) if t is not NoneType else "none" for t in retry.targets()],
                    "max": retry.max,
                    "after": _duration(retry.after),
                    "backoff": retry.backoff,
                }
                if retry.max_delay is not None:
                    retry_dict["max_delay"] = _duration(retry.max_delay)
                if retry.unless is not None:
                    if isinstance(retry.unless, tuple):
                        retry_dict["unless"] = [ref(c) for c in retry.unless]
                    else:
                        retry_dict["unless"] = ref(retry.unless)
                if retry.defer is not None:
                    retry_dict["defer"] = ref(retry.defer)
                if retry.max_defer != 100:
                    retry_dict["max_defer"] = retry.max_defer
            out: dict[str, Any] = {
                "kind": "step",
                "id": node.id,
                **_fn(node.fn, sigs),
                "inputs": {param: src._source for param, src in node.bound},
                "after": list(node.after),
                "deps": list(node.deps),
                "timeout": _duration(node.timeout),
                "retry": retry_dict,
                "catch": [{"exception": ref(exc), "to": _fn(conv, sigs)} for exc, conv in node.catch],
            }
            if node.meta:
                out["meta"] = dict(node.meta)
            return out
        case _CallNode():
            return {
                "kind": "call",
                "id": node.id,
                "arg": None if node.arg is None else node.arg._source,
                "job": job_to_dict(node.job),
                "after": list(node.after),
                "deps": list(node.deps),
            }
        case _MapNode():
            return {
                "kind": "map",
                "id": node.id,
                "items": node.items._source,
                "job": job_to_dict(node.job),
                "max_parallel": node.max_parallel,
                "after": list(node.after),
                "deps": list(node.deps),
            }
        case _SleepNode():
            return {
                "kind": "sleep",
                "id": node.id,
                "duration": _duration(node.duration)
                if isinstance(node.duration, timedelta)
                else _fn(node.duration, sigs),
                "inputs": {param: src._source for param, src in node.bound},
                "after": list(node.after),
                "deps": list(node.deps),
            }
        case _FailNode():
            return {
                "kind": "fail",
                "id": node.id,
                "reason": node.reason,
                "after": list(node.after),
                "deps": list(node.deps),
            }
        case _BranchNode():
            return {
                "kind": "branch",
                "id": node.id,
                "subject": node.subject._source,
                "after": list(node.after),
                "deps": list(node.deps),
                "cases": [
                    {
                        "on": "none" if case_cls is NoneType else ref(case_cls),
                        "scope": _scope(arm, sigs),
                    }
                    for case_cls, arm in node.arms.items()
                ],
            }
        case _:
            assert_never(node)
