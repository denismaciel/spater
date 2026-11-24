"""Declarative typed DAGs: plain business functions wired with explicit `Ref[T]` handles.

Business code stays plain Python with type annotations and zero Spater imports.
A wiring module defines a `Job` with `@Job.define(...)`, validated immediately::

    from spater.flow import Builder, Job, Ref

    @Job.define("nightly_report", provides=[DB])
    def NIGHTLY(f: Builder, day: Ref[Day]) -> Ref[Report]:
        rows = f.step(reports.extract, day)
        report = f.step(reports.transform, day, rows)
        f.step(reports.load, report)
        return report

Independent steps run concurrently up to `Runner(..., concurrency=...)`.
Runner executes a job directly; FlowQueue dispatches whole jobs through a Spater worker.
"""

from spater.context import Context

from ._executor import LocalExecutor, StepCall, StepErrorHandler, StepExecutor, StepMiddleware, StepNext
from ._graph import Graph, GraphEdge, GraphNode
from ._nodes import (
    Builder,
    DuplicatePolicy,
    FlowError,
    Job,
    PerStep,
    PerWorker,
    Problem,
    Ref,
    Retry,
    RunExistsError,
)
from ._run_queue import FlowQueue
from ._runner import Failed, Runner, Succeeded

__all__ = [
    "Builder",
    "Context",
    "DuplicatePolicy",
    "Failed",
    "FlowError",
    "FlowQueue",
    "Graph",
    "GraphEdge",
    "GraphNode",
    "Job",
    "LocalExecutor",
    "PerStep",
    "PerWorker",
    "Problem",
    "Ref",
    "Retry",
    "RunExistsError",
    "Runner",
    "StepCall",
    "StepErrorHandler",
    "StepExecutor",
    "StepMiddleware",
    "StepNext",
    "Succeeded",
]
