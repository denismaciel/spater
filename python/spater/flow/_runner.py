"""Run validated DAGs concurrently and resume them from committed node outcomes."""

from __future__ import annotations

import asyncio
import heapq
import inspect
import json
import logging
import math
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload
from uuid import NAMESPACE_URL, uuid4, uuid5

from spater.inspector import Inspector

from ._executor import LocalExecutor, StepCall, StepErrorHandler, StepExecutor, StepMiddleware, StepNext
from ._graph import Graph, GraphEdge, GraphNode
from ._nodes import (
    DuplicatePolicy,
    FlowFailure,
    Job,
    RunExistsError,
    _BranchNode,
    _CallNode,
    _FailNode,
    _MapNode,
    _Node,
    _Scope,
    _SleepNode,
    _StepNode,
)
from ._run_calls import Env, Stored, call_sync, stored_inputs
from ._run_store import ClaimLost, InputConflict, ObsoleteDelivery, PendingWake, open_store
from ._run_tasks import settle
from ._run_values import canonical_input, decode, encode, preview, selected_type
from ._types import ListOf, NoneType, Ty, assert_never, normalize, ref

if TYPE_CHECKING:
    from typing_extensions import Self

log = logging.getLogger(__name__)

InT = TypeVar("InT")
OutT = TypeVar("OutT")
OutT_co = TypeVar("OutT_co", covariant=True)
T = TypeVar("T")


@dataclass(frozen=True)
class Succeeded(Generic[OutT_co]):
    run_id: str
    value: OutT_co


@dataclass(frozen=True)
class Failed:
    run_id: str
    node_id: str
    reason: str


@dataclass(frozen=True)
class Parked:
    due: float


class _Failure(Exception):
    def __init__(self, node_id: str, reason: str) -> None:
        super().__init__(reason)
        self.node_id = node_id


def _case_ref(cls: type[Any]) -> str:
    return "none" if cls is NoneType else ref(cls)


class Runner:
    """Execute DAG jobs against a Postgres or named in-memory store.

    Use as an async context manager to release worker-scoped providers and pooled
    database connections cleanly on shutdown.
    """

    def __init__(
        self,
        url: str,
        *,
        concurrency: int = 10,
        preview_bytes: int = 16_384,
        executor: StepExecutor | None = None,
        middleware: Sequence[StepMiddleware] = (),
        on_error: Sequence[StepErrorHandler] = (),
        pool_size: int = 4,
    ) -> None:
        if type(concurrency) is not int or concurrency < 1:
            raise ValueError("concurrency must be a positive integer")
        if type(preview_bytes) is not int or preview_bytes < 4:
            raise ValueError("preview_bytes must be an integer of at least 4")
        self.url = url
        self.concurrency = concurrency
        self.preview_bytes = preview_bytes
        self.store = open_store(url, pool_size=pool_size)
        self.executor: StepExecutor = LocalExecutor() if executor is None else executor
        self._middleware: list[StepMiddleware] = list(middleware)
        self._error_handlers: list[StepErrorHandler] = list(on_error)
        self._inspector: Inspector | None = None

    def middleware(self, fn: StepMiddleware) -> StepMiddleware:
        """Register step middleware (first registered is outermost)."""
        self._middleware.append(fn)
        return fn

    def on_error(self, fn: StepErrorHandler) -> StepErrorHandler:
        """Register a hook called whenever a step attempt fails terminally."""
        self._error_handlers.append(fn)
        return fn

    async def _execute(self, call: StepCall) -> Any:
        call_next: StepNext = self.executor.execute
        for mw in reversed(self._middleware):
            nxt = call_next

            async def wrapped(c: StepCall, _mw: StepMiddleware = mw, _nxt: StepNext = nxt) -> Any:
                return await _mw(c, _nxt)

            call_next = wrapped
        return await call_next(call)

    async def _notify_error(self, call: StepCall, exc: BaseException) -> None:
        for handler in self._error_handlers:
            try:
                res = handler(call, exc)
                if inspect.isawaitable(res):
                    await res
            except Exception:
                log.exception("flow on_error hook failed for %s/%s", call.run_id, call.node_id)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        try:
            await self.executor.aclose()
        finally:
            self._inspector = None
            await self.store.aclose()

    def validate(self, job: Job[Any, Any], input: Any) -> Any:
        return encode(input, normalize(job.input))

    async def prepare(
        self,
        job: Job[Any, Any],
        *,
        input: Any = None,
        run_id: str | None = None,
        rerun_of: str | None = None,
    ) -> str:
        data = self.validate(job, input)
        identity = run_id if run_id is not None else str(uuid4())
        await self._create(job, identity, data, identity, rerun_of=rerun_of)
        return identity

    def _build_run(
        self,
        job: Job[Any, Any],
        identity: str,
        data: Any,
        root: str,
        *,
        rerun_of: str | None = None,
        broker_queue: str | None = None,
        parent: str | None = None,
        node: str | None = None,
        index: int | None = None,
    ) -> dict[str, Any]:
        run: dict[str, Any] = {
            "id": identity,
            "definition": job.digest,
            "job": job.name,
            "root_id": root,
            "input": data,
            "status": "running",
            "started_at": self.store.clock(),
            "rerun_of": rerun_of,
            "parent_id": parent,
            "parent_node": node,
            "parent_attempt": 1 if parent is not None else None,
            "item_index": index,
        }
        if broker_queue is not None:
            run.update(broker_queue=broker_queue, delivery={"expected": identity})
        return run

    async def _create(
        self,
        job: Job[Any, Any],
        identity: str,
        data: Any,
        root: str,
        *,
        rerun_of: str | None = None,
        broker_queue: str | None = None,
        owner: tuple[str, int] | None = None,
        parent: str | None = None,
        node: str | None = None,
        index: int | None = None,
        fail_if_exists: bool = False,
    ) -> bool:
        run = self._build_run(
            job,
            identity,
            data,
            root,
            rerun_of=rerun_of,
            broker_queue=broker_queue,
            parent=parent,
            node=node,
            index=index,
        )
        definition, graph = job.to_json(), job._graph_dict()
        try:
            return await self.store.create(run, definition, owner=owner, graph=graph, fail_if_exists=fail_if_exists)
        except InputConflict as conflict:
            canonical = canonical_input(conflict.prior_input, normalize(job.input))
            if json.dumps(canonical, sort_keys=True) != json.dumps(data, sort_keys=True):
                raise
            return await self.store.create(
                run | {"input": conflict.prior_input},
                definition,
                owner=owner,
                graph=graph,
                fail_if_exists=fail_if_exists,
            )

    @overload
    async def run(
        self,
        job: Job[None, OutT],
        input: None = None,
        *,
        run_id: str | None = None,
        on_duplicate: DuplicatePolicy = "fail",
    ) -> Succeeded[OutT] | Failed: ...

    @overload
    async def run(
        self,
        job: Job[InT, OutT],
        input: InT,
        *,
        run_id: str | None = None,
        on_duplicate: DuplicatePolicy = "fail",
    ) -> Succeeded[OutT] | Failed: ...

    async def run(
        self,
        job: Job[Any, Any],
        input: Any = None,
        *,
        run_id: str | None = None,
        on_duplicate: DuplicatePolicy = "fail",
    ) -> Succeeded[Any] | Failed:
        if on_duplicate not in ("fail", "use_existing"):
            raise ValueError("on_duplicate must be 'fail' or 'use_existing'")
        data = self.validate(job, input)
        if run_id is not None and on_duplicate == "use_existing":
            try:
                existing = await self.store.get(run_id)
            except KeyError:
                pass
            else:
                if existing["job"] != job.name:
                    raise ValueError(f"run {run_id!r} belongs to job {existing['job']!r}, not {job.name!r}")
                if existing["status"] == "succeeded":
                    return Succeeded(run_id, decode(existing["result"], normalize(job.output)))
                if existing["status"] == "failed":
                    return Failed(run_id, existing["node_id"], existing["error"])
                if existing["status"] in ("cancelling", "cancelled"):
                    raise asyncio.CancelledError
                raise RunExistsError(f"run {run_id!r} is already running")
        return await self._direct(job, data, run_id=run_id, fail_if_exists=run_id is not None)

    async def rerun(self, job: Job[Any, OutT], run_id: str) -> Succeeded[OutT] | Failed:
        old = await self.store.get(run_id)
        if old["job"] != job.name:
            raise ValueError("rerun requires the same job name")
        data = decode(old["input"], normalize(job.input))
        return await self._direct(job, self.validate(job, data), rerun_of=run_id)

    async def cancel(self, run_id: str) -> dict[str, Any]:
        """Cancel a root run. Active workers are fenced on their next state write."""
        return await self.store.cancel(run_id)

    async def inspect(self, run_id: str) -> dict[str, Any]:
        row = await self._inspect_row(await self.store.get(run_id))
        return {**row, "attempts": await self.store.attempts(run_id), "children": await self.store.children(run_id)}

    async def _inspect_row(self, row: dict[str, Any]) -> dict[str, Any]:
        if row["status"] == "running" and ("delivery" in row or row["id"] != row["root_id"]):
            from ._run_queue import _reconcile_delivery

            watch = await self.store.delivery_watch(row["root_id"])
            if watch:
                if self._inspector is None:
                    self._inspector = Inspector(self.url)
                if await _reconcile_delivery(self.store, self._inspector, watch):
                    row = await self.store.get(row["id"])
        return row

    async def graph(self, run_id: str) -> Graph:
        row = await self.store.get(run_id)
        data = await self.store.graph(row["definition"])
        return Graph(
            data["job"], tuple(GraphNode(**n) for n in data["nodes"]), tuple(GraphEdge(**e) for e in data["edges"])
        )

    async def deliver(
        self,
        job: Job[Any, OutT],
        run_id: str,
        *,
        delivery_id: str | None = None,
        next_delivery_id: str | None = None,
    ) -> Succeeded[OutT] | Failed | Parked | ObsoleteDelivery:
        """Drive a queued run until completion or its next wake time.

        A broker supplies both message IDs. A manual replacement driver explicitly
        takes ownership by omitting them, e.g. when testing crash recovery.
        """
        if (delivery_id is None) != (next_delivery_id is None):
            raise ValueError("supply both delivery IDs together")
        row = await self.store.get(run_id)
        if row["root_id"] != run_id:
            raise ValueError("resume the root run of a mapped child")
        if job.digest != row["definition"]:
            raise ValueError("job definition changed; start a manual rerun")
        self.validate(job, decode(row["input"], normalize(job.input)))
        if delivery_id is None:
            generation = await self.store.claim(run_id)
        else:
            claim = await self.store.claim_delivery(run_id, delivery_id)
            if isinstance(claim, PendingWake):
                return Parked(claim.due)
            if isinstance(claim, ObsoleteDelivery):
                return claim
            generation = claim
        execution = _Execution(self, (run_id, generation), True)
        result = await execution.job(job, run_id)
        if isinstance(result, Parked) and delivery_id is not None and next_delivery_id is not None:
            await self.store.park_delivery(run_id, delivery_id, next_delivery_id, result.due, execution.owner)
        return result

    async def _direct(
        self,
        job: Job[Any, OutT],
        data: Any,
        *,
        run_id: str | None = None,
        rerun_of: str | None = None,
        fail_if_exists: bool = False,
    ) -> Succeeded[OutT] | Failed:
        identity = str(uuid4()) if run_id is None else run_id
        run = self._build_run(job, identity, data, identity, rerun_of=rerun_of)

        async def start() -> _Execution:
            generation = await self.store.create_and_claim(
                run, job.to_json(), graph=job._graph_dict(), fail_if_exists=fail_if_exists
            )
            return _Execution(self, (identity, generation), False)

        admission = asyncio.create_task(start())
        work: asyncio.Task[Succeeded[OutT] | Failed | Parked] | None = None

        async def cancel_and_drain() -> None:
            try:
                execution = await admission
            except RunExistsError:
                return
            try:
                await self.store.update(identity, {"status": "cancelling"}, execution.owner)
            except ClaimLost:
                if (await self.store.get(identity))["status"] not in ("failed", "succeeded", "cancelled"):
                    raise
            finally:
                if work is not None:
                    work.cancel()
                    await asyncio.gather(work, return_exceptions=True)
            row = await self.store.get(identity)
            if row.get("pending_failure"):
                await self.store.finish_failure(identity, execution.owner, self.store.clock())
            elif row["status"] == "cancelling":
                await self.store.update(
                    identity, {"status": "cancelled", "finished_at": self.store.clock()}, execution.owner
                )

        try:
            execution = await asyncio.shield(admission)
            work = asyncio.create_task(execution.job(job, identity, initial_row=run, fresh=True))
            result = await asyncio.shield(work)
            assert not isinstance(result, Parked)
            return result
        except asyncio.CancelledError:
            if work is not None:
                admission.result().stopping = True
            await settle(cancel_and_drain())
            raise


class _Execution:
    def __init__(self, runner: Runner, owner: tuple[str, int], queued: bool) -> None:
        self.runner = runner
        self.store = runner.store
        self.owner = owner
        self.queued = queued
        self.slots = asyncio.Semaphore(runner.concurrency)
        self.child_admissions = asyncio.Semaphore(runner.concurrency)
        self.stopping = False
        self.failure: _Failure | None = None
        self._attempts: dict[str, dict[tuple[str, int], dict[str, Any]]] = {}

    def clock(self) -> float:
        return self.store.clock()

    def admitting(self) -> None:
        if self.failure is not None:
            raise self.failure
        if self.stopping:
            raise asyncio.CancelledError

    async def job(
        self,
        job: Job[Any, OutT],
        identity: str,
        *,
        initial_row: dict[str, Any] | None = None,
        fresh: bool = False,
    ) -> Succeeded[OutT] | Failed | Parked:
        row = initial_row if initial_row is not None else await self.store.get(identity)
        output_ty = normalize(job.output)
        if identity == self.owner[0] and row.get("pending_failure"):
            row = await self.store.finish_failure(identity, self.owner, self.clock())
        if row["status"] == "succeeded":
            return Succeeded(identity, decode(row["result"], output_ty))
        if row["status"] == "failed":
            return Failed(identity, row["node_id"], row["error"])
        if row["status"] in ("cancelling", "cancelled"):
            if row["status"] == "cancelling":
                await self.store.update(identity, {"status": "cancelled", "finished_at": self.clock()}, self.owner)
            raise asyncio.CancelledError

        if identity not in self._attempts:
            if fresh:
                self._attempts[identity] = {}
            else:
                loaded = await self.store.attempts_with_payload(identity)
                self._attempts[identity] = {(a["node_id"], int(a["attempt"])): a for a in loaded}

        env: dict[tuple[int, str], Stored] = {}
        if job.input is not None:
            env[(0, "$input")] = Stored(normalize(job.input), row["input"])
        try:
            final = await self.scope(job, identity, job._root, env)
            if isinstance(final, Parked):
                return final
            self.admitting()
            if job.output is None or final is None:
                value: Any = None
            else:
                value = decode(final.encoded, final.ty)
            encoded = encode(value, output_ty)
        except _Failure as failure:
            if job._on_fail is not None:
                await self._run_on_fail(job, identity, job._on_fail, env, failure)
            self._attempts.pop(identity, None)
            if identity != self.owner[0]:
                raise
            row = await self.store.finish_failure(identity, self.owner, self.clock())
            return Failed(identity, row["node_id"], row["error"])
        await self.store.update(
            identity, {"status": "succeeded", "finished_at": self.clock(), "result": encoded}, self.owner
        )
        self._attempts.pop(identity, None)
        return Succeeded(identity, decode(encoded, output_ty))

    def _cached_attempt(self, identity: str, node: str, attempt: int) -> dict[str, Any] | None:
        return self._attempts[identity].get((node, attempt))

    async def put(self, identity: str, node: str, attempt: int, **fields: Any) -> None:
        await self.store.put_attempt(identity, node, attempt, fields, self.owner)
        bucket = self._attempts.setdefault(identity, {})
        key = (node, attempt)
        existing = bucket.get(key)
        if existing is None:
            bucket[key] = {"run_id": identity, "node_id": node, "attempt": attempt, **fields}
        else:
            bucket[key] = existing | fields

    async def begin(self, identity: str, node: str, attempt: int) -> dict[str, Any]:
        row = self._cached_attempt(identity, node, attempt)
        if row is None:
            self.admitting()
            row = {"status": "running", "started_at": self.clock()}
            await self.put(identity, node, attempt, **row)
        if row["status"] == "failed":
            raise _Failure(node, str(row["error"]))
        return row

    async def fail(self, identity: str, node: str, attempt: int, reason: str) -> _Failure:
        failure = _Failure(node, reason)
        self.failure = self.failure or failure
        await self.put(identity, node, attempt, status="failed", error=reason, finished_at=self.clock())
        return failure

    async def wait(self, due: float, identity: str) -> Parked | None:
        delay = due - self.clock()
        if delay <= 0:
            return None
        if self.queued or identity != self.owner[0]:
            return Parked(due)
        await self.store.wait_until(due)
        return None

    async def scope(self, job: Job[Any, Any], identity: str, scope: _Scope, outer_env: Env) -> Stored | None | Parked:
        """Execute all nodes in a DAG scope concurrently as soon as their dependencies complete."""
        env: dict[tuple[int, str], Stored] = dict(outer_env)
        if not scope.nodes:
            if scope.output is None:
                return None
            return env[(scope.output._scope, scope.output._source)]

        remaining: list[_Node] = list(scope.nodes)
        completed: set[str] = set()
        active: dict[asyncio.Future[Stored | None | Parked], _Node] = {}
        sleeping: list[tuple[float, int, _Node]] = []
        sleep_seq = 0
        timer: asyncio.Task[None] | None = None

        try:
            while remaining or sleeping or active:
                self.admitting()
                while sleeping and sleeping[0][0] <= self.clock():
                    _, _, node = heapq.heappop(sleeping)
                    active[asyncio.ensure_future(self.node(job, identity, node, env))] = node

                still_waiting: list[_Node] = []
                for node in remaining:
                    if all(dep in completed for dep in node.deps):
                        active[asyncio.ensure_future(self.node(job, identity, node, env))] = node
                    else:
                        still_waiting.append(node)
                remaining = still_waiting

                if not active:
                    if sleeping:
                        parked = await self.wait(sleeping[0][0], identity)
                        if parked is not None:
                            return parked
                        continue
                    break

                pending: set[asyncio.Future[Any]] = set(active)
                if sleeping:
                    timer = asyncio.create_task(self.store.wait_until(sleeping[0][0]))
                    pending.add(timer)
                await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                if timer is not None:
                    if timer.done():
                        timer.result()
                    else:
                        timer.cancel()
                        await asyncio.gather(timer, return_exceptions=True)
                    timer = None

                for task in tuple(active):
                    if not task.done():
                        continue
                    node = active.pop(task)
                    outcome = task.result()
                    if isinstance(outcome, Parked):
                        sleep_seq += 1
                        heapq.heappush(sleeping, (outcome.due, sleep_seq, node))
                    else:
                        completed.add(node.id)
                        if outcome is not None:
                            env[(scope.scope_id, node.id)] = outcome
        finally:
            unfinished: list[asyncio.Future[Any]] = list(active)
            if timer is not None:
                unfinished.append(timer)
            for unfinished_call in unfinished:
                unfinished_call.cancel()
            await settle(asyncio.gather(*unfinished, return_exceptions=True))

        if scope.output is None:
            return None
        return env[(scope.output._scope, scope.output._source)]

    async def _run_on_fail(
        self,
        job: Job[Any, Any],
        identity: str,
        node: _StepNode,
        env: Env,
        failure: _Failure,
    ) -> None:
        row = self._cached_attempt(identity, node.id, 1)
        if row is not None and row["status"] == "succeeded":
            return
        if row is None:
            row = {
                "status": "running",
                "started_at": self.clock(),
                "input_preview": preview({"value": stored_inputs(node.bound, env)}, self.runner.preview_bytes),
            }
            if node.timeout is not None:
                row["deadline"] = time.time() + node.timeout.total_seconds()
            await self.put(identity, node.id, 1, **row)
        raw_deadline = row.get("deadline")
        deadline = float(raw_deadline) if isinstance(raw_deadline, (int, float)) else None
        step_call = StepCall(
            job=job,
            run_id=identity,
            node_id=node.id,
            attempt=1,
            fn=node.fn,
            bound=node.bound,
            env=env,
            allow_providers=True,
            deadline=deadline,
            meta=node.meta,
            failure=FlowFailure(run_id=identity, node_id=failure.node_id, reason=str(failure)),
        )
        try:
            await self.runner._execute(step_call)
        except Exception as exc:
            log.exception("flow on_fail handler %s failed for %s", node.id, identity)
            await self.put(
                identity, node.id, 1, status="failed", error=f"{type(exc).__name__}: {exc}", finished_at=self.clock()
            )
            return
        await self.put(identity, node.id, 1, status="succeeded", finished_at=self.clock())

    async def node(self, job: Job[Any, Any], identity: str, node: _Node, env: Env) -> Stored | None | Parked:
        match node:
            case _StepNode():
                return await self.step_node(job, identity, node, env)
            case _BranchNode():
                return await self.branch_node(job, identity, node, env)
            case _SleepNode():
                return await self.sleep_node(job, identity, node, env)
            case _MapNode():
                return await self.map_node(job, identity, node, env)
            case _CallNode():
                return await self.call_node(identity, node, env)
            case _FailNode():
                await self.begin(identity, node.id, 1)
                raise await self.fail(identity, node.id, 1, node.reason)
        assert_never(node)

    async def call_node(self, identity: str, node: _CallNode, env: Env) -> Stored | None | Parked:
        row = await self.begin(identity, node.id, 1)
        if row["status"] == "succeeded":
            return None if node.job.output is None else Stored(node.returns, row["output"])
        child = str(uuid5(NAMESPACE_URL, json.dumps([identity, node.id, 1])))
        if node.arg is None:
            encoded_arg = encode(None, normalize(None))
        else:
            arg_stored = env[(node.arg._scope, node.arg._source)]
            arg_val = decode(arg_stored.encoded, arg_stored.ty)
            encoded_arg = encode(arg_val, normalize(node.job.input))
        async with self.child_admissions:
            self.admitting()
            created = await self.runner._create(
                node.job,
                child,
                encoded_arg,
                self.owner[0],
                owner=self.owner,
                parent=identity,
                node=node.id,
                index=0,
            )
        initial_row = {"id": child, "status": "running", "input": encoded_arg} if created else None
        outcome = await self.job(node.job, child, initial_row=initial_row, fresh=created)
        if isinstance(outcome, Parked):
            return outcome
        if isinstance(outcome, Failed):
            raise await self.fail(identity, node.id, 1, f"child {child}, {outcome.node_id}: {outcome.reason}")
        encoded = encode(outcome.value, node.returns) if node.job.output is not None else encode(None, normalize(None))
        await self.put(
            identity,
            node.id,
            1,
            status="succeeded",
            output=encoded,
            outcome_type=encoded["type"],
            preview=preview(encoded, self.runner.preview_bytes),
            finished_at=self.clock(),
        )
        return Stored(node.returns, encoded)

    async def step_node(self, job: Job[Any, Any], identity: str, node: _StepNode, env: Env) -> Stored | None | Parked:
        retry = node.retry
        exc_targets = tuple(t for t in retry.targets() if issubclass(t, Exception)) if retry else ()
        val_targets = tuple(t for t in retry.targets() if not issubclass(t, Exception)) if retry else ()
        attempt = 1
        business_failures = 0
        deferrals = 0

        while True:
            row = self._cached_attempt(identity, node.id, attempt)
            if row is not None:
                if row["status"] == "succeeded":
                    return Stored(node.returns, row["output"])
                if row["status"] == "failed":
                    raise _Failure(node.id, str(row["error"]))
                if row["status"] == "retried":
                    parked = await self.wait(row["due"], identity)
                    if parked is not None:
                        return parked
                    if row.get("deferred"):
                        deferrals += 1
                    else:
                        business_failures += 1
                    attempt += 1
                    continue

            async with self.slots:
                self.admitting()
                if row is None:
                    row = {"status": "running", "started_at": self.clock()}
                    row["input_preview"] = preview({"value": stored_inputs(node.bound, env)}, self.runner.preview_bytes)
                    if node.timeout is not None:
                        row["deadline"] = time.time() + node.timeout.total_seconds()
                    await self.put(identity, node.id, attempt, **row)
                self.admitting()

                fields: dict[str, Any]
                raw_deadline = row.get("deadline")
                deadline = float(raw_deadline) if isinstance(raw_deadline, (int, float)) else None
                step_call = StepCall(
                    job=job,
                    run_id=identity,
                    node_id=node.id,
                    attempt=attempt,
                    fn=node.fn,
                    bound=node.bound,
                    env=env,
                    allow_providers=True,
                    deadline=deadline,
                    meta=node.meta,
                )
                try:
                    try:
                        value = await self.runner._execute(step_call)
                    except Exception as exc:
                        deferred_delay = retry.defer(exc) if (retry is not None and retry.defer is not None) else None
                        if retry is not None and deferred_delay is not None and deferrals < retry.max_defer:
                            delay = max(0.0, deferred_delay.total_seconds())
                            if not math.isfinite(delay):
                                raise ValueError("defer delay exceeds the supported range") from exc
                            finished = self.clock()
                            fields = {
                                "status": "retried",
                                "deferred": True,
                                "error": f"{type(exc).__name__}: {exc}",
                                "finished_at": finished,
                                "due": finished + delay,
                            }
                        elif (
                            retry is not None
                            and exc_targets
                            and isinstance(exc, exc_targets)
                            and not retry.is_excluded(exc)
                            and business_failures + 1 < retry.max
                        ):
                            delay = retry.after.total_seconds() * retry.backoff**business_failures
                            if retry.max_delay is not None:
                                delay = min(delay, retry.max_delay.total_seconds())
                            if not math.isfinite(delay):
                                raise ValueError("retry delay exceeds the supported range") from exc
                            finished = self.clock()
                            fields = {
                                "status": "retried",
                                "error": f"{type(exc).__name__}: {exc}",
                                "finished_at": finished,
                                "due": finished + delay,
                            }
                        else:
                            converter = next((conv for cls, conv in node.catch if isinstance(exc, cls)), None)
                            if converter is None:
                                raise
                            value = await call_sync(converter, exc)
                            fields = self._finish_value(value, node, retry, val_targets, business_failures)
                    else:
                        fields = self._finish_value(value, node, retry, val_targets, business_failures)
                except Exception as exc:
                    await self.runner._notify_error(step_call, exc)
                    raise await self.fail(identity, node.id, attempt, f"{type(exc).__name__}: {exc}") from exc

            await self.put(identity, node.id, attempt, **fields)
            if fields["status"] == "succeeded":
                return Stored(node.returns, fields["output"])

    def _finish_value(
        self,
        value: Any,
        node: _StepNode,
        retry: Any,
        val_targets: tuple[type[Any], ...],
        business_failures: int,
    ) -> dict[str, Any]:
        encoded = encode(value, node.returns)
        summary = preview(encoded, self.runner.preview_bytes)
        finished = self.clock()
        fields: dict[str, Any] = {
            "output": encoded,
            "outcome_type": encoded["type"],
            "preview": summary,
            "finished_at": finished,
        }
        if retry is not None and val_targets and business_failures + 1 < retry.max:
            case = next(iter(selected_type(value, node.returns).alts))
            if case in val_targets:
                delay = retry.after.total_seconds() * retry.backoff**business_failures
                if retry.max_delay is not None:
                    delay = min(delay, retry.max_delay.total_seconds())
                if not math.isfinite(delay):
                    raise ValueError("retry delay exceeds the supported range")
                fields.update(status="retried", due=finished + delay)
                return fields
        fields["status"] = "succeeded"
        return fields

    async def branch_node(
        self, job: Job[Any, Any], identity: str, node: _BranchNode, env: Env
    ) -> Stored | None | Parked:
        subject_stored = env[(node.subject._scope, node.subject._source)]
        row = self._cached_attempt(identity, node.id, 1)
        if row is not None and row["status"] == "succeeded":
            chosen_cls = next(cls for cls in node.arms if _case_ref(cls) == row["arm"])
        else:
            self.admitting()
            started = self.clock()
            subject_val = decode(subject_stored.encoded, node.subject._ty)
            chosen_cls = next(iter(selected_type(subject_val, node.subject._ty).alts))
            finished = self.clock()
            await self.put(
                identity,
                node.id,
                1,
                status="succeeded",
                started_at=started,
                finished_at=finished,
                arm=_case_ref(chosen_cls),
                preview=preview(subject_stored.encoded, self.runner.preview_bytes),
            )

        arm_scope = node.arms[chosen_cls]
        arm_env = {**env, (arm_scope.scope_id, "$case"): Stored(Ty.of(chosen_cls), subject_stored.encoded)}
        result = await self.scope(job, identity, arm_scope, arm_env)
        if isinstance(result, Parked):
            return result
        if result is None or node.returns is None:
            return None
        if node.returns == Ty.of(NoneType):
            return Stored(node.returns, encode(None, node.returns))
        value = decode(result.encoded, result.ty)
        return Stored(node.returns, encode(value, node.returns))

    async def sleep_node(self, job: Job[Any, Any], identity: str, node: _SleepNode, env: Env) -> None | Parked:
        row = self._cached_attempt(identity, node.id, 1)
        if row is not None:
            if row["status"] == "failed":
                raise _Failure(node.id, str(row["error"]))
            if row["status"] == "succeeded":
                return None
        if row is None or "due" not in row:
            try:
                if isinstance(node.duration, timedelta):
                    self.admitting()
                    started = self.clock() if row is None else row["started_at"]
                    due = started + node.duration.total_seconds()
                    if row is None:
                        await self.put(identity, node.id, 1, status="running", started_at=started, due=due)
                    else:
                        await self.put(identity, node.id, 1, due=due)
                else:
                    if row is None:
                        self.admitting()
                        started = self.clock()
                        await self.put(
                            identity,
                            node.id,
                            1,
                            status="running",
                            started_at=started,
                            input_preview=preview({"value": stored_inputs(node.bound, env)}, self.runner.preview_bytes),
                        )
                    else:
                        started = row["started_at"]
                        await self.put(
                            identity,
                            node.id,
                            1,
                            input_preview=preview({"value": stored_inputs(node.bound, env)}, self.runner.preview_bytes),
                        )
                    async with self.slots:
                        self.admitting()
                        delta = await self.runner._execute(
                            StepCall(
                                job=job,
                                run_id=identity,
                                node_id=node.id,
                                attempt=1,
                                fn=node.duration,
                                bound=node.bound,
                                env=env,
                                allow_providers=False,
                            )
                        )
                    if not isinstance(delta, timedelta):
                        raise TypeError("duration function must return timedelta")
                    due = started + delta.total_seconds()
                    await self.put(identity, node.id, 1, due=due)
            except _Failure:
                raise
            except Exception as exc:
                raise await self.fail(identity, node.id, 1, str(exc)) from exc
        else:
            due = row["due"]
        parked = await self.wait(due, identity)
        if parked is not None:
            return parked
        await self.put(identity, node.id, 1, status="succeeded", finished_at=self.clock())
        return None

    async def map_node(self, job: Job[Any, Any], identity: str, node: _MapNode, env: Env) -> Stored | None | Parked:
        row = self._cached_attempt(identity, node.id, 1)
        ty = Ty.of(ListOf(normalize(node.job.output)))
        if row is not None:
            if row["status"] == "failed":
                raise _Failure(node.id, str(row["error"]))
            if row["status"] == "succeeded":
                return None if node.job.output is None else Stored(ty, row["output"])
        items_ty = node.items._ty
        if row is None or "manifest" not in row:
            try:
                items_stored = env[(node.items._scope, node.items._source)]
                manifest = items_stored.encoded
                fields: dict[str, Any] = {
                    "input_preview": preview({"value": {"items": manifest["value"]}}, self.runner.preview_bytes),
                    "manifest": manifest,
                }
                if row is None:
                    self.admitting()
                    fields["status"] = "running"
                    fields["started_at"] = self.clock()
                await self.put(identity, node.id, 1, **fields)
            except _Failure:
                raise
            except Exception as exc:
                raise await self.fail(identity, node.id, 1, str(exc)) from exc
        else:
            manifest = row["manifest"]
        items = decode(manifest, items_ty)
        results = await self.map_children(identity, node, items)
        if isinstance(results, Parked):
            return results
        encoded = encode(results, ty) if node.job.output is not None else encode(None, normalize(None))
        await self.put(
            identity,
            node.id,
            1,
            status="succeeded",
            output=encoded,
            outcome_type=encoded["type"],
            preview=preview(encoded, self.runner.preview_bytes),
            finished_at=self.clock(),
        )
        return None if node.job.output is None else Stored(ty, encoded)

    async def map_children(self, identity: str, node: _MapNode, items: list[Any]) -> list[Any] | Parked:
        item_ty = normalize(node.job.input)

        async def drive(index: int, fresh: bool) -> Succeeded[Any] | Parked:
            child = str(uuid5(NAMESPACE_URL, json.dumps([identity, node.id, 1, index])))
            initial_row: dict[str, Any] | None = None
            created = False
            if fresh:
                async with self.child_admissions:
                    self.admitting()
                    encoded_item = encode(items[index], item_ty)
                    created = await self.runner._create(
                        node.job,
                        child,
                        encoded_item,
                        self.owner[0],
                        owner=self.owner,
                        parent=identity,
                        node=node.id,
                        index=index,
                    )
                    if created:
                        initial_row = {"id": child, "status": "running", "input": encoded_item}
            outcome = await self.job(node.job, child, initial_row=initial_row, fresh=created)
            if isinstance(outcome, Failed):
                raise await self.fail(identity, node.id, 1, f"child {child}, {outcome.node_id}: {outcome.reason}")
            return outcome

        limit = node.max_parallel or len(items)
        children = await self.drive_group(identity, len(items), min(self.runner.concurrency, limit), limit, drive)
        return children if isinstance(children, Parked) else [child.value for child in children]

    async def drive_group(
        self, identity: str, count: int, drives: int, limit: int, drive: Callable[[int, bool], Awaitable[T | Parked]]
    ) -> list[T] | Parked:
        """Drive at most `drives` children concurrently. `limit` includes sleeping children."""
        results: dict[int, T] = {}
        active: dict[asyncio.Future[T | Parked], int] = {}
        sleeping: list[tuple[float, int]] = []
        next_item = 0
        timer: asyncio.Task[None] | None = None
        try:
            while next_item < count or sleeping or active:
                self.admitting()
                while len(active) < drives:
                    if sleeping and sleeping[0][0] <= self.clock():
                        _, index = heapq.heappop(sleeping)
                        fresh = False
                    elif next_item < count and len(active) + len(sleeping) < limit:
                        index = next_item
                        next_item += 1
                        fresh = True
                    else:
                        break
                    active[asyncio.ensure_future(drive(index, fresh))] = index

                if not active:
                    parked = await self.wait(sleeping[0][0], identity)
                    if parked is not None:
                        return parked
                    continue

                pending: set[asyncio.Future[Any]] = set(active)
                if sleeping and len(active) < drives:
                    timer = asyncio.create_task(self.store.wait_until(sleeping[0][0]))
                    pending.add(timer)
                await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                if timer is not None:
                    if timer.done():
                        timer.result()
                    else:
                        timer.cancel()
                        await asyncio.gather(timer, return_exceptions=True)
                    timer = None
                for task in tuple(active):
                    if not task.done():
                        continue
                    index = active.pop(task)
                    outcome = task.result()
                    if isinstance(outcome, Parked):
                        heapq.heappush(sleeping, (outcome.due, index))
                    else:
                        results[index] = outcome
        finally:
            unfinished: list[asyncio.Future[Any]] = list(active)
            if timer is not None:
                unfinished.append(timer)
            for unfinished_call in unfinished:
                unfinished_call.cancel()
            await settle(asyncio.gather(*unfinished, return_exceptions=True))
        return [results[index] for index in range(count)]
