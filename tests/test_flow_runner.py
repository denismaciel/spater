"""Runner conformance through the same public API on Postgres and memory."""

from __future__ import annotations

import asyncio
import threading
import time
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast

import pytest
from spater import Context
from spater.flow import (
    Builder,
    Failed,
    FlowError,
    Job,
    LocalExecutor,
    PerStep,
    PerWorker,
    Ref,
    Retry,
    RunExistsError,
    Runner,
    StepCall,
    StepNext,
    Succeeded,
)
from spater.flow._runner import Parked
from spater.testing import advance_time


@pytest.fixture(params=["memory_url", "pg_url"], ids=["memory", "postgres"])
def url(request):
    return request.getfixturevalue(request.param)


@dataclass
class Batch:
    count: int


def expand(batch: Batch) -> list[str]:
    return ["x" * 200 for _ in range(batch.count)]


def summarize(values: list[str]) -> int:
    return sum(len(v) for v in values)


async def test_checked_pipeline_rerun_graph_and_full_values(url):
    @Job.define("pipeline")
    def job(f: Builder, batch: Ref[Batch]) -> Ref[int]:
        xs = f.step(expand, batch)
        return f.step(summarize, xs)

    async with Runner(url, preview_bytes=64) as r:
        with pytest.raises(ValueError):
            await r.run(job, cast(Any, 3))
        result = await r.run(job, Batch(5))
        assert isinstance(result, Succeeded) and result.value == 1000
        inspection = await r.inspect(result.run_id)
        assert inspection["status"] == "succeeded"
        rows = {a["node_id"]: a for a in inspection["attempts"]}
        assert rows["expand"]["preview"]["truncated"]
        assert all("output" not in a for a in rows.values())
        assert await r.graph(result.run_id) == job.graph()

        @Job.define("pipeline")
        def changed(f: Builder, batch: Ref[Batch]) -> None:
            f.step(expand, batch)

        rerun = await r.rerun(changed, result.run_id)
        assert isinstance(rerun, Succeeded) and rerun.run_id != result.run_id
        assert (await r.inspect(rerun.run_id))["rerun_of"] == result.run_id
        assert await r.graph(result.run_id) == job.graph()
        with pytest.raises(ValueError, match="definition changed"):
            await r.deliver(changed, result.run_id)


def key(n: int) -> str:
    return str(n)


def integers(batch: Batch) -> list[int]:
    return list(range(batch.count))


@dataclass
class Identity:
    key: str


def make_identity(ctx: Context) -> Identity:
    return Identity(ctx.idempotency_key)


@dataclass
class Denied:
    reason: str


@dataclass
class Lease:
    generation: int


provider_events: list[str] = []
calls: Counter[str] = Counter()


def serve() -> Iterator[Lease]:
    provider_events.append("open")
    try:
        yield Lease(1)
    finally:
        provider_events.append("close")


def attempt(identity: Identity, lease: Lease) -> int:
    calls[identity.key] += 1
    assert lease.generation == 1
    raise TimeoutError("unavailable")


def denied(error: TimeoutError) -> Denied:
    return Denied(str(error))


async def test_retry_exhaustion_on_caught_value_and_provider_lifetime(url):
    provider_events.clear()

    @Job.define("attempt", provides=[PerWorker(serve), PerStep(make_identity)])
    def job(f: Builder) -> Ref[int]:
        out = f.step(
            attempt,
            catch={TimeoutError: denied},
            retry=Retry(on=Denied, max=3, after=timedelta(microseconds=10)),
        )
        return f.branch(out, {int: lambda _, v: v, Denied: lambda b, _: b.fail("provider unavailable")})

    async with Runner(url) as r:
        result = await r.run(job)
        assert isinstance(result, Failed) and result.reason == "provider unavailable"
        assert calls[f"{result.run_id}/attempt"] == 3
        attempts = (await r.inspect(result.run_id))["attempts"]
        assert [a["status"] for a in attempts if a["node_id"] == "attempt"] == ["retried", "retried", "succeeded"]
        assert provider_events == ["open"]
    assert provider_events == ["open", "close"]


async def test_retry_directly_on_exception_with_and_without_catch(url):
    flaky_calls = 0

    def flaky(n: int) -> int:
        nonlocal flaky_calls
        flaky_calls += 1
        if flaky_calls < 3:
            raise TimeoutError("transient")
        return n * 2

    def always_timeout(n: int) -> int:
        raise TimeoutError("still down")

    @Job.define("retry_exc")
    def job_succeeds(f: Builder, n: Ref[int]) -> Ref[int]:
        return f.step(flaky, n, retry=Retry(on=TimeoutError, max=3, after=timedelta(microseconds=10)))

    @Job.define("retry_exc_then_catch")
    def job_catches(f: Builder, n: Ref[int]) -> Ref[int | Denied]:
        return f.step(
            always_timeout,
            n,
            retry=Retry(on=TimeoutError, max=2, after=timedelta(microseconds=10)),
            catch={TimeoutError: denied},
        )

    @Job.define("retry_exc_fails")
    def job_fails(f: Builder, n: Ref[int]) -> Ref[int]:
        return f.step(always_timeout, n, retry=Retry(on=TimeoutError, max=2, after=timedelta(microseconds=10)))

    async with Runner(url) as r:
        res1 = await r.run(job_succeeds, 21)
        assert isinstance(res1, Succeeded) and res1.value == 42
        a1 = (await r.inspect(res1.run_id))["attempts"]
        assert [a["status"] for a in a1] == ["retried", "retried", "succeeded"]

        res2 = await r.run(job_catches, 1)
        assert isinstance(res2, Succeeded) and res2.value == Denied("still down")
        a2 = (await r.inspect(res2.run_id))["attempts"]
        assert [a["status"] for a in a2] == ["retried", "succeeded"]

        res3 = await r.run(job_fails, 1)
        assert isinstance(res3, Failed) and "still down" in res3.reason
        a3 = (await r.inspect(res3.run_id))["attempts"]
        assert [a["status"] for a in a3] == ["retried", "failed"]


async def test_diamond_dag_runs_independent_branches_concurrently(url):
    both_started = asyncio.Event()
    in_flight = 0

    async def branch_a(x: int) -> int:
        nonlocal in_flight
        in_flight += 1
        if in_flight == 2:
            both_started.set()
        await asyncio.wait_for(both_started.wait(), 2)
        return x + 10

    async def branch_b(x: int) -> int:
        nonlocal in_flight
        in_flight += 1
        if in_flight == 2:
            both_started.set()
        await asyncio.wait_for(both_started.wait(), 2)
        return x * 10

    def join_ab(a: int, b: int) -> int:
        return a + b

    @Job.define("diamond")
    def diamond(f: Builder, x: Ref[int]) -> Ref[int]:
        a = f.step(branch_a, x)
        b = f.step(branch_b, x)
        return f.step(join_ab, a, b)

    async with Runner(url, concurrency=2) as r:
        res = await r.run(diamond, 3)
        assert isinstance(res, Succeeded) and res.value == 43


async def test_run_id_duplicate_policies_and_cancel(url):
    @Job.define("echo")
    def echo(f: Builder, n: Ref[int]) -> Ref[str]:
        return f.step(key, n)

    async with Runner(url) as r:
        first = await r.run(echo, 7, run_id="biz-7")
        assert isinstance(first, Succeeded) and first.run_id == "biz-7" and first.value == "7"

        with pytest.raises(RunExistsError):
            await r.run(echo, 7, run_id="biz-7")

        reused = await r.run(echo, 7, run_id="biz-7", on_duplicate="use_existing")
        assert reused == first

        # Cancel an admitted parked run by ID.
        @Job.define("sleepy")
        def sleepy(f: Builder, n: Ref[int]) -> Ref[str]:
            w = f.sleep(timedelta(days=1))
            return f.step(key, n, after=w)

        run_id = await r.prepare(sleepy, input=5, run_id="cancel-me")
        assert isinstance(await r.deliver(sleepy, run_id), Parked)
        cancelled = await r.cancel("cancel-me")
        assert cancelled["status"] == "cancelled"
        assert (await r.inspect("cancel-me"))["status"] == "cancelled"
        # Idempotent on terminal runs.
        assert (await r.cancel("cancel-me"))["status"] == "cancelled"


active = 0
peak = 0


async def left(n: int) -> str:
    global active, peak
    active += 1
    peak = max(active, peak)
    try:
        await asyncio.sleep(0.004 if n % 2 else 0.001)
        return str(n)
    finally:
        active -= 1


async def right(n: int) -> None:
    await left(n)


def items(batch: Batch) -> list[int]:
    return [n % 3 for n in range(batch.count)]


async def test_nested_map_shared_limit_and_order(url):
    global peak
    peak = 0

    @Job.define("child")
    def child(f: Builder, n: Ref[int]) -> Ref[str]:
        out = f.step(left, n)
        f.step(right, n)
        return out

    @Job.define("mapped")
    def parent(f: Builder, batch: Ref[Batch]) -> None:
        seq = f.step(items, batch)
        results = f.map(child, seq, max_parallel=3)
        f.step(summarize, results)

    async with Runner(url, concurrency=2) as r:
        result = await r.run(parent, Batch(5))
        assert isinstance(result, Succeeded)
        rows = (await r.inspect(result.run_id))["children"]
        assert [c["item_index"] for c in rows] == list(range(5))
        assert [c["result"]["value"] for c in rows] == ["0", "1", "2", "0", "1"]
        assert len({c["id"] for c in rows}) == 5
        assert 1 <= peak <= 2
        assert (await r.inspect(rows[0]["id"]))["status"] == "succeeded"


started = threading.Event()
release = threading.Event()
blocking_run = ""


def blocking(identity: Identity) -> int:
    global blocking_run
    blocking_run = identity.key.split("/")[0]
    started.set()
    if not release.wait(10):
        raise RuntimeError("test did not release worker")
    return 3


async def test_cancellation_drains_sync_call(url):
    started.clear()
    release.clear()

    @Job.define("blocking", provides=[PerStep(make_identity)])
    def job(f: Builder) -> Ref[int]:
        return f.step(blocking)

    async with Runner(url) as r:
        task = asyncio.create_task(r.run(job))
        try:
            assert await asyncio.to_thread(started.wait, 4)
            task.cancel()
            while (await r.inspect(blocking_run))["status"] != "cancelling":
                await asyncio.sleep(0.005)
            task.cancel()
            done, _ = await asyncio.wait((task,), timeout=0.05)
            assert not done
            assert (await r.inspect(blocking_run))["status"] == "cancelling"
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        history = await r.inspect(blocking_run)
        assert history["status"] == "cancelled"
        assert [a["status"] for a in history["attempts"]] == ["cancelled"]


async def test_repeated_cancellation_during_admission_closes_run(url, monkeypatch):
    admitted = asyncio.get_running_loop().create_future()
    release_claim = asyncio.Event()

    @Job.define("admission")
    def job(f: Builder, n: Ref[int]) -> Ref[str]:
        return f.step(key, n)

    async with Runner(url) as runner:
        create_and_claim = runner.store.create_and_claim

        async def hold_claim(run, *args, **kwargs):
            generation = await create_and_claim(run, *args, **kwargs)
            admitted.set_result(run["id"])
            await release_claim.wait()
            return generation

        monkeypatch.setattr(runner.store, "create_and_claim", hold_claim)
        work = asyncio.create_task(runner.run(job, 1))
        identity = await asyncio.wait_for(admitted, 4)
        try:
            work.cancel()
            await asyncio.sleep(0)
            work.cancel()
            done, _ = await asyncio.wait((work,), timeout=0.05)
            assert not done
        finally:
            release_claim.set()
        with pytest.raises(asyncio.CancelledError):
            await work
        history = await runner.inspect(identity)
        assert history["status"] == "cancelled"
        assert history["attempts"] == []


count_manifests = 0


def counted_items(batch: Batch) -> list[int]:
    global count_manifests
    count_manifests += 1
    return items(batch)


def wake(n: int) -> timedelta:
    return timedelta(seconds=(n + 1) * 60)


async def test_parked_map_restores_manifest_and_preserves_admission(memory_url):
    global count_manifests
    count_manifests = 0

    @Job.define("child")
    def child(f: Builder, n: Ref[int]) -> Ref[str]:
        w = f.sleep(wake, n)
        return f.step(left, n, after=w)

    @Job.define("parent")
    def parent(f: Builder, batch: Ref[Batch]) -> None:
        seq = f.step(counted_items, batch)
        f.map(child, seq, max_parallel=2)

    async with Runner(memory_url, concurrency=1) as r:
        run_id = await r.prepare(parent, input=Batch(4))
        assert isinstance(await r.deliver(parent, run_id), Parked)
        assert len((await r.inspect(run_id))["children"]) == 2
    async with Runner(memory_url) as r:
        for _ in range(4):
            advance_time(memory_url, 62)
            result = await r.deliver(parent, run_id)
            if isinstance(result, Succeeded):
                break
        assert isinstance(result, Succeeded)
        assert count_manifests == 1
        assert len((await r.inspect(run_id))["children"]) == 4


async def abort_when_sibling_starts() -> None:
    assert await asyncio.to_thread(started.wait, 4)
    raise LookupError("no carrier")


@pytest.mark.parametrize("cancel_during_drain", [False, True])
async def test_parallel_failure_visible_before_sync_sibling_settles(url, cancel_during_drain, monkeypatch):
    started.clear()
    release.clear()

    @Job.define("drain", provides=[PerStep(make_identity)])
    def job(f: Builder) -> None:
        val = f.step(blocking)
        f.step(key, val)
        f.step(abort_when_sibling_starts)

    async with Runner(url) as runner:
        cancellation_started = asyncio.Event()
        update = runner.store.update

        async def observe(identity, fields, owner):
            await update(identity, fields, owner)
            if fields.get("status") == "cancelling":
                cancellation_started.set()

        monkeypatch.setattr(runner.store, "update", observe)
        work = asyncio.create_task(runner.run(job))
        try:
            assert await asyncio.to_thread(started.wait, 4)

            async def wait_for_error():
                while True:
                    snapshot = await runner.inspect(blocking_run)
                    if any(a["status"] == "failed" for a in snapshot["attempts"]):
                        return snapshot
                    await asyncio.sleep(0.005)

            snapshot = await asyncio.wait_for(wait_for_error(), 4)
            assert snapshot["status"] == "running"
            if cancel_during_drain:
                work.cancel()
                await asyncio.wait_for(cancellation_started.wait(), 4)
                work.cancel()
                done, _ = await asyncio.wait((work,), timeout=0.05)
                assert not done
            assert not work.done()
        finally:
            release.set()
        if cancel_during_drain:
            with pytest.raises(asyncio.CancelledError):
                await work
        else:
            result = await work
            assert isinstance(result, Failed) and result.node_id == "abort_when_sibling_starts"
        history = await runner.inspect(blocking_run)
        assert history["status"] == "failed" and "no carrier" in history["error"]
        rows = history["attempts"]
        assert all(a["node_id"] != "key" for a in rows)


async def swallow_timeout() -> int:
    try:
        await asyncio.sleep(60)
    except asyncio.CancelledError:
        return 10
    return 3


async def test_late_result_never_advances_after_timeout(url):
    @Job.define("deadline")
    def job(f: Builder) -> Ref[str]:
        val = f.step(swallow_timeout, timeout=timedelta(milliseconds=10))
        return f.step(key, val)

    async with Runner(url) as runner:
        result = await runner.run(job)
        assert isinstance(result, Failed) and result.node_id == "swallow_timeout"
        assert "Timeout" in result.reason
        assert [a["node_id"] for a in (await runner.inspect(result.run_id))["attempts"]] == ["swallow_timeout"]


async def test_cancellation_while_timed_out_call_is_draining(url, monkeypatch):
    @Job.define("draining")
    def job(f: Builder, n: Ref[int]) -> Ref[str]:
        return f.step(key, n, timeout=timedelta(milliseconds=150))

    draining = asyncio.get_running_loop().create_future()
    release_cleanup = asyncio.Event()
    async with Runner(url) as runner:

        async def hold_cleanup(fn, job, bound, env, context, *, allow_providers):
            try:
                await asyncio.Event().wait()
            finally:
                draining.set_result(context.run_id)
                await release_cleanup.wait()

        assert isinstance(runner.executor, LocalExecutor)
        monkeypatch.setattr(runner.executor._providers, "_call", hold_cleanup)
        work = asyncio.create_task(runner.run(job, 1))
        try:
            run_id = await asyncio.wait_for(draining, 4)
            work.cancel()
            done, _ = await asyncio.wait((work,), timeout=0.05)
            assert not done
        finally:
            release_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await work
        assert (await runner.inspect(run_id))["status"] == "cancelled"


def recover_key(identity: Identity) -> str:
    return identity.key


@pytest.mark.parametrize("remaining", [-60, 60], ids=["expired", "live"])
async def test_redelivery_uses_original_step_deadline(url, remaining, monkeypatch):
    @Job.define("timed", provides=[PerStep(make_identity)])
    def job(f: Builder) -> Ref[str]:
        return f.step(recover_key, timeout=timedelta(minutes=5))

    async with Runner(url) as runner:
        run_id = await runner.prepare(job, input=None)
        generation = await runner.store.claim(run_id)
        deadline = time.time() + remaining
        await runner.store.put_attempt(
            run_id, "recover_key", 1, {"status": "running", "deadline": deadline}, (run_id, generation)
        )
        observed_calls = []
        assert isinstance(runner.executor, LocalExecutor)
        original = runner.executor._providers._call

        def observe(fn, *args, **kwargs):
            observed_calls.append(fn)
            return original(fn, *args, **kwargs)

        monkeypatch.setattr(runner.executor._providers, "_call", observe)
        result = await runner.deliver(job, run_id)
        if remaining < 0:
            assert isinstance(result, Failed) and "TimeoutError" in result.reason
            assert observed_calls == []
        else:
            assert isinstance(result, Succeeded) and result.value == f"{run_id}/recover_key"
            assert observed_calls == [recover_key]
        (attempt_row,) = (await runner.inspect(run_id))["attempts"]
        assert (attempt_row["attempt"], attempt_row["deadline"]) == (1, deadline)


async def inner_cancelled() -> int:
    fut = asyncio.get_running_loop().create_future()
    fut.cancel()
    return await fut


async def test_application_cancelled_awaitable_fails_the_node(url):
    @Job.define("inner_cancel")
    def job(f: Builder) -> Ref[int]:
        return f.step(inner_cancelled)

    async with Runner(url) as r:
        run_id = await r.prepare(job, input=None)
        result = await r.deliver(job, run_id)
        assert isinstance(result, Failed)
        assert result.node_id == "inner_cancelled"
        assert (await r.inspect(run_id))["status"] == "failed"


async def test_unlimited_map_releases_root_slots_for_sleeping_children(memory_url, monkeypatch):
    @Job.define("child")
    def child(f: Builder, n: Ref[int]) -> None:
        f.sleep(timedelta(days=1))

    @Job.define("root")
    def root(f: Builder, batch: Ref[Batch]) -> None:
        seq = f.step(integers, batch)
        f.map(child, seq)

    async with Runner(memory_url, concurrency=1) as r:
        original_create = r.store.create
        ready = asyncio.Event()
        children = []

        async def track_create(row, *args, **kwargs):
            created = await original_create(row, *args, **kwargs)
            if row.get("parent_id"):
                children.append(row["id"])
                if len(children) == 3:
                    ready.set()
            return created

        monkeypatch.setattr(r.store, "create", track_create)
        work = asyncio.create_task(r.run(root, Batch(3)))
        try:
            await asyncio.wait_for(ready.wait(), timeout=2)
            for identity in children:
                while (await r.store.attempt(identity, "sleep", 1) or {}).get("due") is None:
                    await asyncio.sleep(0)
            advance_time(memory_url, timedelta(days=1, seconds=1))
            assert isinstance(await asyncio.wait_for(work, 2), Succeeded)
        finally:
            work.cancel()
            await asyncio.gather(work, return_exceptions=True)


async def test_custom_step_executor_receives_meta_and_closes(url):
    events: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    closed = False

    class RoutingExecutor:
        def __init__(self) -> None:
            self.local = LocalExecutor()

        async def execute(self, call: StepCall) -> Any:
            events.append((call.node_id, dict(call.meta), call.inputs()))
            if call.meta.get("route") == "remote":
                return f"remote:{call.inputs()['n']}"
            return await self.local.execute(call)

        async def aclose(self) -> None:
            nonlocal closed
            closed = True
            await self.local.aclose()

    def double(n: int) -> int:
        return n * 2

    def render(n: int) -> str:
        return f"local:{n}"

    @Job.define("routed")
    def job(f: Builder, n: Ref[int]) -> Ref[str]:
        doubled = f.step(double, n, meta={"tier": "fast"})
        return f.step(render, doubled, meta={"route": "remote", "memory": "4Gi"})

    async with Runner(url, executor=RoutingExecutor()) as r:
        result = await r.run(job, 5)
        assert isinstance(result, Succeeded) and result.value == "remote:10"
        assert events == [
            ("double", {"tier": "fast"}, {"n": 5}),
            ("render", {"route": "remote", "memory": "4Gi"}, {"n": 10}),
        ]
    assert closed


async def test_step_middleware_ordering_context_and_on_error(url):
    import contextvars

    trace_var: contextvars.ContextVar[str] = contextvars.ContextVar("trace", default="")
    log_events: list[str] = []
    errors: list[tuple[str, int, str]] = []
    attempts_seen = 0

    def flaky(n: int) -> str:
        nonlocal attempts_seen
        attempts_seen += 1
        if attempts_seen == 1:
            raise TimeoutError("transient")
        if n < 0:
            raise RuntimeError("boom")
        return f"{trace_var.get()}:{n}"

    @Job.define("observed")
    def job(f: Builder, n: Ref[int]) -> Ref[str]:
        return f.step(
            flaky,
            n,
            retry=Retry(on=TimeoutError, max=2, after=timedelta(microseconds=10)),
            meta={"pool": "llm"},
        )

    async def outer(call: StepCall, call_next: StepNext) -> Any:
        log_events.append(f"outer:before:{call.node_id}:{call.attempt}:{call.meta.get('pool')}")
        token = trace_var.set(f"trace-{call.attempt}")
        try:
            return await call_next(call)
        finally:
            trace_var.reset(token)
            log_events.append(f"outer:after:{call.node_id}:{call.attempt}")

    async def inner(call: StepCall, call_next: StepNext) -> Any:
        log_events.append(f"inner:{call.attempt}")
        return await call_next(call)

    async def async_error_hook(call: StepCall, exc: BaseException) -> None:
        errors.append((call.node_id, call.attempt, str(exc)))

    def broken_hook(call: StepCall, exc: BaseException) -> None:
        raise ValueError("hook bug must not mask step failure")

    async with Runner(url, middleware=[outer]) as r:
        r.middleware(inner)
        r.on_error(async_error_hook)
        r.on_error(broken_hook)

        ok = await r.run(job, 7)
        assert isinstance(ok, Succeeded) and ok.value == "trace-2:7"
        assert errors == []  # retried attempt did not fire terminal on_error
        assert log_events == [
            "outer:before:flaky:1:llm",
            "inner:1",
            "outer:after:flaky:1",
            "outer:before:flaky:2:llm",
            "inner:2",
            "outer:after:flaky:2",
        ]

        attempts_seen = 1  # skip TimeoutError, trigger RuntimeError directly
        bad = await r.run(job, -1)
        assert isinstance(bad, Failed) and "boom" in bad.reason
        assert errors == [("flaky", 1, "boom")]


def test_step_meta_validation_and_version_sensitivity():
    def inc(n: int) -> int:
        return n + 1

    @Job.define("m1")
    def j1(f: Builder, n: Ref[int]) -> Ref[int]:
        return f.step(inc, n, meta={"queue": "default"})

    @Job.define("m2")
    def j2(f: Builder, n: Ref[int]) -> Ref[int]:
        return f.step(inc, n, meta={"queue": "libreoffice"})

    assert j1.to_dict()["scope"]["nodes"][0]["meta"] == {"queue": "default"}
    assert j1.version != j2.version

    with pytest.raises(FlowError) as exc:

        @Job.define("bad_meta")
        def _(f: Builder, n: Ref[int]) -> Ref[int]:
            return f.step(inc, n, meta={"bad": object()})

    assert exc.value.codes == {"bad-option"}


async def test_aclose_detaches_flow_queue_hooks_and_clears_runner_inspector(memory_url):
    from spater.app import Asynq
    from spater.flow import FlowQueue

    @Job.define("noop")
    def noop(f: Builder, n: Ref[int]) -> Ref[str]:
        return f.step(key, n)

    app = Asynq(memory_url)
    queue = FlowQueue(app, [noop])
    assert queue._reconcile in app._worker_maintenance
    assert "spater.flow.deliver" in app.tasks
    queue.runner._inspector = app.inspector
    await queue.aclose()
    assert queue._reconcile not in app._worker_maintenance
    assert "spater.flow.deliver" not in app.tasks
    assert queue.runner._inspector is None
