"""DAG deliveries through real Asynq workers and their delayed/recovery queues."""

from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from conftest import FAST, wait_for
from spater import Asynq, Context, HandlerNotFound, Retry, TaskContext
from spater._core import Broker
from spater.flow import Builder, FlowQueue, Job, PerStep, Ref, RunExistsError
from spater.testing import advance_time


@dataclass
class Request:
    seconds: int


@dataclass
class Run:
    id: str
    key: str


def run_info(ctx: Context) -> Run:
    return Run(ctx.run_id, ctx.idempotency_key)


seen: list[tuple[str, str]] = []


def begin(run: Run) -> None:
    seen.append((run.id, "begin"))


def delay(request: Request) -> timedelta:
    return timedelta(seconds=request.seconds)


def finish(run: Run) -> str:
    seen.append((run.id, "finish"))
    return run.id


@Job.define("queued_timer", provides=[PerStep(run_info)])
def TIMER(f: Builder, req: Ref[Request]) -> Ref[str]:
    started = f.step(begin)
    waited = f.sleep(delay, req, after=started)
    return f.step(finish, after=waited)


@pytest.fixture(params=["memory_url", "pg_url"], ids=["memory", "postgres"])
def flow_url(request):
    return request.getfixturevalue(request.param)


async def test_timer_releases_worker_and_resumes_in_restarted_worker(flow_url, run_app):
    app = Asynq(flow_url)
    queue = FlowQueue(app, [TIMER])
    is_memory = flow_url.startswith("memory://")
    seconds = 86400 if is_memory else 2
    if is_memory:
        advance_time(app, 600)
    run_id = await queue.enqueue(TIMER, Request(seconds))
    try:
        async with run_app(app, concurrency=1) as worker:
            await wait_for(lambda: app.inspector.list_scheduled_tasks("default") and worker.active_count == 0)
            assert [phase for observed, phase in seen if observed == run_id] == ["begin"]
    finally:
        await queue.aclose()

    if is_memory:
        advance_time(app, seconds + 1)
    replacement = Asynq(flow_url)
    resumed = FlowQueue(replacement, [TIMER])
    try:
        async with run_app(replacement, concurrency=1):
            await wait_for(lambda: (run_id, "finish") in seen, timeout=6)
        assert [phase for observed, phase in seen if observed == run_id] == ["begin", "finish"]
    finally:
        await resumed.aclose()


async def test_early_wakeup_honors_the_original_due_time(memory_url, run_app):
    app = Asynq(memory_url)
    queue = FlowQueue(app, [TIMER])
    run_id = await queue.enqueue(TIMER, Request(3600))
    try:
        async with run_app(app):
            [original] = await wait_for(lambda: app.inspector.list_scheduled_tasks("default"))
            advance_time(app, 1800)
            app.inspector.run_task("default", original.id)
            [next_wakeup] = await wait_for(lambda: app.inspector.list_scheduled_tasks("default"))
            assert next_wakeup.id != original.id
            assert abs((next_wakeup.next_process_at - original.next_process_at).total_seconds()) <= 1
            assert (run_id, "finish") not in seen
            advance_time(app, 1801)
            await wait_for(lambda: (run_id, "finish") in seen)
    finally:
        await queue.aclose()


async def test_enqueue_duplicate_policies_and_cancel(flow_url, run_app):
    app = Asynq(flow_url)
    queue = FlowQueue(app, [TIMER])
    try:
        first = await queue.enqueue(TIMER, Request(3600), run_id="timer-dup")
        assert first == "timer-dup"
        with pytest.raises(RunExistsError):
            await queue.enqueue(TIMER, Request(3600), run_id="timer-dup")
        assert await queue.enqueue(TIMER, Request(3600), run_id="timer-dup", on_duplicate="use_existing") == "timer-dup"

        async with run_app(app):
            await wait_for(lambda: app.inspector.list_scheduled_tasks("default"))
            await queue.cancel("timer-dup")
            assert (await queue.runner.inspect("timer-dup"))["status"] == "cancelled"
            assert app.inspector.list_scheduled_tasks("default") == []
    finally:
        await queue.aclose()


async def test_expired_initial_claim_and_lost_ack_reuse_run_and_continuation(flow_url, run_app):
    app = Asynq(flow_url, retry_delay=lambda *_: 0)
    queue = FlowQueue(app, [TIMER])
    is_memory = flow_url.startswith("memory://")
    seconds = 86400 if is_memory else 3
    run_id = await queue.enqueue(TIMER, Request(seconds))

    claimed = Broker(flow_url).dequeue("default", lease=0.1)
    assert claimed and claimed[0].id == run_id
    resumed_ids: list[str] = []

    @app.middleware
    async def lose_first_ack(ctx: TaskContext, call_next: Any) -> Any:
        result = await call_next(ctx)
        if ctx.id == run_id:
            [scheduled] = app.inspector.list_scheduled_tasks("default")
            resumed_ids.append(scheduled.id)
            if len(resumed_ids) == 1:
                raise Retry("ack lost", after=0)
        return result

    try:
        async with run_app(app, concurrency=1, recoverer_interval=0.05, recoverer_skew=0):
            await wait_for(lambda: len(resumed_ids) == 2)
            assert resumed_ids[0] == resumed_ids[1]
            assert resumed_ids[0] != run_id
            assert [phase for observed, phase in seen if observed == run_id] == ["begin"]
            if is_memory:
                advance_time(app, seconds + 1)
            await wait_for(lambda: (run_id, "finish") in seen, timeout=6)
        assert [phase for observed, phase in seen if observed == run_id] == ["begin", "finish"]
    finally:
        await queue.aclose()


release_effect: dict[str, asyncio.Event] = {}


async def hold_effect(run: Run) -> str:
    seen.append((run.id, "effect"))
    await release_effect[run.id].wait()
    return run.id


async def test_stale_wakes_cannot_take_over_an_active_successor(flow_url, run_app):
    @Job.define("stale_wake", provides=[PerStep(run_info)])
    def job(f: Builder, req: Ref[Request]) -> Ref[str]:
        waited = f.sleep(delay, req)
        return f.step(hold_effect, after=waited)

    app = Asynq(flow_url)
    queue = FlowQueue(app, [job])
    is_memory = flow_url.startswith("memory://")
    run_id = await queue.enqueue(job, Request(3600 if is_memory else 2))
    first = app.inspector.get_task_info("default", run_id)
    assert first.timeout == timedelta(days=1) and first.max_retry == 3
    release = release_effect[run_id] = asyncio.Event()
    acknowledgements: list[str] = []

    @app.middleware
    async def observe_ack(ctx: TaskContext, call_next: Any) -> Any:
        result = await call_next(ctx)
        acknowledgements.append(ctx.id)
        return result

    try:
        async with run_app(app, concurrency=3):
            try:
                [second] = await wait_for(lambda: app.inspector.list_scheduled_tasks("default"))
                app.inspector.run_task("default", second.id)
                [third] = await wait_for(lambda: app.inspector.list_scheduled_tasks("default"))
                assert third.id != second.id
                if is_memory:
                    advance_time(app, 3601)
                await wait_for(lambda: (run_id, "effect") in seen and app.inspector.get_queue_info("default").size == 1)
                for replay in (first, second):
                    await app.client.aenqueue(replay.type, replay.payload, task_id=replay.id)
                await wait_for(
                    lambda: (
                        all(acknowledgements.count(old.id) == 2 for old in (first, second))
                        or seen.count((run_id, "effect")) > 1
                    )
                )
                assert seen.count((run_id, "effect")) == 1
                assert third.id not in acknowledgements
                assert (await queue.runner.inspect(run_id))["status"] == "running"
            finally:
                release.set()
            await wait_for(lambda: third.id in acknowledgements)
        assert (await queue.runner.inspect(run_id))["status"] == "succeeded"
    finally:
        release_effect.pop(run_id, None)
        await queue.aclose()


async def test_retry_after_wake_publish_fails_recovers_the_timer(memory_url, run_app):
    app = Asynq(memory_url, retry_delay=lambda *_: 0)
    queue = FlowQueue(app, [TIMER])
    run_id = await queue.enqueue(TIMER, Request(3600))
    publishes = 0

    @app.before_enqueue
    def drop_first_publish(name: str, headers: dict[str, str]) -> None:
        nonlocal publishes
        publishes += 1
        if publishes == 1:
            raise ConnectionError("failed before publishing the wake")

    try:
        async with run_app(app):
            await wait_for(lambda: publishes == 2 and app.inspector.list_scheduled_tasks("default"))
            assert [phase for observed, phase in seen if observed == run_id] == ["begin"]
            advance_time(app, 3601)
            await wait_for(lambda: (run_id, "finish") in seen)
        assert (await queue.runner.inspect(run_id))["status"] == "succeeded"
    finally:
        await queue.aclose()


async def test_delivery_rejects_worker_with_incompatible_job_version(memory_url, run_app):
    producer = Asynq(memory_url)
    original = FlowQueue(producer, [TIMER])
    run_id = await original.enqueue(TIMER, Request(60))
    worker_app = Asynq(memory_url)

    @Job.define(TIMER.name)
    def changed(f: Builder, req: Ref[Request]) -> None:
        f.sleep(timedelta(seconds=1))

    destination = FlowQueue(worker_app, [changed])
    failures = []

    @worker_app.on_error
    def failed(ctx: TaskContext, exc: BaseException) -> None:
        failures.append((ctx.id, exc))

    try:
        async with run_app(worker_app):
            await wait_for(lambda: failures)
        assert failures[0][0] == run_id
        assert isinstance(failures[0][1], HandlerNotFound)
        assert TIMER.version in str(failures[0][1])
        assert all(observed != run_id for observed, _ in seen)
    finally:
        await destination.aclose()
        await original.aclose()


inflight_keys: dict[str, list[str]] = {}


async def interrupt_once(run: Run) -> str:
    keys = inflight_keys.setdefault(run.id, [])
    keys.append(run.key)
    if len(keys) == 1:
        await asyncio.Event().wait()
    return run.id


async def test_shutdown_mid_step_replays_only_the_uncommitted_node(flow_url, run_app):
    @Job.define("interrupted", provides=[PerStep(run_info)])
    def job(f: Builder) -> Ref[str]:
        started = f.step(begin)
        return f.step(interrupt_once, after=started)

    original_app = Asynq(flow_url)
    original = FlowQueue(original_app, [job])
    run_id = await original.enqueue(job)
    try:
        async with run_app(original_app, shutdown_timeout=0.1):
            await wait_for(lambda: run_id in inflight_keys)
    finally:
        await original.aclose()

    next_app = Asynq(flow_url)
    next_queue = FlowQueue(next_app, [job])
    try:
        async with run_app(next_app):
            await wait_for(lambda: len(inflight_keys[run_id]) == 2)
        keys = inflight_keys.pop(run_id)
        assert keys[0] == keys[1]
        assert [phase for observed, phase in seen if observed == run_id] == ["begin"]
    finally:
        await next_queue.aclose()


@dataclass
class CrashPoint:
    directory: str
    boundary: str


def committed_step(probe: CrashPoint) -> None:
    with (Path(probe.directory) / "effects").open("a") as stream:
        stream.write("committed\n")


async def external_write(probe: CrashPoint, run: Run) -> str:
    path = Path(probe.directory)
    with (path / "effects").open("a") as stream:
        stream.write(f"write {run.key}\n")
    if probe.boundary == "in_step" and not (path / "killed").exists():
        (path / "ready").touch()
        await asyncio.Event().wait()
    return run.id


@Job.define("crash", provides=[PerStep(run_info)])
def CRASH_JOB(f: Builder, probe: Ref[CrashPoint]) -> Ref[str]:
    committed = f.step(committed_step, probe)
    return f.step(external_write, probe, after=committed)


async def _serve_crash_worker(url: str, directory: str) -> None:
    app = Asynq(url)
    queue = FlowQueue(app, [CRASH_JOB])

    @app.middleware
    async def before_ack(ctx: TaskContext, call_next: Any) -> Any:
        result = await call_next(ctx)
        (Path(directory) / "ready").touch()
        await asyncio.Event().wait()
        return result

    try:
        await app.arun_worker(**{**FAST, "lease_duration": 1.0})
    finally:
        await queue.aclose()


@pytest.mark.parametrize("boundary", ["in_step", "after_commit"])
async def test_postgres_worker_process_kill_on_either_side_of_commit(pg_url, run_app, tmp_path, boundary):
    app = Asynq(pg_url, retry_delay=lambda *_: 0)
    queue = FlowQueue(app, [CRASH_JOB])
    run_id = await queue.enqueue(CRASH_JOB, CrashPoint(str(tmp_path), boundary))
    script = (
        "import asyncio, sys; sys.path.insert(0, sys.argv[1]); "
        "from test_flow_queue import _serve_crash_worker; "
        "asyncio.run(_serve_crash_worker(sys.argv[2], sys.argv[3]))"
    )
    child = await asyncio.create_subprocess_exec(
        sys.executable, "-c", script, str(Path(__file__).parent), pg_url, str(tmp_path)
    )
    try:
        await wait_for(lambda: (tmp_path / "ready").exists() or child.returncode is not None, timeout=15)
        assert child.returncode is None
    finally:
        if child.returncode is None:
            child.kill()
        await child.wait()
    (tmp_path / "killed").touch()

    try:
        async with run_app(app, recoverer_interval=0.05, recoverer_skew=0):
            await wait_for(lambda: app.inspector.get_queue_info("default").size == 0, timeout=10)
        row = await queue.runner.inspect(run_id)
        assert row["status"] == "succeeded"
        assert {attempt["node_id"] for attempt in row["attempts"]} == {"committed_step", "external_write"}
        effects = (tmp_path / "effects").read_text().splitlines()
        assert effects[0] == "committed"
        assert len(effects) == (3 if boundary == "in_step" else 2)
        assert effects[1].startswith("write ") and all(line == effects[1] for line in effects[1:])
    finally:
        await queue.aclose()


async def test_enqueue_validates_input_before_publishing(memory_url):
    app = Asynq(memory_url)
    queue = FlowQueue(app, [TIMER])
    try:
        with pytest.raises(ValueError):
            await queue.enqueue(TIMER, cast(Any, {"seconds": 1}))
        assert app.inspector.queues() == []
    finally:
        await queue.aclose()


def requests(request: Request) -> list[Request]:
    return [request]


async def never_complete(run: Run) -> str:
    seen.append((run.id, "blocked"))
    await asyncio.Event().wait()
    return run.id


async def wait_terminal(store, run_id):
    for _ in range(200):
        try:
            row = await store.get(run_id)
        except KeyError:
            row = {}
        if row.get("status") in ("failed", "succeeded", "cancelled"):
            return row
        await asyncio.sleep(0.03)
    pytest.fail("worker did not reconcile archived delivery")


async def test_deadline_archival_terminalizes_mapped_tree(flow_url, run_app):
    @Job.define("blocked_child", provides=[PerStep(run_info)])
    def child(f: Builder, req: Ref[Request]) -> Ref[str]:
        return f.step(never_complete)

    @Job.define("blocked_parent")
    def parent(f: Builder, req: Ref[Request]) -> None:
        reqs = f.step(requests, req)
        f.map(child, reqs)

    app = Asynq(flow_url, retry_delay=lambda *_: 0)
    queue = FlowQueue(app, [parent], delivery_timeout=2, delivery_max_retry=1)
    run_id = await queue.enqueue(parent, Request(0))
    try:
        async with run_app(app, health_check_interval=0.05):
            row = await wait_terminal(queue.runner.store, run_id)
        assert row["status"] == "failed"
        assert row["node_id"] == "$delivery" and row["failure_kind"] == "delivery"
        assert "deadline exceeded" in row["error"]
        assert run_id in row["error"]
        archived = app.inspector.get_task_info("default", run_id)
        assert archived.state == "archived" and archived.retried == 1
        parent_row = await queue.runner.inspect(run_id)
        map_attempts = [a for a in parent_row["attempts"] if a["node_id"] == "map_blocked_child"]
        assert [a["status"] for a in map_attempts] == ["cancelled"]
        [child_row] = parent_row["children"]
        child_row = await queue.runner.inspect(child_row["id"])
        assert child_row["status"] == "cancelled"
        assert [a["status"] for a in child_row["attempts"]] == ["cancelled"]
        assert seen.count((child_row["id"], "blocked")) == 2
    finally:
        await queue.aclose()


async def test_lease_recovery_archives_unhandled_wake(flow_url, run_app):
    app = Asynq(flow_url)
    queue = FlowQueue(app, [TIMER], delivery_max_retry=0)
    run_id = await queue.enqueue(TIMER, Request(300))
    try:
        async with run_app(app):
            [wake] = await wait_for(lambda: app.inspector.list_scheduled_tasks("default"))
        app.inspector.run_task("default", wake.id)
        claim = Broker(flow_url).dequeue("default", lease=0.1)
        assert claim and claim[0].id == wake.id
        async with run_app(app, recoverer_interval=0.05, recoverer_skew=0):
            await wait_for(lambda: app.inspector.get_task_info("default", wake.id).state == "archived")
        row = await queue.runner.inspect(run_id)
        assert row["status"] == "failed" and "lease expired" in row["error"]
        assert wake.id in row["error"]
        assert (run_id, "finish") not in seen
    finally:
        await queue.aclose()


@pytest.mark.parametrize("published", [False, True])
async def test_exhausted_publisher_only_fails_unpublished_wake(flow_url, run_app, published):
    producer = Asynq(flow_url)
    queue = FlowQueue(producer, [TIMER], delivery_timeout=53, delivery_max_retry=0)
    run_id = await queue.enqueue(TIMER, Request(300))
    app = Asynq(flow_url)
    destination = FlowQueue(app, [TIMER])
    if published:

        @app.middleware
        async def lose_ack(ctx: TaskContext, call_next: Any) -> Any:
            await call_next(ctx)
            raise RuntimeError("lost ack")
    else:

        @app.before_enqueue
        def lose_wake(name: str, headers: dict[str, str]) -> None:
            raise RuntimeError("lost publish")

    try:
        async with run_app(app, health_check_interval=0.05):
            await wait_for(lambda: app.inspector.get_task_info("default", run_id).state == "archived")
            if published:
                [wake] = app.inspector.list_scheduled_tasks("default")
                assert wake.timeout == timedelta(seconds=53) and wake.max_retry == 0
                assert (await destination.runner.inspect(run_id))["status"] == "running"
            else:
                row = await wait_terminal(destination.runner.store, run_id)
                assert row["status"] == "failed" and "lost publish" in row["error"]
    finally:
        await destination.aclose()
        await queue.aclose()


async def test_deleted_wake_reports_missing_delivery(flow_url, run_app):
    app = Asynq(flow_url)
    queue = FlowQueue(app, [TIMER])
    run_id = await queue.enqueue(TIMER, Request(300))
    try:
        async with run_app(app):
            [wake] = await wait_for(lambda: app.inspector.list_scheduled_tasks("default"))
        app.inspector.delete_task("default", wake.id)
        row = await queue.runner.inspect(run_id)
        assert row["status"] == "failed"
        assert wake.id in row["error"] and "missing" in row["error"]
    finally:
        await queue.aclose()


async def test_publisher_ack_between_inspector_reads_preserves_wake(flow_url, run_app, monkeypatch):
    from spater import TaskNotFoundError
    from spater.inspector import Inspector

    app = Asynq(flow_url)
    queue = FlowQueue(app, [TIMER])
    run_id = await queue.enqueue(TIMER, Request(300))
    publishing, release = asyncio.Event(), asyncio.Event()
    enqueue = queue._task.aenqueue

    async def hold_publish(*args, **kwargs):
        publishing.set()
        await release.wait()
        return await enqueue(*args, **kwargs)

    monkeypatch.setattr(queue._task, "aenqueue", hold_publish)
    try:
        async with run_app(app):
            try:
                await asyncio.wait_for(publishing.wait(), 5)
                assert (await queue.runner.inspect(run_id))["status"] == "running"
                inspect_task = Inspector.aget_task_info

                async def publish_after_missing(self, name, task_id):
                    try:
                        return await inspect_task(self, name, task_id)
                    except TaskNotFoundError:
                        if not release.is_set():
                            release.set()
                            await wait_for(lambda: app.inspector.get_queue_info("default").active == 0)
                        raise

                monkeypatch.setattr(Inspector, "aget_task_info", publish_after_missing)
                row = await queue.runner.inspect(run_id)
                assert release.is_set() and row["status"] == "running"
                [wake] = app.inspector.list_scheduled_tasks("default")
                assert wake.id == row["delivery"]["expected"]
            finally:
                release.set()
    finally:
        await queue.aclose()


async def test_archive_observer_cannot_fail_replacement_driver(flow_url, run_app):
    from spater.flow._run_store import ObsoleteDelivery
    from spater.flow._runner import Parked

    app = Asynq(flow_url)
    queue = FlowQueue(app, [TIMER])
    run_id = await queue.enqueue(TIMER, Request(300))
    try:
        async with run_app(app):
            [wake] = await wait_for(lambda: app.inspector.list_scheduled_tasks("default"))
        app.inspector.archive_task("default", wake.id)
        stale = await queue.runner.store.delivery_watch(run_id)
        assert stale and stale.expected == wake.id
        assert isinstance(await queue.runner.deliver(TIMER, run_id), Parked)
        await queue.runner.store.fail_delivery(stale, "archived before replacement")
        replay = await queue.runner.deliver(TIMER, run_id, delivery_id=wake.id, next_delivery_id="obsolete")
        assert isinstance(replay, ObsoleteDelivery)
        assert (await queue.runner.inspect(run_id))["status"] == "running"
    finally:
        await queue.aclose()


async def test_queue_forwards_middleware_and_on_error(flow_url, run_app):
    from spater.flow import StepCall, StepNext

    def explode(req: Request) -> None:
        raise RuntimeError(f"failed-{req.seconds}")

    @Job.define("failing_queued")
    def job(f: Builder, req: Ref[Request]) -> None:
        f.step(explode, req, meta={"pool": "heavy"})

    app = Asynq(flow_url)
    queue = FlowQueue(app, [job])
    observed_mw: list[tuple[str, dict[str, Any]]] = []
    observed_err: list[tuple[str, str]] = []

    @queue.middleware
    async def track(call: StepCall, call_next: StepNext) -> Any:
        observed_mw.append((call.node_id, dict(call.meta)))
        return await call_next(call)

    @queue.on_error
    def record_err(call: StepCall, exc: BaseException) -> None:
        observed_err.append((call.node_id, str(exc)))

    run_id = await queue.enqueue(job, Request(9))
    try:
        async with run_app(app):
            row = await wait_terminal(queue.runner.store, run_id)
        assert row["status"] == "failed"
        assert observed_mw == [("explode", {"pool": "heavy"})]
        assert observed_err == [("explode", "failed-9")]
    finally:
        await queue.aclose()
