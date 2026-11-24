from __future__ import annotations

import asyncio
import hashlib
import time
from datetime import timedelta
from typing import Any

import pytest
from spater import testing
from spater.flow import Builder, Job, Ref, RunExistsError, Runner, Succeeded
from spater.flow._run_store import ClaimLost, ObsoleteDelivery, PendingWake, now, open_store

DEFINITION = '{"name":"example"}'
DIGEST = hashlib.sha256(DEFINITION.encode()).hexdigest()


@pytest.fixture(params=["memory_url", "pg_url"])
def url(request: pytest.FixtureRequest) -> str:
    return str(request.getfixturevalue(request.param))


def run(identity: str, *, parent: str | None = None, index: int = 0) -> dict[str, Any]:
    return {
        "id": identity,
        "root_id": "root" if parent is not None else identity,
        "parent_id": parent,
        "parent_node": "expand" if parent else None,
        "parent_attempt": 1 if parent else None,
        "item_index": index if parent else None,
        "definition": DIGEST,
        "job": "example",
        "input": {"value": index},
        "status": "running",
        "started_at": 1.0,
    }


async def test_idempotency_and_competing_child_admissions(url: str) -> None:
    store = open_store(url)
    other = open_store(url)
    graph = {"nodes": [{"id": "example"}], "edges": []}
    await asyncio.gather(store.create(run("root"), DEFINITION, graph=graph), other.create(run("root"), DEFINITION))
    owner = ("root", await store.claim("root"))
    await other.create(run("root") | {"started_at": 2}, DEFINITION)
    assert (await store.get("root"))["started_at"] == 1
    assert await other.graph(DIGEST) == graph
    assert await other.definition(DIGEST) == DEFINITION
    with pytest.raises(ValueError, match="different input"):
        await store.create(run("root") | {"input": {"value": 1}}, DEFINITION)
    a, b = run("a", parent="root"), run("b", parent="root")
    raced = await asyncio.gather(
        store.create(a, DEFINITION, owner=owner), other.create(b, DEFINITION, owner=owner), return_exceptions=True
    )
    assert sum(isinstance(v, ValueError) for v in raced) == 1
    (winner,) = await store.children("root")
    await store.create(winner, DEFINITION, owner=owner)
    await store.create(run("next", parent="root", index=1), DEFINITION, owner=owner)
    assert [row["item_index"] for row in await other.children("root")] == [0, 1]


async def test_replacement_fences_every_mutation_in_the_tree(url: str) -> None:
    store = open_store(url)
    await store.create(run("root"), DEFINITION)
    old = ("root", await store.claim("root"))
    await store.create(run("child", parent="root"), DEFINITION, owner=old)
    await store.put_attempt("child", "call", 1, {"status": "running"}, old)
    replacement = open_store(url)
    owner = ("root", await replacement.claim("root"))
    assert owner[1] > old[1]
    with pytest.raises(ClaimLost):
        await store.create(run("late", parent="root", index=1), DEFINITION, owner=old)
    with pytest.raises(ClaimLost):
        await store.put_attempt("child", "call", 1, {"status": "succeeded", "output": "old"}, old)
    with pytest.raises(ClaimLost):
        await store.update("root", {"status": "failed"}, old)
    await replacement.put_attempt("child", "call", 1, {"status": "succeeded", "output": None}, owner)
    row = await store.attempt("child", "call", 1)
    assert row and "output" in row and row["output"] is None
    await replacement.create(run("unrelated"), DEFINITION)
    with pytest.raises(ClaimLost):
        await replacement.update("unrelated", {"status": "cancelled"}, owner)
    with pytest.raises(ClaimLost):
        await replacement.put_attempt("unrelated", "call", 1, {"status": "running"}, owner)
    assert (await store.get("unrelated"))["status"] == "running"


async def test_terminal_cleanup_and_ordered_preview_are_atomic(url: str) -> None:
    store = open_store(url)
    await store.create(run("root"), DEFINITION)
    owner = ("root", await store.claim("root"))
    preview = {"z": 12, "a": 3, "q": None}
    await store.put_attempt("root", "map", 1, {"status": "running", "preview": preview, "manifest": [1, 2]}, owner)
    for i, identity in enumerate(("done", "interrupted")):
        await store.create(run(identity, parent="root", index=i), DEFINITION, owner=owner)
        await store.put_attempt(identity, "work", 1, {"status": "running", "output": [1, 2]}, owner)
    await store.put_attempt("done", "work", 1, {"status": "succeeded"}, owner)
    await store.update("done", {"status": "succeeded", "result": 12}, owner)
    await store.update("root", {"status": "cancelling"}, owner)
    with pytest.raises(ClaimLost):
        await store.create(run("late", parent="root", index=2), DEFINITION, owner=owner)
    with pytest.raises(ClaimLost):
        await store.put_attempt("interrupted", "new", 1, {"status": "running"}, owner)
    replacement = ("root", await store.claim("root"))
    await store.update("root", {"status": "cancelled", "finished_at": 4.0}, replacement)
    with pytest.raises(ClaimLost):
        await store.put_attempt("root", "map", 1, {"status": "succeeded"}, replacement)
    assert (await store.get("done"))["result"] == 12
    assert (await store.get("interrupted"))["status"] == "cancelled"
    (mapped,) = await store.attempts("root")
    assert mapped["status"] == "cancelled" and "manifest" not in mapped
    assert list(mapped["preview"]) == list(preview)
    (interrupted,) = await store.attempts("interrupted")
    assert interrupted["status"] == "cancelled" and "output" not in interrupted
    (done,) = await store.attempts("done")
    assert done["status"] == "succeeded" and "output" not in done


async def test_memory_reset_shares_clock_and_isolates_named_stores(memory_url: str) -> None:
    before = now(memory_url)
    store = open_store(memory_url)
    await store.create(run("root"), DEFINITION)
    other = testing.memory_url()
    try:
        testing.advance_time(memory_url, 300.0)
        assert now(memory_url) - before >= 300
        assert abs(now(other) - time.time()) < 1
        testing.reset_memory(memory_url)
        assert store.clock() - before >= 300
        with pytest.raises(KeyError):
            await open_store(memory_url).get("root")
        assert abs(now(memory_url) - time.time()) < 1
    finally:
        testing.reset_memory(other)


async def test_postgres_respects_dsn_schema(pg_url: str) -> None:
    import psycopg

    with psycopg.connect(pg_url) as conn:
        conn.execute("CREATE SCHEMA tenant")
    public_store = open_store(pg_url)
    tenant_store = open_store(pg_url + "?options=-csearch_path%3Dtenant")
    await public_store.create(run("root"), DEFINITION)
    with pytest.raises(KeyError):
        await tenant_store.get("root")
    await tenant_store.create(run("root") | {"input": "tenant value"}, DEFINITION)
    assert (await public_store.get("root"))["input"] == {"value": 0}
    assert (await tenant_store.get("root"))["input"] == "tenant value"


async def test_first_committed_failure_survives_replacement_and_closes_the_tree(url: str) -> None:
    store = open_store(url)
    await store.create(run("root"), DEFINITION)
    old = ("root", await store.claim("root"))
    await store.create(run("child", parent="root"), DEFINITION, owner=old)
    await store.create(run("grandchild", parent="child"), DEFINITION, owner=old)
    await store.create(run("sibling", parent="root", index=1), DEFINITION, owner=old)
    for ancestor in ("root", "child"):
        await store.put_attempt(ancestor, "expand", 1, {"status": "running", "manifest": [1, 2]}, old)
    await store.put_attempt("grandchild", "lookup", 1, {"status": "failed", "error": "first"}, old)
    await store.put_attempt("sibling", "upload", 1, {"status": "failed", "error": "later"}, old)
    await store.update("root", {"status": "cancelling"}, old)
    assert (await store.get("root"))["pending_failure"] == {
        "run_id": "grandchild",
        "node_id": "lookup",
        "error": "first",
    }

    replacement = open_store(url)
    owner = ("root", await replacement.claim("root"))
    with pytest.raises(ClaimLost):
        await store.finish_failure("root", old, 3.0)
    root = await replacement.finish_failure("root", owner, 4.0)
    assert await replacement.finish_failure("root", owner, 5.0) == root
    assert "pending_failure" not in root
    assert (root["status"], root["node_id"], root["error"]) == (
        "failed",
        "expand",
        "child child, expand: child grandchild, lookup: first",
    )
    assert (await store.get("child"))["error"] == "child grandchild, lookup: first"
    assert (await store.get("grandchild"))["error"] == "first"
    assert (await store.get("sibling"))["status"] == "cancelled"
    for ancestor in ("root", "child"):
        (attempt,) = await store.attempts(ancestor)
        assert attempt["status"] == "failed" and "manifest" not in attempt
        assert attempt["error"] == (await store.get(ancestor))["error"]


async def test_committed_cancellation_fences_later_failure(url: str) -> None:
    store = open_store(url)
    await store.create(run("root"), DEFINITION)
    owner = ("root", await store.claim("root"))
    await store.update("root", {"status": "cancelling"}, owner)
    with pytest.raises(ClaimLost):
        await store.put_attempt("root", "call", 1, {"status": "failed", "error": "late"}, owner)
    assert "pending_failure" not in (await store.get("root"))
    await store.update("root", {"status": "cancelled", "finished_at": 2}, owner)


def long_value() -> str:
    return "x" * 20_000


async def test_inspection_of_live_run_exposes_only_capped_previews(url: str) -> None:
    @Job.define("preview")
    def job(f: Builder) -> None:
        val = f.step(long_value)
        f.sleep(timedelta(days=1), after=val)

    async with Runner(url, preview_bytes=32) as runner:
        run_id = await runner.prepare(job, input=None)
        await runner.deliver(job, run_id)
        inspection = await runner.inspect(run_id)
        assert inspection["status"] == "running"
        step = next(row for row in inspection["attempts"] if row["node_id"] == "long_value")
        assert "output" not in step and step["preview"]["truncated"]
        assert len(step["preview"]["value"]) <= 32
        saved = await runner.store.attempt(run_id, "long_value", 1)
        assert saved and saved["output"]["value"] == long_value()


async def test_direct_run_wakes_when_memory_clock_advances(memory_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    @Job.define("timer")
    def job(f: Builder, n: Ref[int]) -> Ref[int]:
        f.sleep(timedelta(days=1))
        return n

    async with Runner(memory_url) as runner:
        waiting = asyncio.Event()
        wait_until = runner.store.wait_until

        async def observe_wait(due: float) -> None:
            waiting.set()
            await wait_until(due)

        monkeypatch.setattr(runner.store, "wait_until", observe_wait)
        task = asyncio.create_task(runner.run(job, 7))
        try:
            await asyncio.wait_for(waiting.wait(), 2)
            await asyncio.to_thread(testing.advance_time, memory_url, timedelta(hours=12))
            assert not task.done()
            await asyncio.to_thread(testing.advance_time, memory_url, timedelta(hours=12))
            result = await asyncio.wait_for(task, 2)
            assert isinstance(result, Succeeded) and result.value == 7
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_store_cancel_fences_active_owner_and_cancels_children(url: str) -> None:
    store = open_store(url)
    assert await store.create(run("root"), DEFINITION, fail_if_exists=True) is True
    with pytest.raises(RunExistsError):
        await store.create(run("root"), DEFINITION, fail_if_exists=True)
    owner = ("root", await store.claim("root"))
    await store.create(run("child", parent="root"), DEFINITION, owner=owner)
    cancelled = await store.cancel("root")
    assert cancelled["status"] == "cancelled"
    assert (await store.get("child"))["status"] == "cancelled"
    with pytest.raises(ClaimLost):
        await store.put_attempt("root", "step", 1, {"status": "running"}, owner)


async def test_postgres_pool_reuses_connections_and_recovers_after_disconnect(pg_url: str) -> None:
    import psycopg

    store = open_store(pg_url, pool_size=2)
    try:
        assert store._pool is not None
        pids: list[int] = []

        def read_pid(db: Any) -> int:
            row = db.execute("SELECT pg_backend_pid()").fetchone()
            assert row is not None
            return int(row[0])

        pids.append(await store._transaction(read_pid))
        pids.append(await store._transaction(read_pid))
        assert pids[0] == pids[1]

        # Terminate the pooled idle backend from an external connection; next op must recover cleanly.
        with psycopg.connect(pg_url, autocommit=True) as admin:
            admin.execute("SELECT pg_terminate_backend(%s)", (pids[0],))

        recovered_pid = await store._transaction(read_pid)
        assert recovered_pid != pids[0]

        # Concurrent transactions respect pool_size and leave idle connections reusable.
        await asyncio.gather(*(store._transaction(read_pid) for _ in range(6)))
        assert 1 <= len(store._pool._idle) <= 2
    finally:
        await store.aclose()
    assert len(store._pool._idle) == 0


async def test_only_expected_broker_task_can_execute_or_park(url: str) -> None:
    store, replacement = open_store(url), open_store(url)
    await store.create(run("root"), DEFINITION)
    assert await store.claim_delivery("root", "foreign") == ObsoleteDelivery()
    first = await store.claim_delivery("root", "root")
    assert isinstance(first, int)
    # Redelivery of the same task after lease loss remains recoverable.
    generation = await replacement.claim_delivery("root", "root")
    assert isinstance(generation, int) and generation > first
    with pytest.raises(ClaimLost):
        await store.park_delivery("root", "root", "wake", 42.0, ("root", first))
    owner = ("root", generation)
    with pytest.raises(ClaimLost):
        await store.park_delivery("root", "wrong-task", "wake", 42.0, owner)
    await replacement.park_delivery("root", "root", "wake", 42.0, owner)

    # Crash after committing park and before (or after) publishing its broker task.
    assert await open_store(url).claim_delivery("root", "root") == PendingWake(42.0)
    assert await store.claim_delivery("root", "root") == PendingWake(42.0)
    previous, wake = await asyncio.gather(
        store.claim_delivery("root", "root"), replacement.claim_delivery("root", "wake")
    )
    assert isinstance(previous, (PendingWake, ObsoleteDelivery))
    assert isinstance(wake, int) and wake > generation
    assert await store.claim_delivery("root", "root") == ObsoleteDelivery()
    with pytest.raises(ClaimLost):
        await store.put_attempt("root", "step", 1, {"status": "running"}, owner)
    # A second timer invalidates the first handoff and has its own saved deadline.
    await replacement.park_delivery("root", "wake", "second", 80.0, ("root", wake))
    assert await store.claim_delivery("root", "wake") == PendingWake(80.0)
    assert await store.claim_delivery("root", "root") == ObsoleteDelivery()
    final = await store.claim_delivery("root", "second")
    assert isinstance(final, int)
    await store.update("root", {"status": "succeeded"}, ("root", final))
    assert await replacement.claim_delivery("root", "second") == ObsoleteDelivery()
    with pytest.raises(ClaimLost):
        await store.park_delivery("root", "second", "extra", 100, ("root", final))


@pytest.mark.parametrize("reclaim_first", [False, True])
async def test_park_racing_same_task_redelivery_has_one_owner(url: str, reclaim_first: bool) -> None:
    store = open_store(url)
    await store.create(run("root"), DEFINITION)
    first = await store.claim_delivery("root", "root")
    assert isinstance(first, int)
    park = store.park_delivery("root", "root", "next", 42, ("root", first))
    reclaim = open_store(url).claim_delivery("root", "root")
    if reclaim_first:
        claimed, parked = await asyncio.gather(reclaim, park, return_exceptions=True)
    else:
        parked, claimed = await asyncio.gather(park, reclaim, return_exceptions=True)
    if isinstance(claimed, int):
        assert claimed > first and isinstance(parked, ClaimLost)
        assert await store.claim_delivery("root", "next") == ObsoleteDelivery()
    else:
        assert claimed == PendingWake(42) and parked is None
        assert isinstance(await store.claim_delivery("root", "next"), int)
