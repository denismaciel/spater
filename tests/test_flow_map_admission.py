"""Admission through the direct Map API, including virtual sleepers and nested maps."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest
from spater import testing
from spater.flow import Builder, Job, PerWorker, Ref, Runner, Succeeded


@dataclass
class Batch:
    count: int


def items(batch: Batch) -> list[int]:
    return list(range(batch.count))


class Control:
    def __init__(self) -> None:
        self.began: asyncio.Queue[int] = asyncio.Queue()
        self.release = asyncio.Event()


control: Control | None = None


def provide_control() -> Control:
    assert control is not None
    return control


async def block(index: int, control: Control) -> str:
    control.began.put_nowait(index)
    await control.release.wait()
    return str(index)


def track_root(runner: Runner, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    roots: list[str] = []
    create = runner.store.create
    create_and_claim = runner.store.create_and_claim

    async def track(row: dict[str, Any], *args: Any, **kwargs: Any) -> bool:
        created = await create(row, *args, **kwargs)
        if row["id"] == row["root_id"]:
            roots.append(row["id"])
        return created

    async def track_claim(row: dict[str, Any], *args: Any, **kwargs: Any) -> int:
        gen = await create_and_claim(row, *args, **kwargs)
        if row["id"] == row["root_id"]:
            roots.append(row["id"])
        return gen

    monkeypatch.setattr(runner.store, "create", track)
    monkeypatch.setattr(runner.store, "create_and_claim", track_claim)
    return roots


async def test_large_manifest_allocates_only_bounded_drives_and_rows(
    memory_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate = Control()
    monkeypatch.setattr(__name__ + ".control", gate)

    @Job.define("child", provides=[PerWorker(provide_control)])
    def child(f: Builder, n: Ref[int]) -> Ref[str]:
        return f.step(block, n)

    @Job.define("fanout")
    def parent(f: Builder, batch: Ref[Batch]) -> None:
        seq = f.step(items, batch)
        f.map(child, seq)

    async with Runner(memory_url, concurrency=2) as runner:
        roots = track_root(runner, monkeypatch)
        before = asyncio.all_tasks()
        work = asyncio.create_task(runner.run(parent, Batch(2_000)))
        try:
            assert {await asyncio.wait_for(gate.began.get(), 2), await asyncio.wait_for(gate.began.get(), 2)} == {0, 1}
            assert len(await runner.store.children(roots[0])) == 2
            assert len(asyncio.all_tasks() - before) < 30
        finally:
            work.cancel()
            gate.release.set()
            await asyncio.gather(work, return_exceptions=True)


@pytest.mark.parametrize("max_parallel", [None, 5])
async def test_parked_child_occupies_only_its_local_map_slot(
    max_parallel: int | None,
    memory_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @Job.define("child")
    def child(f: Builder, n: Ref[int]) -> None:
        f.sleep(timedelta(days=1))

    @Job.define("sleepers")
    def root(f: Builder, batch: Ref[Batch]) -> None:
        seq = f.step(items, batch)
        f.map(child, seq, max_parallel=max_parallel)

    sleeping = asyncio.Event()
    async with Runner(memory_url, concurrency=1) as runner:
        roots = track_root(runner, monkeypatch)
        wait_until = runner.store.wait_until

        async def mark_timer(due: float) -> None:
            sleeping.set()
            await wait_until(due)

        monkeypatch.setattr(runner.store, "wait_until", mark_timer)
        before = asyncio.all_tasks()
        work = asyncio.create_task(runner.run(root, Batch(80)))
        try:
            await asyncio.wait_for(sleeping.wait(), 2)
            assert len(await runner.store.children(roots[0])) == (max_parallel or 80)
            assert len(asyncio.all_tasks() - before) < 10
        finally:
            work.cancel()
            await asyncio.gather(work, return_exceptions=True)


@dataclass
class Sleeper:
    pass


@dataclass
class Blocker:
    pass


def kind(index: int) -> Sleeper | Blocker:
    return Sleeper() if index == 0 else Blocker()


async def unblock(control: Control) -> str:
    control.release.set()
    return "slept"


@dataclass
class Results:
    items: list[str]


def collect(values: list[str]) -> Results:
    return Results(values)


async def wait_for_release(index: int, control: Control) -> None:
    await block(index, control)


@pytest.mark.parametrize("parallel_inside_child", [False, True])
async def test_sleeper_wakes_while_a_sibling_is_still_executing(
    parallel_inside_child: bool, memory_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate = Control()
    monkeypatch.setattr(__name__ + ".control", gate)
    if parallel_inside_child:

        @Job.define("parallel_child", provides=[PerWorker(provide_control)])
        def child(f: Builder, n: Ref[int]) -> Ref[str]:
            w = f.sleep(timedelta(days=1))
            out = f.step(unblock, after=w)
            f.step(wait_for_release, n)
            return out

        count, blocker, expected = 1, 0, ["slept"]
    else:

        @Job.define("siblings", provides=[PerWorker(provide_control)])
        def child(f: Builder, n: Ref[int]) -> Ref[str]:
            which = f.step(kind, n)
            return f.branch(
                which,
                {
                    Sleeper: lambda b, _: b.step(unblock, after=b.sleep(timedelta(days=1))),
                    Blocker: lambda b, _: b.step(block, n),
                },
            )

        count, blocker, expected = 2, 1, ["slept", "1"]

    @Job.define("root")
    def root(f: Builder, batch: Ref[Batch]) -> Ref[Results]:
        seq = f.step(items, batch)
        mapped = f.map(child, seq)
        return f.step(collect, mapped)

    async with Runner(memory_url, concurrency=2) as runner:
        timer = asyncio.Event()
        wait_until = runner.store.wait_until

        async def mark_timer(due: float) -> None:
            timer.set()
            await wait_until(due)

        monkeypatch.setattr(runner.store, "wait_until", mark_timer)
        work = asyncio.create_task(runner.run(root, Batch(count)))
        try:
            await asyncio.wait_for(timer.wait(), 2)
            assert await asyncio.wait_for(gate.began.get(), 2) == blocker
            testing.advance_time(memory_url, timedelta(days=1))
            result = await asyncio.wait_for(work, 2)
            assert isinstance(result, Succeeded) and result.value == Results(expected)
        finally:
            work.cancel()
            gate.release.set()
            await asyncio.gather(work, return_exceptions=True)


def children(index: int) -> list[int]:
    return [index * 10 + n for n in range(3)]


def label(index: int) -> str:
    return str(index)


def flatten(groups: list[Results]) -> Results:
    return Results([item for group in groups for item in group.items])


async def test_nested_map_runs_and_joins_in_order_with_one_root_slot(memory_url: str) -> None:
    @Job.define("leaf")
    def leaf(f: Builder, n: Ref[int]) -> Ref[str]:
        return f.step(label, n)

    @Job.define("child")
    def child(f: Builder, n: Ref[int]) -> Ref[Results]:
        nums = f.step(children, n)
        labels = f.map(leaf, nums)
        return f.step(collect, labels)

    @Job.define("root")
    def root(f: Builder, batch: Ref[Batch]) -> Ref[Results]:
        seq = f.step(items, batch)
        groups = f.map(child, seq)
        return f.step(flatten, groups)

    async with Runner(memory_url, concurrency=1) as runner:
        result = await asyncio.wait_for(runner.run(root, Batch(2)), 2)
    assert isinstance(result, Succeeded) and result.value == Results(["0", "1", "2", "10", "11", "12"])
