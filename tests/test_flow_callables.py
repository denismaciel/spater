from __future__ import annotations

from collections.abc import Awaitable
from datetime import timedelta

import pytest
from spater.flow import Builder, FlowError, Job, PerWorker, Ref, Runner, Succeeded


async def fetch(count: int) -> int:
    if count == 0:
        raise TimeoutError("awaiting")
    return count


def start_fetch(count: int) -> Awaitable[int]:
    if count < 0:
        raise TimeoutError("starting")
    return fetch(count)


def unavailable(error: TimeoutError) -> str:
    return str(error)


@pytest.mark.parametrize(("count", "expected"), [(-1, "starting"), (0, "awaiting"), (7, 7)])
async def test_catch_awaitable_factory_handles_creation_and_await(
    memory_url: str, count: int, expected: str | int
) -> None:
    @Job.define("fetch")
    def job(f: Builder, n: Ref[int]) -> Ref[int | str]:
        return f.step(start_fetch, n, catch={TimeoutError: unavailable})

    async with Runner(memory_url) as runner:
        result = await runner.run(job, count)
    assert isinstance(result, Succeeded) and result.value == expected


def count_provider() -> int:
    return 3


def duration(count: int) -> timedelta:
    return timedelta(days=count)


def default_duration(count: int = 1) -> timedelta:
    return timedelta(days=count)


def consume(count: int) -> int:
    return count


def produce_count() -> int:
    return 10


async def test_pure_sleep_reads_explicit_ref_not_provider(memory_url: str) -> None:
    @Job.define("stored_only", provides=[PerWorker(count_provider)])
    def job(f: Builder, n: Ref[int]) -> Ref[int]:
        w = f.sleep(duration, n)
        return f.step(consume, n, after=w)

    assert job.output is int


def test_pure_sleep_defaults_apply_and_reject_provider_only() -> None:
    @Job.define("optional", provides=[PerWorker(count_provider)])
    def optional_job(f: Builder) -> None:
        f.sleep(default_duration)

    assert optional_job.name == "optional"

    with pytest.raises(FlowError) as exc:

        @Job.define("required", provides=[PerWorker(count_provider)])
        def _(f: Builder) -> None:
            f.sleep(duration)

    assert exc.value.codes == {"bad-duration"}


async def test_explicit_ref_overrides_provider_type_without_ambiguity(memory_url: str) -> None:
    @Job.define("explicit_vs_provider", provides=[PerWorker(count_provider)])
    def job(f: Builder) -> Ref[int]:
        from_step = f.step(produce_count)
        via_provider = f.step(consume, name="from_provider")
        return f.step(lambda_free_add, from_step, via_provider)

    async with Runner(memory_url) as runner:
        res = await runner.run(job)
    assert isinstance(res, Succeeded) and res.value == 13


def lambda_free_add(a: int, b: int) -> int:
    return a + b
