"""Static typing checks for `@Job.define`, `Builder`, `Ref`, `Runner` and `FlowQueue`."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from spater import Asynq
from spater.flow import (
    Builder,
    Failed,
    FlowQueue,
    Job,
    LocalExecutor,
    Ref,
    Retry,
    Runner,
    StepCall,
    StepExecutor,
    StepNext,
    Succeeded,
)
from typing_extensions import assert_type


@dataclass
class Day:
    date: str


@dataclass
class Rows:
    values: list[int]


@dataclass
class Report:
    total: int


@dataclass
class Fallback:
    reason: str


def extract(day: Day) -> Rows:
    return Rows([len(day.date)])


async def transform(day: Day, rows: Rows) -> Report:
    return Report(sum(rows.values))


def to_fallback(exc: TimeoutError) -> Fallback:
    return Fallback(str(exc))


def make_days(day: Day) -> list[Day]:
    return [day]


def make_strings(day: Day) -> list[str]:
    return [day.date]


def ping() -> None:
    pass


@Job.define("nightly")
def NIGHTLY(f: Builder, day: Ref[Day]) -> Ref[Report]:
    rows = f.step(extract, day)
    assert_type(rows, Ref[Rows])
    report = f.step(transform, day, rows)
    assert_type(report, Ref[Report])
    return report


@Job.define("ping")
def PING(f: Builder) -> None:
    f.step(ping)


@Job.define("with_catch")
def WITH_CATCH(f: Builder, day: Ref[Day]) -> Ref[Report | Fallback]:
    rows = f.step(extract, day)
    outcome = f.step(
        transform,
        day,
        rows,
        retry=Retry(on=TimeoutError, max=2, after=timedelta(seconds=1)),
        catch={TimeoutError: to_fallback},
    )
    assert_type(outcome, Ref[Report | Fallback])
    return f.branch(
        outcome,
        {
            Report: lambda _, r: r,
            Fallback: lambda _, fb: fb,
        },
    )


@Job.define("batch")
def BATCH(f: Builder, day: Ref[Day]) -> Ref[list[Report]]:
    days = f.step(make_days, day)
    reports = f.map(NIGHTLY, days)
    assert_type(reports, Ref[list[Report]])
    return reports


def check_bad_map(f: Builder, day: Ref[Day]) -> None:
    bad = f.step(make_strings, day)
    f.map(NIGHTLY, bad)  # type: ignore[arg-type, misc]


async def check_execution(runner: Runner, queue: FlowQueue, app: Asynq) -> None:
    assert_type(NIGHTLY, Job[Day, Report])
    assert_type(PING, Job[None, None])
    assert_type(WITH_CATCH, Job[Day, Report | Fallback])
    assert_type(BATCH, Job[Day, list[Report]])

    res = await runner.run(NIGHTLY, Day("2026-11-24"))
    assert_type(res, Succeeded[Report] | Failed)
    if isinstance(res, Succeeded):
        assert_type(res.value, Report)

    ping_res = await runner.run(PING)
    assert_type(ping_res, Succeeded[None] | Failed)

    await runner.run(NIGHTLY, "wrong")  # type: ignore[misc]
    await queue.enqueue(NIGHTLY, Day("2026-11-24"), run_id="day-1", on_duplicate="use_existing")
    await queue.enqueue(NIGHTLY, "wrong")  # type: ignore[misc]
    await queue.enqueue(PING)

    executor: StepExecutor = LocalExecutor()
    custom_runner = Runner("memory://typing", executor=executor, pool_size=4)

    @custom_runner.middleware
    async def trace(call: StepCall, call_next: StepNext) -> object:
        assert_type(call.run_id, str)
        assert_type(call.attempt, int)
        return await call_next(call)

    @custom_runner.on_error
    def handle_err(call: StepCall, exc: BaseException) -> None:
        _ = (call.node_id, exc)

    custom_queue = FlowQueue(app, [NIGHTLY], executor=executor, middleware=[trace], on_error=[handle_err])
    _ = custom_queue
