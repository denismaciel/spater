"""spater.flow: typed DAG definitions, validation, snapshots, graphs and sample jobs."""

from __future__ import annotations

import dataclasses
import enum
import importlib
import sys
from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any, NewType, cast

import pytest
from pydantic import BaseModel
from spater.flow import Builder, Context, FlowError, Job, PerStep, PerWorker, Ref, Retry, Runner, Succeeded
from spater.flow._types import Ty, Unsupported, assignable, normalize, serializable

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "examples"))

broken = importlib.import_module("flows.broken")
draw = importlib.import_module("flows.draw")
jobs = importlib.import_module("flows.jobs")
lr = importlib.import_module("flows.loss_runs")
ml = importlib.import_module("flows.mail")
od = importlib.import_module("flows.oddities")
ob = importlib.import_module("flows.onboarding")
q = importlib.import_module("flows.quotes")
rp = importlib.import_module("flows.reports")


@dataclasses.dataclass
class RecursiveNode:
    name: str
    child: RecursiveNode | None = None


@pytest.mark.parametrize("job", jobs.ALL, ids=lambda j: j.name)
def test_example_jobs_build_snapshot_and_graph(job: Job[Any, Any]) -> None:
    snap = job.to_dict()
    assert snap["spater_flow"] == 3
    assert snap["name"] == job.name
    assert len(job.version) == 12
    assert len(job.digest) == 64

    graph = job.graph()
    mermaid = graph.to_mermaid()
    dot = graph.to_dot()
    assert mermaid.startswith("flowchart TD\n")
    assert f'digraph "{job.name}"' in dot
    ids = {n.id for n in graph.nodes}
    assert "start" in ids and "done" in ids
    for edge in graph.edges:
        assert edge.src in ids and edge.dst in ids


@pytest.mark.parametrize(("expected_codes", "builder"), broken.REJECTED, ids=lambda item: getattr(item, "__name__", ""))
def test_rejected_jobs_fail_with_expected_codes(expected_codes: frozenset[str], builder: Any) -> None:
    with pytest.raises(FlowError) as exc:
        builder()
    assert expected_codes <= exc.value.codes, f"got {exc.value.codes}, expected {expected_codes}\n{exc.value}"


def test_generated_graphs_gallery_matches_definitions() -> None:
    gallery = Path(__file__).resolve().parents[1] / "examples" / "flows" / "GRAPHS.md"
    assert gallery.read_text() == draw.render()


async def test_all_example_jobs_are_executable(memory_url: str) -> None:
    from spater.testing import advance_time

    async with Runner(memory_url, concurrency=4) as runner:
        r_nightly = await runner.run(jobs.NIGHTLY, rp.Day("2026-11-24"))
        assert isinstance(r_nightly, Succeeded) and r_nightly.value == rp.Report("2026-11-24", 6)

        r_archived = await runner.run(jobs.ARCHIVED_REPORT, rp.Day("2026-11-24"))
        assert isinstance(r_archived, Succeeded) and r_archived.value == rp.Report("2026-11-24", 15)

        r_quote = await runner.run(jobs.CARRIER_QUOTE, q.CarrierAsk("s1", "a@carrier.test"))
        assert isinstance(r_quote, Succeeded) and isinstance(r_quote.value, q.Quote)

        r_sub = await runner.run(jobs.SUBMISSION, q.Submission("s1", ["silent@c.test", "fast@c.test"]))
        assert isinstance(r_sub, Succeeded) and r_sub.value.best is not None
        assert r_sub.value.best.carrier_email == "fast@c.test"

        r_onboard = await runner.run(jobs.ONBOARD, ob.Account("a1", "a1@example.com"))
        assert isinstance(r_onboard, Succeeded) and r_onboard.value == ob.Activation("a1", "a1@example.com")

        r_triage = await runner.run(jobs.TRIAGE, ml.Email("t1", "u@e.com", "Any updates?"))
        assert isinstance(r_triage, Succeeded) and r_triage.value is None

        policy = lr.Policy("p1", "c@carrier.test", days_until_request=1, reply_days=2)
        run_id = await runner.prepare(jobs.LOSS_RUNS, input=policy)
        await runner.deliver(jobs.LOSS_RUNS, run_id)
        advance_time(memory_url, timedelta(days=1, seconds=1))
        await runner.deliver(jobs.LOSS_RUNS, run_id)
        advance_time(memory_url, timedelta(days=2, seconds=1))
        r_loss = await runner.deliver(jobs.LOSS_RUNS, run_id)
        assert isinstance(r_loss, Succeeded) and r_loss.value == lr.LossRuns("p1", claims=1)


def test_type_model_basics() -> None:
    class Color(enum.Enum):
        RED = "red"

    class Model(BaseModel):
        x: int

    UserId = NewType("UserId", str)

    assert assignable(normalize(od.Dog), normalize(od.Animal))
    assert not assignable(normalize(od.Animal), normalize(od.Dog))
    assert assignable(normalize(list[od.Dog]), normalize(list[od.Animal]))
    assert assignable(normalize(UserId), normalize(str))
    assert serializable(normalize(Color))
    assert serializable(normalize(Model))
    assert serializable(normalize(RecursiveNode))
    assert serializable(normalize(UserId))
    assert not serializable(normalize(od.Connection))
    with pytest.raises(Unsupported):
        normalize(Any)
    assert str(Ty.of(int, str)) == "int | str"


def test_version_changes_on_structure_not_body() -> None:
    def f1(x: int) -> int:
        return x + 1

    def f2(x: int) -> int:
        return x + 2

    @Job.define("v")
    def j1(f: Builder, x: Ref[int]) -> Ref[int]:
        return f.step(f1, x, name="inc")

    @Job.define("v")
    def j2(f: Builder, x: Ref[int]) -> Ref[int]:
        return f.step(f1, x, name="inc", timeout=timedelta(seconds=5))

    @Job.define("v")
    def j3(f: Builder, x: Ref[int]) -> Ref[int]:
        return f.step(f2, x, name="inc")

    assert j1.version != j2.version
    assert j1.version != j3.version


def test_providers_accept_sync_async_and_generators_and_reject_invalid() -> None:
    def sync_gen() -> Iterator[int]:
        yield 1

    async def async_gen() -> AsyncIterator[str]:
        yield "ok"

    def step_adapter(ctx: Context) -> float:
        return float(len(ctx.run_id))

    def use_all(a: int, b: str, c: float) -> str:
        return f"{a}-{b}-{c}"

    @Job.define("prov", provides=[PerWorker(sync_gen), PerWorker(async_gen), PerStep(step_adapter)])
    def job(f: Builder) -> Ref[str]:
        return f.step(use_all)

    assert len(job.provides) == 3

    def bad_worker(x: int) -> int:
        return x

    def bad_step() -> int:
        return 1

    def bad_step_gen(ctx: Context) -> Iterator[int]:
        yield 1

    def nothing() -> None:
        return None

    for bad in (
        PerWorker(cast(Any, bad_worker)),
        PerStep(cast(Any, bad_step)),
        PerStep(cast(Any, bad_step_gen)),
        PerWorker(nothing),
    ):
        with pytest.raises(FlowError) as exc:

            @Job.define("bad_prov", provides=[bad])
            def _(f: Builder) -> None:
                f.step(nothing)

        assert "bad-provider" in exc.value.codes


def test_retry_accepts_exceptions_values_and_tuples() -> None:
    def fetch(x: int) -> int | str:
        return x

    @Job.define("retries")
    def job(f: Builder, x: Ref[int]) -> Ref[int | str]:
        return f.step(
            fetch,
            x,
            retry=Retry(on=(TimeoutError, ConnectionError, str), max=4, after=timedelta(seconds=1), backoff=2.0),
        )

    assert "retry ×4 on TimeoutError | ConnectionError | str" in job.graph().to_mermaid()


def test_kwargs_and_after_binding_and_validation() -> None:
    def combine(a: int, /, b: str, *, c: bool = False) -> str:
        return f"{a}:{b}:{c}"

    def make_str(a: int) -> str:
        return str(a)

    def make_bool(a: int) -> bool:
        return a > 0

    @Job.define("kw")
    def job(f: Builder, a: Ref[int]) -> Ref[str]:
        s = f.step(make_str, a)
        flag = f.step(make_bool, a)
        return f.step(combine, a, kwargs={"b": s, "c": flag})

    assert job.output is str

    with pytest.raises(FlowError) as exc:

        @Job.define("pos_only_kw")
        def _(f: Builder, a: Ref[int]) -> Ref[str]:
            s = f.step(make_str, a)
            return f.step(combine, kwargs={"a": a, "b": s})

    assert "bad-input" in exc.value.codes
