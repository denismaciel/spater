"""Jobs the loader rejects. Each builder raises `FlowError` with the codes named
in its decorator; `tests/test_flow.py` runs all of them.

Read these as the list of mistakes you cannot ship.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta as td
from typing import Any

from spater.flow import Builder, Job, PerStep, PerWorker, Ref, Retry

from . import loss_runs as lr
from . import oddities as od
from . import quotes as q
from . import reports as rp
from . import triage as tr
from .jobs import CARRIER_QUOTE, DB, MAILER, report_write
from .mail import Email

REJECTED: list[tuple[frozenset[str], Callable[[], Job[Any, Any]]]] = []


def rejected(*codes: str) -> Callable[[Callable[[], Job[Any, Any]]], Callable[[], Job[Any, Any]]]:
    def register(build: Callable[[], Job[Any, Any]]) -> Callable[[], Job[Any, Any]]:
        REJECTED.append((frozenset(codes), build))
        return build

    return register


# ═══ data flow ══════════════════════════════════════════════════════════════


@rejected("type-mismatch")
def wrong_ref_type() -> Job[Any, Any]:
    @Job.define("wrong_type")
    def job(f: Builder, day: Ref[rp.Day]) -> Ref[rp.Report]:
        return f.step(rp.transform, day, day)  # day is Day, not Rows

    return job


@rejected("missing-input")
def parameter_nobody_provides() -> Job[Any, Any]:
    @Job.define("no_factor")
    def job(f: Builder, day: Ref[rp.Day]) -> None:
        f.step(od.scale, day)

    return job


@rejected("missing-input")
def provider_not_declared() -> Job[Any, Any]:
    @Job.define("no_db", provides=[PerStep(report_write)])
    def job(f: Builder, day: Ref[rp.Day]) -> None:
        rows = f.step(rp.extract, day)
        report = f.step(rp.transform, day, rows)
        f.step(rp.load, report)

    return job


@rejected("type-mismatch")
def optional_result_used_as_if_present() -> Job[Any, Any]:
    @Job.define("maybe_quote")
    def job(f: Builder, sub: Ref[q.Submission]) -> None:
        maybe = f.step(od.cheapest, sub)
        f.step(od.bind, maybe)

    return job


@rejected("bad-input")
def raw_value_passed_instead_of_ref() -> Job[Any, Any]:
    @Job.define("raw_value")
    def job(f: Builder, day: Ref[rp.Day]) -> None:
        f.step(od.scale, day, 2)  # type: ignore[arg-type]

    return job


@rejected("bad-input")
def unknown_kwarg() -> Job[Any, Any]:
    @Job.define("unknown_kwarg")
    def job(f: Builder, day: Ref[rp.Day]) -> None:
        f.step(rp.extract, kwargs={"nope": day})

    return job


@rejected("ref-out-of-scope")
def ref_leaked_from_branch_arm() -> Job[Any, Any]:
    leaked: list[Ref[Any]] = []

    def capture(b: Builder, question: Ref[tr.Question], email: Ref[Email]) -> Ref[tr.Draft]:
        d = b.step(tr.draft_answer, email, question)
        leaked.append(d)
        return d

    @Job.define("leaked", provides=[MAILER, DB])
    def job(f: Builder, email: Ref[Email]) -> None:
        outcome = f.step(tr.classify, email)
        f.branch(
            outcome,
            {
                tr.Spam: lambda b, _: b.fail("spam"),
                tr.Question: lambda b, question: capture(b, question, email),
                tr.Claim: lambda b, claim: b.step(tr.draft_ack, email, b.step(tr.open_claim, claim)),
            },
        )
        f.step(tr.send, leaked[0])

    return job


@rejected("missing-output")
def annotated_output_but_returned_none() -> Job[Any, Any]:
    @Job.define("no_return")
    def job(f: Builder, day: Ref[rp.Day]) -> Ref[rp.Report]:  # type: ignore[return]
        f.step(rp.extract, day)

    return job


# ═══ branching ══════════════════════════════════════════════════════════════


@rejected("branch-missing-case")
def missing_branch_case() -> Job[Any, Any]:
    @Job.define("missing_case", provides=[MAILER])
    def job(f: Builder, policy: Ref[lr.Policy]) -> None:
        pending = f.step(lr.send_request, policy)
        decision = f.step(lr.on_silence, pending)
        f.branch(decision, {lr.Remind: lambda b, _: b.step(lr.send_reminder, pending)})

    return job


@rejected("branch-extra-case")
def extra_branch_case() -> Job[Any, Any]:
    @Job.define("extra_case", provides=[MAILER])
    def job(f: Builder, policy: Ref[lr.Policy]) -> None:
        pending = f.step(lr.send_request, policy)
        decision = f.step(lr.on_silence, pending)
        f.branch(
            decision,
            {
                lr.Remind: lambda b, _: b.step(lr.send_reminder, pending),
                lr.GiveUp: lambda b, _: b.fail("give up"),
                rp.Day: lambda b, _: b.fail("impossible"),
            },
        )

    return job


@rejected("branch-overlap")
def overlapping_branch_outcomes() -> Job[Any, Any]:
    @Job.define("overlap")
    def job(f: Builder, animal: Ref[od.Animal]) -> None:
        sp = f.step(od.species, animal)
        f.branch(sp, {od.Animal: lambda b, _: None, od.Dog: lambda b, _: None})

    return job


@rejected("branch-return-mismatch")
def mixed_branch_arm_returns() -> Job[Any, Any]:
    @Job.define("mixed_return", provides=[MAILER, DB])
    def job(f: Builder, email: Ref[Email]) -> None:
        outcome = f.step(tr.classify, email)
        f.branch(
            outcome,
            {
                tr.Spam: lambda b, _: b.fail("spam"),
                tr.Question: lambda b, question: b.step(tr.draft_answer, email, question),
                tr.Claim: lambda b, claim: None,  # type: ignore[dict-item]
            },
        )

    return job


# ═══ structure ══════════════════════════════════════════════════════════════


@rejected("never-succeeds")
def every_path_fails() -> Job[Any, Any]:
    @Job.define("doomed")
    def job(f: Builder, day: Ref[rp.Day]) -> None:
        f.step(rp.extract, day)
        f.fail("always")

    return job


@rejected("unreachable", "never-succeeds")
def code_after_fail() -> Job[Any, Any]:
    @Job.define("dead_step")
    def job(f: Builder, day: Ref[rp.Day]) -> None:
        f.fail("stop")
        f.step(rp.extract, day)

    return job


@rejected("duplicate-name")
def same_step_twice_without_name() -> Job[Any, Any]:
    @Job.define("dup")
    def job(f: Builder, day: Ref[rp.Day]) -> None:
        f.step(rp.extract, day)
        f.step(rp.extract, day)

    return job


@rejected("bad-option")
def empty_job() -> Job[Any, Any]:
    @Job.define("empty")
    def job(f: Builder, day: Ref[rp.Day]) -> None:
        return None

    return job


# ═══ signatures & serialization ═════════════════════════════════════════════


@rejected("missing-annotation")
def unannotated_function() -> Job[Any, Any]:
    @Job.define("untyped")
    def job(f: Builder, day: Ref[rp.Day]) -> None:
        f.step(od.untyped, day)

    return job


@rejected("bad-signature")
def varargs_function() -> Job[Any, Any]:
    @Job.define("greedy")
    def job(f: Builder, day: Ref[rp.Day]) -> None:
        f.step(od.greedy, day)

    return job


@rejected("unsupported-type")
def any_return_type() -> Job[Any, Any]:
    @Job.define("vague")
    def job(f: Builder, day: Ref[rp.Day]) -> None:
        f.step(od.vague, day)

    return job


@rejected("not-serializable")
def unserializable_step_result() -> Job[Any, Any]:
    @Job.define("conn")
    def job(f: Builder, day: Ref[rp.Day]) -> None:
        f.step(od.connect, day)

    return job


@rejected("bad-callable")
def lambda_step() -> Job[Any, Any]:
    @Job.define("lambda_step")
    def job(f: Builder, day: Ref[rp.Day]) -> None:
        f.step(lambda d: d, day)

    return job


# ═══ retries, catch, sleep, map, providers ══════════════════════════════════


@rejected("bad-retry")
def retry_on_unrelated_type() -> Job[Any, Any]:
    @Job.define("bad_retry")
    def job(f: Builder, day: Ref[rp.Day]) -> None:
        f.step(rp.extract, day, retry=Retry(on=rp.Report, max=2, after=td(seconds=1)))

    return job


@rejected("bad-catch")
def catch_non_exception() -> Job[Any, Any]:
    @Job.define("bad_catch")
    def job(f: Builder, day: Ref[rp.Day]) -> None:
        f.step(rp.extract, day, catch={rp.Day: q.carrier_unavailable})  # type: ignore[dict-item]

    return job


@rejected("bad-duration")
def sleep_duration_uses_provider() -> Job[Any, Any]:
    @Job.define("db_sleep", provides=[DB])
    def job(f: Builder, day: Ref[rp.Day]) -> None:
        f.sleep(od.db_delay)

    return job


@rejected("map-items-not-list")
def map_on_non_list() -> Job[Any, Any]:
    @Job.define("map_one")
    def job(f: Builder, sub: Ref[q.Submission]) -> None:
        one = f.step(od.first_carrier, sub)
        f.map(CARRIER_QUOTE, one)  # type: ignore[arg-type]

    return job


@rejected("map-item-mismatch")
def map_item_type_mismatch() -> Job[Any, Any]:
    @Job.define("map_wrong_item")
    def job(f: Builder, sub: Ref[q.Submission]) -> None:
        names = f.step(od.carrier_names, sub)
        f.map(CARRIER_QUOTE, names)  # type: ignore[arg-type]

    return job


@rejected("bad-provider")
def duplicate_provider_type() -> Job[Any, Any]:
    from .mail import Db, MemoryDb

    def other_db() -> Db:
        return MemoryDb()

    @Job.define("dup_provider", provides=[DB, PerWorker(other_db)])
    def job(f: Builder, day: Ref[rp.Day]) -> None:
        f.step(rp.extract, day)

    return job
