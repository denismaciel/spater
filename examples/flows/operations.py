"""Executable sample histories for an explicitly requested in-memory operations demo."""

from dataclasses import dataclass

from spater.flow import Builder, Job, Ref, Runner

from .jobs import ARCHIVED_REPORT, TRIAGE
from .mail import Email
from .reports import Day


@dataclass
class ReportingWeek:
    dates: list[str]


def report_days(week: ReportingWeek) -> list[Day]:
    return [Day(date) for date in week.dates]


@Job.define("weekly_reports")
def BATCH(f: Builder, week: Ref[ReportingWeek]) -> None:
    days = f.step(report_days, week)
    f.map(ARCHIVED_REPORT, days, max_parallel=3)


async def seed(runner: Runner) -> None:
    await runner.run(ARCHIVED_REPORT, Day("2026-11-16"))
    await runner.run(TRIAGE, Email("request-17", "broker@example.org", "Is the renewal ready?"))
    await runner.run(BATCH, ReportingWeek(["2026-11-16", "2026-11-17", "2026-11-18"]))
    await runner.run(TRIAGE, Email("claim-24", "claims@example.org", "open claim P-42"))
    await runner.run(TRIAGE, Email("spam-08", "offer@example.org", "Unsolicited offer"))
