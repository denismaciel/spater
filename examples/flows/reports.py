"""A plain three-step pipeline."""

from __future__ import annotations

from dataclasses import dataclass

from .mail import Db


@dataclass
class Day:
    date: str


@dataclass
class Rows:
    values: list[int]


@dataclass
class Report:
    date: str
    total: int


def extract(day: Day) -> Rows:
    return Rows([1, 2, 3])


def extract_archive(day: Day) -> Rows:
    return Rows([4, 5, 6])


@dataclass
class ReportWrite:
    key: str


def transform(day: Day, rows: Rows) -> Report:
    return Report(day.date, sum(rows.values))


async def load(report: Report, db: Db, write: ReportWrite) -> None:
    await db.save("reports", report, key=write.key)
