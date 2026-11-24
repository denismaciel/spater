"""Requesting loss runs from a carrier with scheduled follow-up. Plain business code."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from .mail import Db, Email, Mailer


@dataclass
class Policy:
    id: str
    carrier_email: str
    days_until_request: int = 1
    reply_days: int = 5
    allow_reminder: bool = True


@dataclass
class Pending:
    policy: Policy
    thread_id: str


@dataclass
class LossRuns:
    policy_id: str
    claims: int


@dataclass
class Received:
    reply: Email


@dataclass
class NoReply:
    pending: Pending


@dataclass
class Remind:
    pass


@dataclass
class GiveUp:
    pass


def until_request_window(policy: Policy) -> timedelta:
    return timedelta(days=policy.days_until_request)


def reply_window(policy: Policy) -> timedelta:
    return timedelta(days=policy.reply_days)


async def send_request(policy: Policy, mailer: Mailer) -> Pending:
    thread_id = await mailer.send(policy.carrier_email, "Loss runs request", f"Policy {policy.id}")
    return Pending(policy, thread_id)


def check_reply(pending: Pending) -> Received | NoReply:
    return Received(Email(pending.thread_id, pending.policy.carrier_email, "1 claim on file"))


def on_silence(pending: Pending) -> Remind | GiveUp:
    """The decision is pure, so it is testable without any workflow engine."""
    return Remind() if pending.policy.allow_reminder else GiveUp()


async def send_reminder(pending: Pending, mailer: Mailer) -> None:
    await mailer.send(pending.policy.carrier_email, "Reminder: loss runs", f"Policy {pending.policy.id}")


def parse(received: Received, pending: Pending) -> LossRuns:
    return LossRuns(pending.policy.id, claims=received.reply.body.count("claim"))


async def store(runs: LossRuns, db: Db) -> None:
    await db.save("loss_runs", runs)
