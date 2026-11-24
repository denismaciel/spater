"""A submission goes out to several carriers; each one is chased separately."""

from __future__ import annotations

from dataclasses import dataclass

from .mail import Mailer


@dataclass
class Submission:
    id: str
    carriers: list[str]


@dataclass
class CarrierAsk:
    submission_id: str
    carrier_email: str


@dataclass
class PendingAsk:
    ask: CarrierAsk
    thread_id: str


@dataclass
class Quote:
    carrier_email: str
    premium: float


@dataclass
class Recommendation:
    submission_id: str
    best: Quote | None


@dataclass
class NoQuote:
    reason: str


def carrier_unavailable(exc: TimeoutError) -> NoQuote:
    return NoQuote(str(exc))


def validate(sub: Submission) -> None:
    if not sub.carriers:
        raise ValueError("a submission needs at least one carrier")


def carriers(sub: Submission) -> list[CarrierAsk]:
    """What to fan out over. Everything a child needs is in the item."""
    return [CarrierAsk(sub.id, email) for email in sub.carriers]


async def send_ask(ask: CarrierAsk, mailer: Mailer) -> PendingAsk:
    return PendingAsk(ask, await mailer.send(ask.carrier_email, "Quote request", ask.submission_id))


def fetch_quote(pending: PendingAsk) -> Quote | NoQuote:
    if pending.ask.carrier_email.startswith("silent@"):
        return NoQuote(f"No reply from {pending.ask.carrier_email}")
    return Quote(pending.ask.carrier_email, premium=1000.0 + len(pending.ask.carrier_email))


def compare(sub: Submission, quotes: list[Quote | NoQuote]) -> Recommendation:
    offers = (quote for quote in quotes if isinstance(quote, Quote))
    return Recommendation(sub.id, min(offers, key=lambda quote: quote.premium, default=None))


async def notify_broker(rec: Recommendation, mailer: Mailer) -> None:
    await mailer.send("broker@example.com", "Quotes are in", str(rec.best))
