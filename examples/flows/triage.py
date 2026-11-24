"""Classify an incoming email and answer it. The classifier is a step with three outcomes."""

from __future__ import annotations

from dataclasses import dataclass

from .mail import Db, Email, Mailer


@dataclass
class Spam:
    score: float


@dataclass
class Question:
    topic: str


@dataclass
class Claim:
    policy_id: str


@dataclass
class ClaimRef:
    number: str


@dataclass
class Draft:
    to: str
    body: str


async def classify(email: Email) -> Spam | Question | Claim:
    """An LLM call in real life: slow and fallible, so it is a Step, not a decision function."""
    if "claim" in email.body:
        return Claim(policy_id=email.body.split()[-1])
    return Question(topic=email.body[:20]) if "?" in email.body else Spam(score=0.9)


def draft_answer(email: Email, question: Question) -> Draft:
    return Draft(email.sender, f"About {question.topic}: ...")


async def open_claim(claim: Claim, db: Db) -> ClaimRef:
    await db.save("claims", claim)
    return ClaimRef(f"C-{claim.policy_id}")


def draft_ack(email: Email, ref: ClaimRef) -> Draft:
    return Draft(email.sender, f"We opened claim {ref.number}")


async def send(draft: Draft, mailer: Mailer) -> None:
    await mailer.send(draft.to, "Re: your message", draft.body)
