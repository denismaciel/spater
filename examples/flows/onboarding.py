"""Three independent things happen at once, then the account is activated."""

from __future__ import annotations

from dataclasses import dataclass

from .mail import Mailer


@dataclass
class Account:
    id: str
    email: str


@dataclass
class Mailbox:
    address: str


@dataclass
class Documents:
    account_id: str
    files: list[str]


@dataclass
class CreditScore:
    value: int


@dataclass
class Activation:
    account_id: str
    mailbox: str


@dataclass
class CreditDeclined:
    score: int


@dataclass
class MissingDocuments:
    account_id: str


def create_mailbox(account: Account) -> Mailbox:
    return Mailbox(f"{account.id}@example.com")


async def fetch_documents(account: Account, mailer: Mailer) -> Documents:
    await mailer.send(account.email, "Documents verified", account.id)
    return Documents(account.id, ["id.pdf"])


def credit_check(account: Account) -> CreditScore:
    return CreditScore(700)


def activate(
    account: Account, mailbox: Mailbox, documents: Documents, score: CreditScore
) -> Activation | CreditDeclined | MissingDocuments:
    if score.value < 500:
        return CreditDeclined(score.value)
    if not documents.files:
        return MissingDocuments(account.id)
    return Activation(account.id, mailbox.address)
