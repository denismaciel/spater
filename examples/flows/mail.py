"""Shared mail types. Nothing here knows about spater."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass
class Email:
    thread_id: str
    sender: str
    body: str


class Mailer(Protocol):
    """A resource: injected by the runtime, never stored."""

    async def send(self, to: str, subject: str, body: str) -> str: ...


class Db(Protocol):
    async def save(self, table: str, row: object, *, key: str | None = None) -> None: ...


class Outbox:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str, str]] = []

    async def send(self, to: str, subject: str, body: str) -> str:
        thread_id = str(len(self.messages) + 1)
        self.messages.append((to, subject, body))
        return thread_id


class MemoryDb:
    def __init__(self) -> None:
        self.tables: dict[str, dict[str | int, object]] = {}

    async def save(self, table: str, row: object, *, key: str | None = None) -> None:
        rows = self.tables.setdefault(table, {})
        rows[key if key is not None else len(rows)] = row


def open_mailer() -> Mailer:
    return Outbox()


def open_db() -> Db:
    return MemoryDb()
