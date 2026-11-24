"""Business-looking functions that are wrong in some way. Used by broken.py."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from .loss_runs import GiveUp, Pending, Remind
from .mail import Db
from .onboarding import Account
from .quotes import CarrierAsk, Quote, Submission
from .reports import Day


@dataclass
class Shelter:
    city: str


@dataclass
class Animal:
    name: str


@dataclass
class Dog(Animal):
    pass


@dataclass
class Cat(Animal):
    pass


class Connection:
    """Not a dataclass: there is no way to store one between steps."""


def find_dog(shelter: Shelter) -> Dog:
    return Dog("Rex")


def find_cat(shelter: Shelter) -> Cat:
    return Cat("Tom")


def feed(animal: Animal) -> None:
    pass


def species(animal: Animal) -> Animal | Dog:
    return animal


def untyped(day):
    return day


def greedy(*days: Day) -> None:
    pass


def vague(day: Day) -> Any:
    return day


def connect(day: Day) -> Connection:
    return Connection()


def scale(day: Day, factor: int) -> None:
    pass


async def slow_choice(pending: Pending) -> Remind | GiveUp:
    return Remind()


def peek(pending: Pending, db: Db) -> Remind | GiveUp:
    return Remind()


def numeric_key(account: Account) -> int:
    return 1


def first_carrier(sub: Submission) -> CarrierAsk:
    return CarrierAsk(sub.id, sub.carriers[0])


def carrier_names(sub: Submission) -> list[str]:
    return sub.carriers


def db_delay(db: Db) -> timedelta:
    return timedelta(hours=1)


def cheapest(sub: Submission) -> Quote | None:
    return None


def bind(quote: Quote) -> None:
    pass
