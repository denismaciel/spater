"""A deliberately small model of Python annotations for workflow boundaries and codecs.

Workflow steps are checked by comparing Ref types with parameter annotations.
This understands classes, unions, `list[T]`, `NewType` and a few opaque containers,
and rejects unsupported annotations with a clear error instead of guessing.
"""

from __future__ import annotations

import asyncio
import collections.abc
import dataclasses
import datetime
import enum
import importlib
import inspect
import sys
import types
import typing
import uuid
from typing import Any, NoReturn, TypeGuard


def assert_never(value: NoReturn) -> NoReturn:
    """Exhaustiveness check for the project's Python 3.10 minimum."""
    raise AssertionError(f"unexpected variant: {value!r}")


NoneType = type(None)
_OPAQUE = (dict, tuple, set, frozenset, collections.abc.Sequence, collections.abc.Mapping)
_JSON_SCALARS = (NoneType, str, int, float, bool, dict, list, uuid.UUID, datetime.datetime, datetime.date)


class Unsupported(Exception):
    pass


class SigError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclasses.dataclass(frozen=True)
class ListOf:
    item: Ty


@dataclasses.dataclass(frozen=True)
class Ty:
    """A normalized annotation: one or more alternatives."""

    alts: frozenset[Any]

    @staticmethod
    def of(*atoms: Any) -> Ty:
        return Ty(frozenset(atoms))

    def text(self, *, full: bool = False) -> str:
        return " | ".join(sorted(_atom_text(a, full) for a in self.alts))

    def __str__(self) -> str:
        return self.text()


def awaited_annotation(annotation: Any) -> tuple[bool, Any]:
    """Whether a synchronous factory promises an awaitable, and the type it yields."""
    origin = typing.get_origin(annotation) or annotation
    if isinstance(origin, type) and issubclass(origin, collections.abc.Awaitable):
        args = typing.get_args(annotation)
        if origin is collections.abc.Coroutine and len(args) == 3:
            return True, args[2]
        if origin in (collections.abc.Awaitable, asyncio.Future, asyncio.Task) and len(args) == 1:
            return True, args[0]
        return True, Any
    return False, annotation


def plain_class(value: Any) -> TypeGuard[type]:
    # Python 3.10 also reports parameterized aliases such as dict[str, int] as classes.
    return isinstance(value, type) and typing.get_origin(value) is None


def _atom_text(atom: Any, full: bool) -> str:
    if atom is NoneType:
        return "None"
    if isinstance(atom, ListOf):
        return f"list[{atom.item.text(full=full)}]"
    if plain_class(atom) or isinstance(atom, typing.NewType):
        if not full or getattr(atom, "__module__", "") == "builtins":
            return str(getattr(atom, "__qualname__", atom))
        return ref(atom)
    return repr(atom)


def normalize(ann: Any) -> Ty:
    if ann is None or ann is NoneType:
        return Ty.of(NoneType)
    if ann is Any:
        raise Unsupported("Any says nothing about what flows between steps")
    origin = typing.get_origin(ann)
    if origin is typing.Union or origin is types.UnionType:
        alts: set[Any] = set()
        for arg in typing.get_args(ann):
            alts |= normalize(arg).alts
        return Ty(frozenset(alts))
    if origin is list:
        args = typing.get_args(ann)
        if len(args) != 1:
            raise Unsupported("a list annotation needs one item type")
        return Ty.of(ListOf(normalize(args[0])))
    if origin is typing.Literal:
        args = typing.get_args(ann)
        if not args:
            raise Unsupported("Literal needs at least one value")
        for arg in args:
            if not (arg is None or type(arg) in (str, int, bool) or isinstance(arg, enum.Enum)):
                raise Unsupported(f"Literal value {arg!r} is not supported")
        return Ty.of(ann)
    if origin in _OPAQUE:
        for arg in typing.get_args(ann):
            if arg is not Ellipsis:
                normalize(arg)
        return Ty.of(ann)
    if isinstance(ann, typing.NewType):
        normalize(ann.__supertype__)
        return Ty.of(ann)
    if isinstance(ann, type) and origin is None:
        return Ty.of(ann)
    raise Unsupported(f"{ann!r} is not supported; use a class, a union, list[T] or a NewType")


def assignable(src: Ty, dst: Ty) -> bool:
    """Whether every alternative of `src` fits some alternative of `dst`."""
    return all(any(_atom_fits(a, b) for b in dst.alts) for a in src.alts)


def _atom_fits(a: Any, b: Any) -> bool:
    if a is b or a == b:
        return True
    if typing.get_origin(a) is typing.Literal:
        a_args = typing.get_args(a)
        if typing.get_origin(b) is typing.Literal:
            b_args = typing.get_args(b)
            return all(any(type(x) is type(y) and x == y for y in b_args) for x in a_args)
        return all(_atom_fits(type(x), b) for x in a_args)
    if isinstance(a, ListOf):
        return isinstance(b, ListOf) and assignable(a.item, b.item)
    if isinstance(a, typing.NewType):
        return assignable(normalize(a.__supertype__), Ty.of(b))
    if plain_class(a) and plain_class(b):
        try:
            return issubclass(a, b)
        except TypeError:  # non-runtime Protocol
            return False
    return False


def serializable(ty: Ty) -> bool:
    try:
        return _storable(ty, frozenset())
    except (Unsupported, NameError, TypeError):
        return False


def _storable(ty: Ty, parents: frozenset[type]) -> bool:
    return all(_atom_serializable(atom, parents) for atom in ty.alts)


def _atom_serializable(atom: Any, parents: frozenset[type]) -> bool:
    if isinstance(atom, ListOf):
        return _storable(atom.item, parents)
    if isinstance(atom, typing.NewType):
        return _storable(normalize(atom.__supertype__), parents)
    if plain_class(atom):
        if atom in parents or atom in _JSON_SCALARS or issubclass(atom, enum.Enum) or hasattr(atom, "model_validate"):
            return True
        if dataclasses.is_dataclass(atom):
            hints = typing.get_type_hints(atom)
            return all(_storable(normalize(hints[f.name]), parents | {atom}) for f in dataclasses.fields(atom))
        return False
    if typing.get_origin(atom) is typing.Literal:
        return True
    return all(_storable(normalize(arg), parents) for arg in typing.get_args(atom) if arg is not Ellipsis)


def describe(obj: Any) -> str:
    return str(getattr(obj, "__qualname__", None) or repr(obj))


def ref(obj: Any) -> str:
    """`module:qualname`, the framework-independent name of a function or class."""
    return f"{obj.__module__}:{obj.__qualname__}"


def resolve(text: str) -> Any:
    module_name, _, qualname = text.partition(":")
    obj: Any = sys.modules.get(module_name) or importlib.import_module(module_name)
    for part in qualname.split("."):
        obj = getattr(obj, part)
    return obj


@dataclasses.dataclass(frozen=True)
class Param:
    name: str
    ty: Ty
    optional: bool
    kind: inspect._ParameterKind


class CallMode(enum.Enum):
    SYNC = "sync"
    ASYNC = "async"
    GENERATOR = "generator"
    ASYNC_GENERATOR = "async-generator"


@dataclasses.dataclass(frozen=True)
class Sig:
    params: tuple[Param, ...]
    returns: Ty
    mode: CallMode
    reference: str

    @property
    def is_async(self) -> bool:
        return self.mode is CallMode.ASYNC

    def text(self) -> str:
        params = ", ".join(f"{p.name}: {p.ty.text(full=True)}" for p in self.params)
        return f"({params}) -> {self.returns.text(full=True)}"


class Signatures:
    """Callable metadata shared across validation, snapshot serialization and execution."""

    def __init__(self, localns: dict[str, Any] | None = None) -> None:
        self._entries: dict[tuple[int, bool], Sig] = {}
        self._localns = localns

    def get(self, fn: Any, *, provider: bool = False) -> Sig:
        key = (id(fn), provider)
        if key not in self._entries:
            self._entries[key] = _inspect_signature(fn, provider, self._localns)
        return self._entries[key]


def _read_hints(fn: Any, localns: dict[str, Any] | None) -> dict[str, Any]:
    try:
        return typing.get_type_hints(fn)
    except NameError:
        if localns is None:
            raise
        return typing.get_type_hints(fn, localns=localns)


def _inspect_signature(fn: Any, provider: bool, localns: dict[str, Any] | None) -> Sig:
    if not callable(fn) or isinstance(fn, type):
        raise SigError("not-callable", f"expected a function, got {fn!r}")
    if getattr(fn, "__name__", None) == "<lambda>":
        raise SigError("bad-callable", "step and provider functions must be named functions, not lambdas")
    try:
        sig = inspect.signature(fn)
        hints = _read_hints(fn, localns)
    except Exception as exc:
        raise SigError("bad-annotation", f"cannot read the annotations of {describe(fn)}: {exc}") from exc
    params: list[Param] = []
    for p in sig.parameters.values():
        if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            raise SigError("bad-signature", f"{describe(fn)} takes *{p.name}; every input must be a named parameter")
        if p.name not in hints:
            raise SigError("missing-annotation", f"parameter '{p.name}' of {describe(fn)} has no type annotation")
        params.append(Param(p.name, _normalize(fn, hints[p.name]), p.default is not p.empty, p.kind))
    if "return" not in hints:
        raise SigError("missing-annotation", f"{describe(fn)} has no return annotation")
    result = hints["return"]
    if inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
        mode = CallMode.ASYNC_GENERATOR if inspect.isasyncgenfunction(fn) else CallMode.GENERATOR
        if not provider:
            raise SigError("bad-signature", "only a worker provider may be a generator")
        origins = (
            (collections.abc.AsyncIterator, collections.abc.AsyncGenerator)
            if inspect.isasyncgenfunction(fn)
            else (collections.abc.Iterator, collections.abc.Generator)
        )
        args = typing.get_args(result)
        if typing.get_origin(result) not in origins or not args:
            raise SigError("bad-provider", "a generator provider must annotate the type it yields")
        result = args[0]
    elif inspect.iscoroutinefunction(fn):
        mode = CallMode.ASYNC
    else:
        awaits_result, result = awaited_annotation(result)
        mode = CallMode.ASYNC if awaits_result else CallMode.SYNC
    return Sig(tuple(params), _normalize(fn, result), mode, ref(fn))


def _normalize(fn: Any, ann: Any) -> Ty:
    try:
        return normalize(ann)
    except Unsupported as exc:
        raise SigError("unsupported-type", f"{describe(fn)}: {exc}") from exc
