"""Typed DAG wiring: plain business functions connected with explicit `Ref[T]` handles."""

from __future__ import annotations

import hashlib
import inspect
import json
import math
import typing
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from types import GenericAlias, MappingProxyType
from typing import TYPE_CHECKING, Any, Generic, Literal, NewType, NoReturn, TypeVar, cast, overload

from spater.context import Context

from ._types import (
    CallMode,
    ListOf,
    NoneType,
    Sig,
    SigError,
    Signatures,
    Ty,
    Unsupported,
    assignable,
    describe,
    normalize,
    plain_class,
    serializable,
)

if TYPE_CHECKING:
    from ._graph import Graph

T_co = TypeVar("T_co", covariant=True)
InT = TypeVar("InT")
OutT = TypeVar("OutT")
ItemT = TypeVar("ItemT")
ChildOutT = TypeVar("ChildOutT")
R = TypeVar("R")
C = TypeVar("C")

DuplicatePolicy = Literal["fail", "use_existing"]

_STORABLE = "use a dataclass, a pydantic model, an Enum or a JSON scalar"
_TERMINAL = object()


class RunExistsError(ValueError):
    """A run with the requested ID already exists."""


@dataclass(frozen=True)
class PerWorker:
    """Build a dependency once per worker. Generator providers can also tear it down."""

    fn: Callable[[], Any]


@dataclass(frozen=True)
class PerStep:
    """Adapt the current step context to an application-owned dependency."""

    fn: Callable[[Context], Any]


@dataclass(frozen=True)
class Retry:
    """Retry a step on matching raised exceptions or returned domain outcomes."""

    on: type[Any] | tuple[type[Any], ...]
    max: int
    after: timedelta
    backoff: float = 1.0

    def targets(self) -> tuple[type[Any], ...]:
        if isinstance(self.on, tuple):
            return self.on
        return (self.on,)


@dataclass(frozen=True)
class Problem:
    code: str
    where: str
    message: str

    def __str__(self) -> str:
        return f"[{self.code}] {self.where}: {self.message}"


class FlowError(Exception):
    """A job definition broke one or more invariants. All of them are listed."""

    def __init__(self, job: str, problems: Sequence[Problem]) -> None:
        self.job = job
        self.problems = tuple(problems)
        lines = "\n".join(f"  {p}" for p in self.problems)
        super().__init__(f"job {job!r} is invalid ({len(self.problems)} problem(s))\n{lines}")

    @property
    def codes(self) -> set[str]:
        return {p.code for p in self.problems}


@dataclass(frozen=True)
class Ref(Generic[T_co]):
    """Symbolic typed handle to a value produced by a job input, step, map or branch."""

    _token: object = field(repr=False, compare=False)
    _scope: int = field(repr=False)
    _source: str = field(repr=False)
    _ty: Ty = field(repr=False)
    _label: str = field(repr=False)

    def __repr__(self) -> str:
        return f"Ref({self._label}: {self._ty})"


After = Ref[Any] | Sequence[Ref[Any]] | None


@dataclass(frozen=True)
class _StepNode:
    id: str
    fn: Callable[..., Any]
    bound: tuple[tuple[str, Ref[Any]], ...]
    after: tuple[str, ...]
    deps: tuple[str, ...]
    retry: Retry | None
    timeout: timedelta | None
    catch: tuple[tuple[type[Exception], Callable[..., Any]], ...]
    returns: Ty
    meta: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))


@dataclass(frozen=True)
class _MapNode:
    id: str
    job: Job[Any, Any]
    items: Ref[Any]
    max_parallel: int | None
    after: tuple[str, ...]
    deps: tuple[str, ...]
    returns: Ty


@dataclass(frozen=True)
class _SleepNode:
    id: str
    duration: timedelta | Callable[..., timedelta]
    bound: tuple[tuple[str, Ref[Any]], ...]
    after: tuple[str, ...]
    deps: tuple[str, ...]


@dataclass(frozen=True)
class _FailNode:
    id: str
    reason: str
    after: tuple[str, ...]
    deps: tuple[str, ...]


@dataclass(frozen=True)
class _BranchNode:
    id: str
    subject: Ref[Any]
    arms: Mapping[type[Any], _Scope]
    after: tuple[str, ...]
    deps: tuple[str, ...]
    returns: Ty | None


_Node = _StepNode | _MapNode | _SleepNode | _FailNode | _BranchNode


@dataclass(frozen=True)
class _Scope:
    scope_id: int
    nodes: tuple[_Node, ...]
    output: Ref[Any] | None
    terminated: bool


class _BuildState:
    def __init__(self, name: str, provides: Sequence[PerWorker | PerStep], localns: dict[str, Any] | None) -> None:
        self.token = object()
        self.name = name
        self.signatures = Signatures(localns=localns)
        self.problems: list[Problem] = []
        self.names: set[str] = set()
        self.providers: set[Ty] = set()
        self.provider_entries: tuple[PerWorker | PerStep, ...] = ()
        self._scope_seq = 0
        self._fail_seq = 0
        self._check_providers(provides)

    def add(self, code: str, where: str, message: str) -> None:
        self.problems.append(Problem(code, where, message))

    def next_scope(self) -> int:
        self._scope_seq += 1
        return self._scope_seq

    def next_fail_name(self) -> str:
        while True:
            self._fail_seq += 1
            candidate = f"fail_{self._fail_seq}"
            if candidate not in self.names:
                self.names.add(candidate)
                return candidate

    def register_name(self, name: Any, fallback: str) -> str:
        chosen = fallback if name is None else name
        if not isinstance(chosen, str) or not chosen:
            self.add("bad-option", fallback, f"name must be a non-empty string, got {chosen!r}")
            return fallback
        if chosen in ("start", "done") or chosen.startswith("$"):
            self.add("bad-option", chosen, f"node name {chosen!r} is reserved")
            return chosen
        if chosen in self.names:
            self.add("duplicate-name", chosen, "two nodes share this name; pass name= to one of them")
        self.names.add(chosen)
        return chosen

    def sig(self, fn: Any, where: str, *, provider: bool = False) -> Sig | None:
        try:
            return self.signatures.get(fn, provider=provider)
        except SigError as exc:
            self.add(exc.code, where, exc.message)
            return None

    def positive(self, value: Any, where: str, option: str) -> None:
        if not isinstance(value, timedelta) or value <= timedelta(0):
            self.add("bad-option", where, f"{option} must be a positive timedelta, got {value!r}")

    def _check_providers(self, provides: Sequence[PerWorker | PerStep]) -> None:
        if not isinstance(provides, (list, tuple)):
            self.add("bad-provider", "job", "provides must be a sequence of PerWorker or PerStep")
            return
        entries: list[PerWorker | PerStep] = []
        for provider in provides:
            if not isinstance(provider, (PerWorker, PerStep)):
                self.add("bad-provider", "job", "providers must be PerWorker or PerStep")
                continue
            entries.append(provider)
            where = describe(provider.fn)
            sig = self.sig(provider.fn, where, provider=True)
            if sig is None:
                continue
            if isinstance(provider, PerWorker):
                if sig.params:
                    self.add("bad-provider", where, "a per-worker provider must take no parameters")
            elif (
                len(sig.params) != 1
                or sig.params[0].ty != Ty.of(Context)
                or sig.params[0].kind == inspect.Parameter.KEYWORD_ONLY
            ):
                self.add("bad-provider", where, "a per-step provider must take a single Context")
            elif sig.mode in (CallMode.GENERATOR, CallMode.ASYNC_GENERATOR):
                self.add("bad-provider", where, "a per-step provider must return its value directly")
            if len(sig.returns.alts) != 1 or sig.returns == Ty.of(NoneType):
                self.add("bad-provider", where, "a provider must supply a single type")
            elif not any(plain_class(alt) or isinstance(alt, NewType) for alt in sig.returns.alts):
                self.add("bad-provider", where, "a provider must supply a class or NewType")
            elif sig.returns in self.providers:
                self.add("bad-provider", where, f"a second provider also supplies {sig.returns}")
            else:
                self.providers.add(sig.returns)
        self.provider_entries = tuple(entries)


class Builder:
    """Record nodes and explicit `Ref` edges while defining a job or branch arm."""

    def __init__(self, state: _BuildState, scope_id: int, ancestors: frozenset[int]) -> None:
        self._state = state
        self._scope_id = scope_id
        self._visible_scopes = ancestors | {scope_id}
        self._nodes: list[_Node] = []
        self._terminated = False

    def _ref(self, source: str, ty: Ty, label: str) -> Ref[Any]:
        return Ref(self._state.token, self._scope_id, source, ty, label)

    def _check_reachable(self, where: str) -> None:
        if self._terminated:
            self._state.add("unreachable", where, "the previous node never continues")

    def _check_ref(self, value: Any, where: str, role: str) -> Ref[Any] | None:
        if not isinstance(value, Ref):
            self._state.add("bad-input", where, f"{role} must be a Ref, got {value!r}")
            return None
        if value._token is not self._state.token or value._scope not in self._visible_scopes:
            self._state.add("ref-out-of-scope", where, f"{role} ({value._label}) is not visible in this scope")
            return None
        return value

    def _collect_after(self, after: After, where: str) -> tuple[tuple[str, ...], list[str]]:
        if after is None:
            return (), []
        refs: Sequence[Any] = (after,) if isinstance(after, Ref) else after
        if not isinstance(refs, (list, tuple)):
            self._state.add("bad-option", where, "after must be a Ref or sequence of Refs")
            return (), []
        after_ids: list[str] = []
        deps: list[str] = []
        for item in refs:
            checked = self._check_ref(item, where, "after")
            if checked is None:
                continue
            if checked._source not in ("$input", "$case"):
                after_ids.append(checked._source)
                if checked._scope == self._scope_id:
                    deps.append(checked._source)
        return tuple(dict.fromkeys(after_ids)), deps

    def _bind_call(
        self,
        sig: Sig,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any] | None,
        where: str,
        *,
        allow_providers: bool,
        pure_error_code: str = "bad-duration",
    ) -> tuple[tuple[tuple[str, Ref[Any]], ...], list[str]]:
        if kwargs is not None and not isinstance(kwargs, Mapping):
            self._state.add("bad-input", where, "kwargs must map parameter names to Refs")
            kwargs = {}
        kw = dict(kwargs) if kwargs is not None else {}
        bound: dict[str, Ref[Any]] = {}
        deps: list[str] = []

        positional_params = [p for p in sig.params if p.kind != inspect.Parameter.KEYWORD_ONLY]
        if len(args) > len(positional_params):
            self._state.add(
                "bad-input",
                where,
                f"expected at most {len(positional_params)} positional argument(s), got {len(args)}",
            )
        for param, raw in zip(positional_params, args):
            checked = self._check_ref(raw, where, f"parameter '{param.name}'")
            if checked is not None:
                bound[param.name] = checked
                if checked._scope == self._scope_id and checked._source not in ("$input", "$case"):
                    deps.append(checked._source)

        by_name = {p.name: p for p in sig.params}
        for name, raw in kw.items():
            if name not in by_name:
                self._state.add("bad-input", where, f"unknown parameter {name!r}")
                continue
            param = by_name[name]
            if param.kind == inspect.Parameter.POSITIONAL_ONLY:
                self._state.add("bad-input", where, f"parameter {name!r} is positional-only")
                continue
            if name in bound:
                self._state.add("bad-input", where, f"multiple values for parameter {name!r}")
                continue
            checked = self._check_ref(raw, where, f"parameter '{name}'")
            if checked is not None:
                bound[name] = checked
                if checked._scope == self._scope_id and checked._source not in ("$input", "$case"):
                    deps.append(checked._source)

        for param in sig.params:
            label = f"parameter '{param.name}: {param.ty}'"
            if param.name in bound:
                ref_arg = bound[param.name]
                if not assignable(ref_arg._ty, param.ty):
                    self._state.add("type-mismatch", where, f"{label} cannot accept {ref_arg._label} ({ref_arg._ty})")
                continue
            matching_providers = [ty for ty in self._state.providers if assignable(ty, param.ty)]
            if allow_providers:
                if len(matching_providers) == 1:
                    continue
                if len(matching_providers) > 1:
                    names = ", ".join(sorted(str(ty) for ty in matching_providers))
                    self._state.add("ambiguous-input", where, f"{label} matches multiple providers: {names}")
                    continue
            elif matching_providers and not param.optional:
                self._state.add(pure_error_code, where, f"{label} requires a provider; this function must be pure")
                continue
            if not param.optional:
                self._state.add("missing-input", where, f"{label} was not supplied and has no matching provider")

        return tuple(bound.items()), deps

    @overload
    def step(
        self,
        fn: Callable[..., Awaitable[R]],
        *args: Ref[Any],
        kwargs: Mapping[str, Ref[Any]] | None = None,
        name: str | None = None,
        retry: Retry | None = None,
        timeout: timedelta | None = None,
        after: After = None,
        meta: Mapping[str, Any] | None = None,
    ) -> Ref[R]: ...

    @overload
    def step(
        self,
        fn: Callable[..., R],
        *args: Ref[Any],
        kwargs: Mapping[str, Ref[Any]] | None = None,
        name: str | None = None,
        retry: Retry | None = None,
        timeout: timedelta | None = None,
        after: After = None,
        meta: Mapping[str, Any] | None = None,
    ) -> Ref[R]: ...

    @overload
    def step(
        self,
        fn: Callable[..., Awaitable[R]],
        *args: Ref[Any],
        kwargs: Mapping[str, Ref[Any]] | None = None,
        name: str | None = None,
        retry: Retry | None = None,
        timeout: timedelta | None = None,
        catch: Mapping[type[Exception], Callable[..., C]],
        after: After = None,
        meta: Mapping[str, Any] | None = None,
    ) -> Ref[R | C]: ...

    @overload
    def step(
        self,
        fn: Callable[..., R],
        *args: Ref[Any],
        kwargs: Mapping[str, Ref[Any]] | None = None,
        name: str | None = None,
        retry: Retry | None = None,
        timeout: timedelta | None = None,
        catch: Mapping[type[Exception], Callable[..., C]],
        after: After = None,
        meta: Mapping[str, Any] | None = None,
    ) -> Ref[R | C]: ...

    def step(
        self,
        fn: Callable[..., Any],
        *args: Ref[Any],
        kwargs: Mapping[str, Ref[Any]] | None = None,
        name: str | None = None,
        retry: Retry | None = None,
        timeout: timedelta | None = None,
        catch: Mapping[type[Exception], Callable[..., Any]] | None = None,
        after: After = None,
        meta: Mapping[str, Any] | None = None,
    ) -> Ref[Any]:
        """Run a plain business function as a checkpointed DAG step."""
        fallback = str(getattr(fn, "__name__", "step"))
        node_id = self._state.register_name(name, fallback)
        self._check_reachable(node_id)
        if timeout is not None:
            self._state.positive(timeout, node_id, "timeout")
        frozen_meta = self._check_meta(meta, node_id)
        sig = self._state.sig(fn, node_id)
        bound: tuple[tuple[str, Ref[Any]], ...] = ()
        arg_deps: list[str] = []
        if sig is not None:
            bound, arg_deps = self._bind_call(sig, args, kwargs, node_id, allow_providers=True)
        after_ids, after_deps = self._collect_after(after, node_id)
        deps = tuple(dict.fromkeys((*arg_deps, *after_deps)))

        caught_entries: list[tuple[type[Exception], Callable[..., Any]]] = []
        return_alts: set[Any] = set(sig.returns.alts) if sig is not None else {NoneType}
        if catch is not None:
            if not isinstance(catch, Mapping) or not catch:
                self._state.add("bad-catch", node_id, "catch must be a non-empty mapping of Exception to converter")
            else:
                for exc_cls, converter in catch.items():
                    if not plain_class(exc_cls) or not issubclass(exc_cls, Exception):
                        self._state.add("bad-catch", node_id, f"catch key {exc_cls!r} must be an Exception class")
                        continue
                    conv_sig = self._state.sig(converter, node_id)
                    if conv_sig is None:
                        continue
                    if (
                        conv_sig.is_async
                        or len(conv_sig.params) != 1
                        or not assignable(Ty.of(exc_cls), conv_sig.params[0].ty)
                        or conv_sig.params[0].kind == inspect.Parameter.KEYWORD_ONLY
                    ):
                        self._state.add(
                            "bad-catch",
                            node_id,
                            f"converter for {describe(exc_cls)} must be sync and take one {describe(exc_cls)}",
                        )
                        continue
                    return_alts |= conv_sig.returns.alts
                    caught_entries.append((exc_cls, converter))

        returns = Ty(frozenset(return_alts))
        if returns != Ty.of(NoneType) and not serializable(returns):
            self._state.add("not-serializable", node_id, f"result {returns} cannot be stored; {_STORABLE}")
        if retry is not None:
            self._check_retry(retry, returns, node_id)

        self._nodes.append(
            _StepNode(
                id=node_id,
                fn=fn,
                bound=bound,
                after=after_ids,
                deps=deps,
                retry=retry,
                timeout=timeout,
                catch=tuple(caught_entries),
                returns=returns,
                meta=frozen_meta,
            )
        )
        return self._ref(node_id, returns, node_id)

    def _check_meta(self, meta: Mapping[str, Any] | None, where: str) -> Mapping[str, Any]:
        if meta is None:
            return MappingProxyType({})
        if not isinstance(meta, Mapping) or not all(isinstance(k, str) for k in meta):
            self._state.add("bad-option", where, "meta must be a mapping with string keys")
            return MappingProxyType({})
        try:
            copied = json.loads(json.dumps(dict(meta), allow_nan=False))
        except (TypeError, ValueError):
            self._state.add("bad-option", where, "meta values must be JSON-serializable")
            return MappingProxyType({})
        return MappingProxyType(copied)

    def _check_retry(self, retry: Retry, returns: Ty, where: str) -> None:
        if not isinstance(retry, Retry):
            self._state.add("bad-retry", where, "retry must be a Retry policy")
            return
        targets = retry.targets()
        if not targets:
            self._state.add("bad-retry", where, "retry.on must specify at least one class")
        for target in targets:
            if not plain_class(target):
                self._state.add("bad-retry", where, f"retry.on entry {target!r} must be a class")
                continue
            if not issubclass(target, Exception) and target not in returns.alts:
                self._state.add(
                    "bad-retry",
                    where,
                    f"retry.on ({describe(target)}) must be an Exception class or a step return type",
                )
        if type(retry.max) is not int or retry.max < 1:
            self._state.add("bad-option", where, "retry.max must be a positive integer")
        self._state.positive(retry.after, where, "retry.after")
        if (
            type(retry.backoff) not in (int, float)
            or retry.backoff < 1
            or (isinstance(retry.backoff, float) and not math.isfinite(retry.backoff))
        ):
            self._state.add("bad-option", where, "retry.backoff must be a finite number at least 1")

    @overload
    def map(
        self,
        job: Job[ItemT, None],
        items: Ref[list[ItemT]],
        *,
        max_parallel: int | None = None,
        name: str | None = None,
        after: After = None,
    ) -> Ref[None]: ...

    @overload
    def map(
        self,
        job: Job[ItemT, ChildOutT],
        items: Ref[list[ItemT]],
        *,
        max_parallel: int | None = None,
        name: str | None = None,
        after: After = None,
    ) -> Ref[list[ChildOutT]]: ...

    def map(
        self,
        job: Job[Any, Any],
        items: Ref[Any],
        *,
        max_parallel: int | None = None,
        name: str | None = None,
        after: After = None,
    ) -> Ref[Any]:
        """Run one child invocation of `job` per element of `items` and join outputs in order."""
        fallback = f"map_{job.name}" if isinstance(job, Job) else "map"
        node_id = self._state.register_name(name, fallback)
        self._check_reachable(node_id)
        if max_parallel is not None and (type(max_parallel) is not int or max_parallel < 1):
            self._state.add("bad-option", node_id, f"max_parallel must be 1 or more, got {max_parallel!r}")
        if not isinstance(job, Job):
            self._state.add("bad-option", node_id, f"map needs a Job to run per item, got {job!r}")
            return self._ref(node_id, Ty.of(NoneType), node_id)
        checked_items = self._check_ref(items, node_id, "items")
        after_ids, after_deps = self._collect_after(after, node_id)
        deps: list[str] = []
        if checked_items is not None:
            if checked_items._scope == self._scope_id and checked_items._source not in ("$input", "$case"):
                deps.append(checked_items._source)
            atom = next(iter(checked_items._ty.alts)) if len(checked_items._ty.alts) == 1 else None
            if not isinstance(atom, ListOf):
                self._state.add("map-items-not-list", node_id, f"items must be list[T], not {checked_items._ty}")
            elif job.input is None or not assignable(atom.item, normalize(job.input)):
                self._state.add(
                    "map-item-mismatch", node_id, f"items are {atom.item} but child takes {describe(job.input)}"
                )
        deps.extend(after_deps)
        returns = Ty.of(NoneType) if job.output is None else Ty.of(ListOf(normalize(job.output)))
        if checked_items is not None:
            self._nodes.append(
                _MapNode(
                    id=node_id,
                    job=job,
                    items=checked_items,
                    max_parallel=max_parallel,
                    after=after_ids,
                    deps=tuple(dict.fromkeys(deps)),
                    returns=returns,
                )
            )
        return self._ref(node_id, returns, node_id)

    def sleep(
        self,
        duration: timedelta | Callable[..., timedelta],
        *args: Ref[Any],
        kwargs: Mapping[str, Ref[Any]] | None = None,
        name: str | None = None,
        after: After = None,
    ) -> Ref[None]:
        """Park the branch until a fixed or computed duration has elapsed."""
        node_id = self._state.register_name(name, "sleep")
        self._check_reachable(node_id)
        bound: tuple[tuple[str, Ref[Any]], ...] = ()
        arg_deps: list[str] = []
        if isinstance(duration, timedelta):
            self._state.positive(duration, node_id, "duration")
            if args or kwargs:
                self._state.add("bad-duration", node_id, "fixed timedelta sleep does not accept arguments")
        elif callable(duration):
            sig = self._state.sig(duration, node_id)
            if sig is not None:
                if sig.is_async or sig.returns != Ty.of(timedelta):
                    self._state.add("bad-duration", node_id, "a duration function must be sync and return timedelta")
                bound, arg_deps = self._bind_call(
                    sig, args, kwargs, node_id, allow_providers=False, pure_error_code="bad-duration"
                )
        else:
            self._state.add("bad-duration", node_id, "expected a timedelta or a named duration function")
        after_ids, after_deps = self._collect_after(after, node_id)
        deps = tuple(dict.fromkeys((*arg_deps, *after_deps)))
        self._nodes.append(_SleepNode(id=node_id, duration=duration, bound=bound, after=after_ids, deps=deps))
        return self._ref(node_id, Ty.of(NoneType), node_id)

    def fail(self, reason: str, *, name: str | None = None, after: After = None) -> NoReturn:
        """End the run on this path as failed."""
        node_id = self._state.next_fail_name() if name is None else self._state.register_name(name, "fail")
        self._check_reachable(node_id)
        if not isinstance(reason, str) or not reason:
            self._state.add("bad-option", node_id, "reason must be a non-empty string")
        after_ids, after_deps = self._collect_after(after, node_id)
        self._nodes.append(_FailNode(id=node_id, reason=reason, after=after_ids, deps=tuple(dict.fromkeys(after_deps))))
        self._terminated = True
        if not TYPE_CHECKING:
            return _TERMINAL
        raise AssertionError("unreachable")

    @overload
    def branch(
        self,
        subject: Ref[Any],
        cases: Mapping[type[Any], Callable[[Builder, Ref[Any]], Ref[R]]],
        *,
        name: str | None = None,
        after: After = None,
    ) -> Ref[R]: ...

    @overload
    def branch(
        self,
        subject: Ref[Any],
        cases: Mapping[type[Any], Callable[[Builder, Ref[Any]], None]],
        *,
        name: str | None = None,
        after: After = None,
    ) -> Ref[None]: ...

    def branch(
        self,
        subject: Ref[Any],
        cases: Mapping[type[Any], Callable[[Builder, Ref[Any]], Any]],
        *,
        name: str | None = None,
        after: After = None,
    ) -> Ref[Any]:
        """Exhaustively route on the runtime class of `subject`."""
        fallback = f"branch_{subject._label}" if isinstance(subject, Ref) else "branch"
        node_id = self._state.register_name(name, fallback)
        self._check_reachable(node_id)
        checked = self._check_ref(subject, node_id, "subject")
        after_ids, after_deps = self._collect_after(after, node_id)
        if not isinstance(cases, Mapping) or not cases:
            self._state.add("bad-branch", node_id, "cases must be a non-empty mapping of type to arm builder")
            return self._ref(node_id, Ty.of(NoneType), node_id)

        outcomes: set[type[Any]] | None = None
        if checked is not None:
            alts = list(checked._ty.alts)
            if not all(plain_class(a) for a in alts):
                self._state.add(
                    "branch-bad-outcome", node_id, f"cannot branch on {checked._ty}; outcomes must be distinct classes"
                )
            else:
                for i, a in enumerate(alts):
                    for b in alts[i + 1 :]:
                        if assignable(Ty.of(a), Ty.of(b)) or assignable(Ty.of(b), Ty.of(a)):
                            self._state.add("branch-overlap", node_id, f"{describe(a)} and {describe(b)} overlap")
                outcomes = set(alts)
                if missing := outcomes - set(cases):
                    self._state.add("branch-missing-case", node_id, f"no case for {_classes(missing)}")
                if extra := set(cases) - outcomes:
                    self._state.add("branch-extra-case", node_id, f"{_classes(extra)} can never happen here")

        arms: dict[type[Any], _Scope] = {}
        surviving_outputs: list[Ref[Any] | None] = []
        for case_cls, arm_fn in cases.items():
            if outcomes is not None and case_cls not in outcomes:
                continue
            if not callable(arm_fn):
                self._state.add("bad-branch", node_id, f"case {describe(case_cls)} handler must be callable")
                continue
            arm_scope_id = self._state.next_scope()
            arm_builder = Builder(self._state, arm_scope_id, self._visible_scopes)
            case_ty = Ty.of(case_cls) if plain_class(case_cls) else Ty.of(NoneType)
            case_ref = arm_builder._ref("$case", case_ty, describe(case_cls))
            result = arm_fn(arm_builder, case_ref)
            if arm_builder._terminated:
                if result is not _TERMINAL and result is not None:
                    self._state.add("unreachable", node_id, f"failed case {describe(case_cls)} should not return a Ref")
                arm_output: Ref[Any] | None = None
            elif result is None:
                arm_output = None
                surviving_outputs.append(None)
            else:
                checked_out = arm_builder._check_ref(result, node_id, f"case {describe(case_cls)} return value")
                arm_output = checked_out
                surviving_outputs.append(checked_out)
            arms[case_cls] = _Scope(arm_scope_id, tuple(arm_builder._nodes), arm_output, arm_builder._terminated)

        deps: list[str] = []
        if checked is not None and checked._scope == self._scope_id and checked._source not in ("$input", "$case"):
            deps.append(checked._source)
        deps.extend(after_deps)

        if not surviving_outputs:
            self._terminated = True
            output_ty: Ty | None = None
        else:
            has_ref = [o for o in surviving_outputs if o is not None]
            has_none = any(o is None for o in surviving_outputs)
            if has_ref and has_none:
                self._state.add(
                    "branch-return-mismatch",
                    node_id,
                    "surviving branch arms must either all return a Ref or all return None",
                )
                output_ty = Ty.of(NoneType)
            elif has_ref:
                alts_union: set[Any] = set()
                for out in has_ref:
                    alts_union |= out._ty.alts
                output_ty = Ty(frozenset(alts_union))
            else:
                output_ty = Ty.of(NoneType)

        if checked is not None:
            self._nodes.append(
                _BranchNode(
                    id=node_id,
                    subject=checked,
                    arms=MappingProxyType(arms),
                    after=after_ids,
                    deps=tuple(dict.fromkeys(deps)),
                    returns=output_ty,
                )
            )
        return self._ref(node_id, output_ty or Ty.of(NoneType), node_id)


def _classes(classes: set[Any]) -> str:
    return ", ".join(sorted(describe(c) for c in classes))


def _unwrap_ref_annotation(ann: Any, where: str, role: str, state: _BuildState) -> tuple[Any, Ty | None]:
    origin = typing.get_origin(ann)
    if origin is not Ref:
        state.add("bad-annotation", where, f"{role} must be annotated as Ref[T], got {ann!r}")
        return None, None
    args = typing.get_args(ann)
    if len(args) != 1:
        state.add("bad-annotation", where, f"{role} must be annotated as Ref[T]")
        return None, None
    inner = args[0]
    try:
        ty = normalize(inner)
    except Unsupported as exc:
        state.add("bad-option", where, f"invalid {role} type: {exc}")
        return None, None
    if not serializable(ty):
        state.add("not-serializable", where, f"{role} {ty} cannot be stored; {_STORABLE}")
        return None, None
    return inner, ty


class _JobDecorator:
    def __init__(self, name: str, provides: Sequence[PerWorker | PerStep]) -> None:
        self._name = name
        self._provides = provides

    @overload
    def __call__(self, fn: Callable[[Builder, Ref[InT]], Ref[OutT]]) -> Job[InT, OutT]: ...

    @overload
    def __call__(self, fn: Callable[[Builder, Ref[InT]], None]) -> Job[InT, None]: ...

    @overload
    def __call__(self, fn: Callable[[Builder], Ref[OutT]]) -> Job[None, OutT]: ...

    @overload
    def __call__(self, fn: Callable[[Builder], None]) -> Job[None, None]: ...

    def __call__(self, fn: Callable[..., Any]) -> Job[Any, Any]:
        frame = inspect.currentframe()
        localns = dict(frame.f_back.f_locals) if frame is not None and frame.f_back is not None else None
        return _build_job(self._name, fn, self._provides, localns)


def _build_job(
    name: str, fn: Callable[..., Any], provides: Sequence[PerWorker | PerStep], localns: dict[str, Any] | None
) -> Job[Any, Any]:
    state = _BuildState(name, provides, localns)
    if not isinstance(name, str) or not name:
        state.add("bad-option", "job", "name must be a non-empty string")
    if not callable(fn) or isinstance(fn, type):
        state.add("not-callable", "job", f"job builder must be a function, got {fn!r}")
        raise FlowError(str(name), state.problems)

    try:
        sig = inspect.signature(fn)
        try:
            hints = typing.get_type_hints(fn)
        except NameError:
            hints = typing.get_type_hints(fn, localns=localns) if localns is not None else {}
            if not hints:
                raise
    except Exception as exc:
        state.add("bad-annotation", "job", f"cannot read annotations of {describe(fn)}: {exc}")
        raise FlowError(str(name), state.problems) from exc

    params = list(sig.parameters.values())
    if len(params) not in (1, 2):
        state.add("bad-signature", "job", "a job builder must take (builder) or (builder, input: Ref[T])")
        raise FlowError(str(name), state.problems)

    builder = Builder(state, 0, frozenset())
    input_ann: Any = None
    input_ref: Ref[Any] | None = None
    if len(params) == 2:
        p = params[1]
        if p.name not in hints:
            state.add("missing-annotation", "job", f"input parameter '{p.name}' must be annotated as Ref[T]")
            raise FlowError(str(name), state.problems)
        input_ann, input_ty = _unwrap_ref_annotation(hints[p.name], "job", "input", state)
        if input_ty is not None:
            input_ref = builder._ref("$input", input_ty, p.name)
        else:
            raise FlowError(str(name), state.problems)

    result = fn(builder) if input_ref is None else fn(builder, input_ref)

    if builder._terminated:
        state.add("never-succeeds", "job", "no path reaches the end of the flow; every run would fail")

    if not builder._nodes:
        state.add("bad-option", "job", "job must contain at least one step or node")

    output_ann: Any = None
    output_ref: Ref[Any] | None = None
    ret_hint = hints.get("return")
    annotated_out_ann: Any = None
    annotated_out_ty: Ty | None = None
    if ret_hint is not None and ret_hint is not NoneType:
        annotated_out_ann, annotated_out_ty = _unwrap_ref_annotation(ret_hint, "job", "output", state)

    if result is None or result is _TERMINAL:
        if annotated_out_ty is not None and annotated_out_ty != Ty.of(NoneType) and not builder._terminated:
            state.add("missing-output", "job", f"job is annotated to return Ref[{annotated_out_ty}] but returned None")
    else:
        output_ref = builder._check_ref(result, "job", "return value")
        if output_ref is not None:
            if not serializable(output_ref._ty):
                state.add("not-serializable", "job", f"output {output_ref._ty} cannot be stored; {_STORABLE}")
            if annotated_out_ty is not None:
                if not assignable(output_ref._ty, annotated_out_ty):
                    state.add(
                        "type-mismatch",
                        "job",
                        f"returned {output_ref._label} ({output_ref._ty}) does not fit declared output {annotated_out_ty}",
                    )
                output_ann = annotated_out_ann
            elif len(output_ref._ty.alts) == 1:
                atom = next(iter(output_ref._ty.alts))
                output_ann = None if atom is NoneType else _ty_to_annotation(output_ref._ty)
            else:
                output_ann = _ty_to_annotation(output_ref._ty)

    if state.problems:
        raise FlowError(str(name), state.problems)

    root = _Scope(0, tuple(builder._nodes), output_ref, builder._terminated)
    return Job(
        name=name,
        input=input_ann,
        output=output_ann,
        provides=state.provider_entries,
        _root=root,
        _signatures=state.signatures,
    )


def _ty_to_annotation(ty: Ty) -> Any:
    items = [_atom_to_annotation(a) for a in sorted(ty.alts, key=str)]
    if len(items) == 1:
        return items[0]
    acc: Any = items[0]
    for item in items[1:]:
        acc = acc | item
    return acc


def _atom_to_annotation(atom: Any) -> Any:
    if isinstance(atom, ListOf):
        return GenericAlias(list, (_ty_to_annotation(atom.item),))
    return atom


@dataclass(frozen=True)
class Job(Generic[InT, OutT]):
    """A validated workflow DAG definition. Build with `@Job.define(...)`."""

    name: str
    input: Any
    output: Any
    provides: tuple[PerWorker | PerStep, ...]
    _root: _Scope = field(repr=False, compare=False)
    _signatures: Signatures = field(repr=False, compare=False)
    _definition_json: str = field(init=False, repr=False, compare=False)
    _digest: str = field(init=False, repr=False, compare=False)
    _graph_cache: Graph | None = field(init=False, default=None, repr=False, compare=False)
    _graph_dict_cache: dict[str, Any] | None = field(init=False, default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        from ._json import job_to_dict

        definition = job_to_dict(self)
        encoded = json.dumps(definition, sort_keys=True)
        object.__setattr__(self, "_definition_json", encoded)
        object.__setattr__(self, "_digest", hashlib.sha256(encoded.encode()).hexdigest())

    @staticmethod
    def define(name: str, *, provides: Sequence[PerWorker | PerStep] = ()) -> _JobDecorator:
        """Define and validate a typed workflow DAG from a wiring function."""
        return _JobDecorator(name, provides)

    def to_dict(self) -> dict[str, Any]:
        return cast(dict[str, Any], json.loads(self._definition_json))

    def to_json(self, *, indent: int | None = None) -> str:
        if indent is None:
            return self._definition_json
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    def graph(self) -> Graph:
        """Nodes and edges, for drawing: `job.graph().to_mermaid()`."""
        if self._graph_cache is None:
            from ._graph import project

            object.__setattr__(self, "_graph_cache", project(self))
        assert self._graph_cache is not None
        return self._graph_cache

    def _graph_dict(self) -> dict[str, Any]:
        if self._graph_dict_cache is None:
            from dataclasses import asdict

            object.__setattr__(self, "_graph_dict_cache", asdict(self.graph()))
        assert self._graph_dict_cache is not None
        return self._graph_dict_cache

    @property
    def digest(self) -> str:
        """Full SHA-256 hex digest of the definition snapshot."""
        return self._digest

    @property
    def version(self) -> str:
        """Short hash of the definition's shape and signatures. Function bodies are not part of it."""
        return self._digest[:12]
