"""Execution seam separating DAG orchestration from how individual steps run."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Protocol

from spater.context import Context

from ._nodes import Job, Ref
from ._run_calls import Env, Providers, stored_inputs


@dataclass(frozen=True)
class StepCall:
    """One logical invocation handed from the DAG orchestrator to middleware and a `StepExecutor`."""

    job: Job[Any, Any]
    run_id: str
    node_id: str
    attempt: int
    fn: Callable[..., Any]
    bound: tuple[tuple[str, Ref[Any]], ...]
    env: Env
    allow_providers: bool
    deadline: float | None = None
    meta: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    @property
    def context(self) -> Context:
        """Runtime facts for this attempt. Retries and recovery reuse `idempotency_key`."""
        return Context(self.run_id, f"{self.run_id}/{self.node_id}")

    def inputs(self) -> dict[str, Any]:
        """Persisted JSON-shaped step inputs, excluding injected providers and defaults."""
        return stored_inputs(self.bound, self.env)


StepNext = Callable[[StepCall], Awaitable[Any]]
StepMiddleware = Callable[[StepCall, StepNext], Awaitable[Any]]
StepErrorHandler = Callable[[StepCall, BaseException], Awaitable[None] | None]


class StepExecutor(Protocol):
    """Pluggable execution backend for individual step and duration invocations.

    The default `LocalExecutor` runs callables in-process with `PerWorker` and
    `PerStep` providers. Custom executors can intercept `StepCall` (for example,
    to route steps carrying `call.meta` onto remote worker queues or specialized
    machines) while keeping DAG orchestration, retries, persistence and recovery
    unchanged.
    """

    async def execute(self, call: StepCall) -> Any: ...

    async def aclose(self) -> None: ...


class LocalExecutor:
    """Run step callables in-process using the job's `PerWorker` and `PerStep` providers."""

    def __init__(self) -> None:
        self._providers = Providers()

    async def execute(self, call: StepCall) -> Any:
        return await self._providers.call(
            call.fn,
            call.job,
            call.bound,
            call.env,
            call.context,
            allow_providers=call.allow_providers,
            deadline=call.deadline,
        )

    async def aclose(self) -> None:
        await self._providers.close()
