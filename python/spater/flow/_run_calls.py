"""Bind explicit `Ref` arguments and manage live provider lifetimes."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Callable, Mapping
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import Any

from spater.context import Context

from ._nodes import Job, PerStep, PerWorker, Ref
from ._run_tasks import settle
from ._run_values import decode
from ._types import Sig, Ty, assignable


@dataclass(frozen=True)
class Stored:
    ty: Ty
    encoded: Any


Env = Mapping[tuple[int, str], Stored]


def stored_inputs(bound: tuple[tuple[str, Ref[Any]], ...], env: Env) -> dict[str, Any]:
    """Only persisted workflow values. Runtime dependencies and defaults stay out of history."""
    return {param: env[(ref._scope, ref._source)].encoded["value"] for param, ref in bound}


async def call_sync(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    # Cancelling an asyncio wrapper does not interrupt a running Python thread.
    return await settle(asyncio.to_thread(fn, *args, **kwargs))


async def invoke(fn: Callable[..., Any], sig: Sig, args: list[Any], kwargs: dict[str, Any]) -> Any:
    if sig.is_async:
        return await fn(*args, **kwargs)
    return await call_sync(fn, *args, **kwargs)


class Providers:
    def __init__(self) -> None:
        self._stack = AsyncExitStack()
        self._worker_values: dict[Callable[..., Any], Any] = {}
        self._lock = asyncio.Lock()

    async def close(self) -> None:
        await self._stack.aclose()
        self._worker_values.clear()

    async def worker(self, provider: PerWorker, sig: Sig) -> Any:
        fn = provider.fn
        async with self._lock:
            if fn not in self._worker_values:
                if inspect.isasyncgenfunction(fn):
                    value = await self._stack.enter_async_context(asynccontextmanager(fn)())
                elif inspect.isgeneratorfunction(fn):
                    cm = contextmanager(fn)()
                    entering = asyncio.create_task(asyncio.to_thread(cm.__enter__))
                    try:
                        value = await asyncio.shield(entering)
                    except asyncio.CancelledError:
                        # A sync generator may acquire its resource after cancellation.
                        # Finish setup and close it before reporting cancellation.
                        async def release_after_enter() -> None:
                            await entering
                            await call_sync(cm.__exit__, None, None, None)

                        await settle(release_after_enter())
                        raise
                    self._stack.push_async_callback(call_sync, cm.__exit__, None, None, None)
                else:
                    value = await invoke(fn, sig, [], {})
                self._worker_values[fn] = value
            return self._worker_values[fn]

    async def call(
        self,
        fn: Callable[..., Any],
        job: Job[Any, Any],
        bound: tuple[tuple[str, Ref[Any]], ...],
        env: Env,
        context: Context,
        *,
        allow_providers: bool,
        deadline: float | None = None,
    ) -> Any:
        # asyncio.wait distinguishes a cancelled application awaitable from
        # cancellation of the owning DAG (including on Python 3.10).
        remaining = None if deadline is None else deadline - time.time()
        if remaining is not None and remaining <= 0:
            raise TimeoutError("step deadline elapsed")
        work = asyncio.create_task(self._call(fn, job, bound, env, context, allow_providers=allow_providers))
        try:
            done, _ = await asyncio.wait((work,), timeout=remaining)
            if not done or (deadline is not None and time.time() >= deadline):
                raise TimeoutError("step deadline elapsed")
        except (asyncio.CancelledError, TimeoutError):
            work.cancel()
            await settle(asyncio.gather(work, return_exceptions=True))
            raise
        if work.cancelled():
            raise RuntimeError("application call was cancelled")
        return work.result()

    async def _call(
        self,
        fn: Callable[..., Any],
        job: Job[Any, Any],
        bound: tuple[tuple[str, Ref[Any]], ...],
        env: Env,
        context: Context,
        *,
        allow_providers: bool,
    ) -> Any:
        sig = job._signatures.get(fn)
        bound_map = dict(bound)
        args: list[Any] = []
        kwargs: dict[str, Any] = {}
        dependencies: dict[PerWorker | PerStep, Any] = {}
        for param in sig.params:
            if param.name in bound_map:
                ref = bound_map[param.name]
                stored = env[(ref._scope, ref._source)]
                value = decode(stored.encoded, stored.ty)
            else:
                providers = (
                    [p for p in job.provides if assignable(job._signatures.get(p.fn, provider=True).returns, param.ty)]
                    if allow_providers
                    else []
                )
                if len(providers) == 1:
                    provider = providers[0]
                    if provider not in dependencies:
                        if isinstance(provider, PerWorker):
                            dependencies[provider] = await self.worker(
                                provider, job._signatures.get(provider.fn, provider=True)
                            )
                        else:
                            dependencies[provider] = await invoke(
                                provider.fn, job._signatures.get(provider.fn, provider=True), [context], {}
                            )
                    value = dependencies[provider]
                elif not providers and param.optional:
                    if param.kind is inspect.Parameter.POSITIONAL_ONLY:
                        args.append(inspect.signature(fn).parameters[param.name].default)
                    continue
                else:
                    reason = "ambiguous" if len(providers) > 1 else "missing"
                    raise ValueError(f"cannot bind {param.name!r}: {reason}")
            if param.kind is inspect.Parameter.POSITIONAL_ONLY:
                args.append(value)
            else:
                kwargs[param.name] = value
        return await invoke(fn, sig, args, kwargs)
