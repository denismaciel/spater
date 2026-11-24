"""Broker delivery for durable runs. Each delivery drives one run until it parks or finishes."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, TypeVar, overload
from uuid import NAMESPACE_URL, uuid4, uuid5

from spater._core import TaskInfo
from spater.app import Asynq
from spater.context import TaskContext
from spater.errors import HandlerNotFound, QueueNotFoundError, TaskIDConflictError, TaskNotFoundError
from spater.inspector import Inspector

from ._executor import StepErrorHandler, StepExecutor, StepMiddleware
from ._nodes import DuplicatePolicy, Job, RunExistsError
from ._run_store import ClaimLost, DeliveryWatch, RunStore, now
from ._run_values import decode
from ._runner import Parked, Runner
from ._types import normalize

InT = TypeVar("InT")


@dataclass(frozen=True)
class _Delivery:
    job: str
    version: str
    run_id: str
    input: Any = None
    rerun_of: str | None = None
    submitted_at: float | None = None


async def _reconcile_delivery(store: RunStore, inspector: Inspector, watch: DeliveryWatch) -> bool:
    async def lookup(task_id: str) -> TaskInfo | None:
        try:
            return await inspector.aget_task_info(watch.queue or "default", task_id)
        except (QueueNotFoundError, TaskNotFoundError):
            return None

    info = await lookup(watch.expected)
    if info is None and watch.previous is not None:
        publisher = await lookup(watch.previous)
        if publisher is not None and publisher.state not in ("archived", "completed"):
            return False
        info = await lookup(watch.expected)
        if info is None and publisher is not None and publisher.state == "archived":
            info = publisher
    if info is None and watch.queue is not None:
        reason = f"broker delivery {watch.expected} is missing from queue {watch.queue!r}"
    elif info is not None and info.type == "spater.flow.deliver" and info.state == "archived":
        reason = f"broker archived delivery {info.id} after {info.retried} retries: {info.last_err}"
    else:
        return False
    await store.fail_delivery(watch, reason)
    return True


class FlowQueue:
    """Register a run dispatcher on an Asynq app.

    Producers and workers register the same job versions. An initial delivery
    carries the input, so a worker can create the run even if the producer exits
    immediately after enqueue. Inspect it through `runner` after its first claim.
    Each delivery has 24 hours and three transport retries by default. Set
    `delivery_timeout` and `delivery_max_retry` on the producer to override
    them for the initial message and all wakes. Time parked between deliveries
    does not count toward their timeout. Stop the worker before calling
    `aclose()` to release its providers.
    """

    def __init__(
        self,
        app: Asynq,
        jobs: Sequence[Job[Any, Any]],
        *,
        concurrency: int = 10,
        delivery_timeout: int | timedelta = timedelta(days=1),
        delivery_max_retry: int = 3,
        executor: StepExecutor | None = None,
        middleware: Sequence[StepMiddleware] = (),
        on_error: Sequence[StepErrorHandler] = (),
        pool_size: int = 4,
    ) -> None:
        timeout = delivery_timeout.total_seconds() if isinstance(delivery_timeout, timedelta) else delivery_timeout
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not 1 <= timeout <= 2**31 - 1
            or timeout % 1
        ):
            raise ValueError("delivery_timeout must be a positive whole number of seconds up to 2147483647")
        if type(delivery_max_retry) is not int or not 0 <= delivery_max_retry <= 2**31 - 1:
            raise ValueError("delivery_max_retry must be an integer between 0 and 2147483647")
        self._app = app
        self._cursor = ""
        self._maintenance = asyncio.Lock()
        self._url = app.redis_url
        self._jobs = {(job.name, job.digest): job for job in jobs}
        self.runner = Runner(
            self._url,
            concurrency=concurrency,
            executor=executor,
            middleware=middleware,
            on_error=on_error,
            pool_size=pool_size,
        )
        self._task = app.task("spater.flow.deliver", timeout=timeout, max_retry=delivery_max_retry)(self._deliver)
        app._worker_maintenance.append(self._reconcile)

    def middleware(self, fn: StepMiddleware) -> StepMiddleware:
        """Register step middleware on the underlying runner."""
        return self.runner.middleware(fn)

    def on_error(self, fn: StepErrorHandler) -> StepErrorHandler:
        """Register a step failure hook on the underlying runner."""
        return self.runner.on_error(fn)

    async def _reconcile(self) -> None:
        if self._maintenance.locked():
            return
        async with self._maintenance:
            self._cursor, watches = await self.runner.store.delivery_watches(self._cursor)
            for watch in watches:
                await _reconcile_delivery(self.runner.store, self._app.inspector, watch)

    @overload
    async def enqueue(
        self,
        job: Job[None, Any],
        input: None = None,
        *,
        run_id: str | None = None,
        on_duplicate: DuplicatePolicy = "fail",
        queue: str = "default",
    ) -> str: ...

    @overload
    async def enqueue(
        self,
        job: Job[InT, Any],
        input: InT,
        *,
        run_id: str | None = None,
        on_duplicate: DuplicatePolicy = "fail",
        queue: str = "default",
    ) -> str: ...

    async def enqueue(
        self,
        job: Job[Any, Any],
        input: Any = None,
        *,
        run_id: str | None = None,
        on_duplicate: DuplicatePolicy = "fail",
        queue: str = "default",
    ) -> str:
        """Queue a new run and return its stable run ID."""
        return await self._enqueue(job, input=input, queue=queue, run_id=run_id, on_duplicate=on_duplicate)

    async def _enqueue(
        self,
        job: Job[Any, Any],
        *,
        input: Any,
        queue: str,
        run_id: str | None = None,
        on_duplicate: DuplicatePolicy = "fail",
        rerun_of: str | None = None,
    ) -> str:
        if on_duplicate not in ("fail", "use_existing"):
            raise ValueError("on_duplicate must be 'fail' or 'use_existing'")
        version = job.digest
        if (job.name, version) not in self._jobs:
            raise ValueError(f"job {job.name!r} version {version!r} is not registered")
        encoded = self.runner.validate(job, input)
        identity = str(uuid4()) if run_id is None else run_id
        if run_id is not None:
            try:
                prior = await self.runner.store.get(identity)
            except KeyError:
                pass
            else:
                if on_duplicate == "fail":
                    raise RunExistsError(f"run {identity!r} already exists")
                if prior["job"] != job.name:
                    raise ValueError(f"run {identity!r} belongs to job {prior['job']!r}, not {job.name!r}")
                return identity
        delivery = _Delivery(job.name, version, identity, encoded, rerun_of, now(self._url))
        try:
            await self._task.aenqueue(delivery, task_id=identity, queue=queue)
        except TaskIDConflictError as exc:
            if on_duplicate == "use_existing":
                return identity
            raise RunExistsError(f"run {identity!r} is already queued") from exc
        return identity

    async def cancel(self, run_id: str, *, queue: str = "default") -> None:
        """Cancel a queued or running root run and drop its pending broker task if present."""
        expected: str | None = run_id
        broker_queue = queue
        try:
            row = await self.runner.store.get(run_id)
        except KeyError:
            row = None
        if row is not None:
            broker_queue = row.get("broker_queue") or queue
            expected = row.get("delivery", {}).get("expected")
            await self.runner.cancel(run_id)
        if expected:
            try:
                await asyncio.to_thread(self._app.inspector.cancel_processing, expected)
            except (TaskNotFoundError, QueueNotFoundError):
                pass
            try:
                await asyncio.to_thread(self._app.inspector.delete_task, broker_queue, expected)
            except (TaskNotFoundError, QueueNotFoundError):
                pass

    async def _deliver(self, delivery: _Delivery, ctx: TaskContext) -> None:
        job = self._jobs.get((delivery.job, delivery.version))
        if job is None or job.digest != delivery.version:
            raise HandlerNotFound(f"job {delivery.job!r} version {delivery.version!r} is not registered")
        if ctx.id == delivery.run_id:
            value = decode(delivery.input, normalize(job.input))
            encoded = self.runner.validate(job, value)
            await self.runner._create(
                job, delivery.run_id, encoded, delivery.run_id, broker_queue=ctx.queue, rerun_of=delivery.rerun_of
            )
        next_id = str(uuid5(NAMESPACE_URL, f"spater.flow/continue/{ctx.id}"))
        try:
            result = await self.runner.deliver(job, delivery.run_id, delivery_id=ctx.id, next_delivery_id=next_id)
        except ClaimLost:
            return
        if isinstance(result, Parked):
            delay = max(0.0, result.due - now(self._url))
            last_second = datetime.max.replace(microsecond=0, tzinfo=timezone.utc).timestamp()
            wake = min(math.ceil(time.time() + delay), last_second)
            process_at = datetime.fromtimestamp(wake, timezone.utc)
            continuation = _Delivery(job.name, delivery.version, delivery.run_id)
            try:
                await self._task.aenqueue(
                    continuation,
                    task_id=next_id,
                    queue=ctx.queue,
                    process_at=process_at,
                    max_retry=ctx.max_retry,
                    timeout=ctx.message.timeout,
                )
            except TaskIDConflictError:
                pass

    async def aclose(self) -> None:
        """Release resources acquired by this queue's worker."""
        await self.runner.aclose()
