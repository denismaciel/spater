# Spater

A distributed task queue and typed DAG runner for Python, backed by Redis or Postgres, with the engine written in Rust.

Spater is a port of Go's [asynq](https://github.com/hibiken/asynq). On Redis it is wire-compatible with asynq: Python and Go services can enqueue tasks for each other, share queues, and use [asynqmon](https://github.com/hibiken/asynqmon) or the `asynq` CLI. On [Postgres](#postgres-backend), tasks live in your application database and can be enqueued inside your own transactions.

Rust (`pyo3` + `tokio`) handles dequeuing, leases, heartbeats, retries, recovery, scheduling, and storage (Redis Lua scripts or Postgres queries). Handlers stay plain Python — `async def` on the event loop or `def` in a thread pool.

## Install

Requires Python 3.10+ and Redis or Postgres. Prebuilt wheels are published for Linux (x86-64, ARM64, glibc and musl) and macOS (x86-64, Apple Silicon), including free-threaded CPython 3.14t and 3.15t (where Spater keeps the GIL off and sync handlers run in parallel) — no Rust compiler needed.

```sh
uv add spater
```

Optional extras:

- `uv add "spater[flow-postgres]"` — Postgres store for `spater.flow` DAG runs
- `uv add "spater[metrics]"` — Prometheus collector (`spater.metrics.QueueMetricsCollector`)
- `uv add "spater[otel]"` — OpenTelemetry tracing (`spater.otel.instrument`)

Prebuilt wheels with `SHA256SUMS` and a flat package index are also published on [GitHub Releases](https://github.com/denismaciel/spater/releases); see [docs/development.md](docs/development.md#installing-from-github-releases) for pinned index installs.

## Quick start

```python
# tasks.py
from dataclasses import dataclass
from spater import Asynq, TaskContext

app = Asynq("redis://localhost:6379/0")

@dataclass
class Email:
    to: str
    subject: str

@app.task("email:send", queue="critical", max_retry=5, timeout=30)
async def send_email(email: Email, ctx: TaskContext) -> dict:
    ...  # payload is decoded from the type hint (dataclass, pydantic, bytes, JSON)
    return {"sent": True}
```

Enqueue from anywhere:

```python
info = send_email.enqueue(Email("a@b.c", "hi"))                        # sync
info = await send_email.aenqueue(Email("a@b.c", "hi"), process_in=60)  # async, delayed
result = await send_email.wait_result(info, timeout=10)                # requires retention=...
```

Run workers and the scheduler:

```sh
spater worker tasks:app -q critical=6 -q default=3 -c 20
spater worker tasks:app --processes 4     # one worker server per process
spater scheduler tasks:app                # enqueues @app.periodic tasks
```

## Typing

Spater ships inline type annotations and stubs for the Rust extension. Payload and result types come from the handler signature, so type checkers catch mismatches at the call site:

```python
send_email                                     # TaskDef[Email, dict]
info = send_email.enqueue(Email("a@b.c", "hi"))  # TaskInfo[dict]
await send_email.wait_result(info)             # dict
send_email.enqueue("a@b.c")                    # type error: expected Email
send_email.enqueue(email, proces_in=60)        # type error: unknown option
```

The payload is the handler's first positional parameter; a parameter annotated `ctx: TaskContext` receives the context. A task without a payload is `TaskDef[None, R]` and is enqueued with no arguments.

## Typed DAGs (`spater.flow`)

`spater.flow` defines, validates, renders, and executes typed DAGs. Business functions stay plain Python with zero Spater imports; a wiring module connects them with `@Job.define(...)`, `Builder`, and `Ref[T]` handles:

```python
from spater.flow import Builder, Job, Ref, Runner, Succeeded

@Job.define("archived_report")
def ARCHIVED_REPORT(f: Builder, day: Ref[Day]) -> Ref[Report]:
    f.step(reports.extract, day)                 # runs concurrently with extract_archive
    archive = f.step(reports.extract_archive, day)
    return f.step(reports.transform, day, archive)

async def report():
    async with Runner("memory://reports", concurrency=4) as runner:
        result = await runner.run(ARCHIVED_REPORT, Day("2026-11-18"))
        if isinstance(result, Succeeded):
            print(result.value)                  # typed as Report
        history = await runner.inspect(result.run_id)
        print(history["status"], history["attempts"])
```

Key properties:

- **Static validation at definition time:** `Job` checks `Ref` scopes, parameter and return types, exhaustive `branch` cases, `PerWorker` / `PerStep` providers, retries, and serializability immediately, capturing a read-only snapshot for `job.to_json()`, `job.version`, and `job.graph()`.
- **Concurrency and control flow:** independent steps run concurrently up to `concurrency`; pass `after=` for ordering without a data dependency. Supports `branch`, `map`, `sleep`, `Retry(on=..., max=..., after=...)` (for raised exceptions or returned outcomes), and `meta={...}` step hints.
- **Durable resumption and idempotency:** each step commits its typed output on completion; redelivery skips committed steps and repeats interrupted ones with the same `ctx.idempotency_key` and original deadline. Pass a deterministic `run_id=` (with `on_duplicate="fail"` or `"use_existing"`) for idempotent starts, `runner.cancel(run_id)` to cancel live runs, `runner.graph(run_id)` for the saved graph, and `runner.rerun(job, run_id)` for a fresh linked run.
- **Direct or queued execution:** run directly via `Runner` against `memory://<name>` or a Postgres DSN (`spater[flow-postgres]`), or dispatch through a Spater worker with `queue = FlowQueue(app, [ARCHIVED_REPORT])` (`await queue.enqueue(...)`, `await queue.aclose()` on shutdown). Both accept `executor=` (`StepExecutor`, default `LocalExecutor`), `middleware=` / `@runner.middleware`, `on_error=` / `@runner.on_error`, and `pool_size=`.

See [examples/flows/jobs.py](examples/flows/jobs.py) and [examples/flows/GRAPHS.md](examples/flows/GRAPHS.md). A local web console for inspecting runs and launching workflows is included in the repository; see [docs/development.md#workflow-operations-center](docs/development.md#workflow-operations-center).

## Features

**Task options:** `queue`, `max_retry`, `timeout`, `deadline`, `unique` (dedupe window), `retention` (keep results), `group` (batching), `process_in` / `process_at`, custom `headers`.

**Retry control via exceptions:**

| Raise | Effect |
| --- | --- |
| any exception | retry with exponential backoff, then archive |
| `SkipRetry` | archive immediately |
| `RevokeTask` | drop the task without retrying or archiving |
| `Retry("msg", after=30)` | retry after 30 s |
| `RateLimited(after=5)` | retry later without counting as a failure |

**Middleware, error hooks, and lifespan:**

```python
@app.middleware
async def timing(ctx, call_next):
    return await call_next(ctx)

@app.on_error
def report(ctx, exc): ...

@app.lifespan
async def resources(app):
    db = await connect()
    yield
    await db.close()
```

**Cooperative cancellation:** timeouts, deadlines, and `inspector.cancel_processing(id)` cancel async handlers. Sync handlers should check `ctx.cancelled` or call `ctx.raise_if_cancelled()`.

**Periodic tasks:**

```python
app.periodic("@every 30s", payload=None)(cleanup)
app.periodic("CRON_TZ=Europe/Berlin 0 9 * * *", Report(kind="daily"))(report)
```

**Groups (batching):** tasks enqueued with `group="key"` are aggregated once the grace period, max delay, or max size is reached:

```python
@app.group_aggregator
def merge(group: str, tasks: list[Task]) -> Task: ...
```

**Distributed rate limiting** (compatible with Go's `x/rate` Redis key):

```python
from spater.rate import Semaphore

openai = Semaphore(app.redis_url, "openai", max_tokens=5)

@app.task
async def summarize(doc: Doc, ctx: TaskContext):
    async with openai.hold(ctx, retry_after=3):
        ...
```

**Inspection:**

```python
from spater import Inspector

inspector = Inspector("redis://localhost:6379/0")
inspector.get_queue_info("default")
inspector.list_tasks("default", "retry")
inspector.pause_queue("default")
```

```sh
spater inspect queues
spater inspect tasks default retry
spater inspect cancel <task-id>
```

**Observability:**

- `spater.metrics.QueueMetricsCollector` — Prometheus collector using asynq metric names (`spater[metrics]`).
- `spater.otel.instrument(app)` — OpenTelemetry trace context propagation via task headers (`spater[otel]`).
- Logs emit through Python's `logging` under `spater.*`.

**Testing:**

```python
from datetime import timedelta
from spater.testing import advance_time, drain, memory_url, run_task

assert await run_task(send_email, Email("a@b.c", "hi")) == {"sent": True}  # no broker

app = Asynq(memory_url())  # in-process broker, no Redis/Postgres needed
info = await report.aenqueue(process_in=timedelta(hours=1))
advance_time(app, timedelta(hours=1))  # move virtual clock forward without sleeping
await drain(app)                       # run real worker until queues are empty
```

All clients, workers, inspectors, and schedulers in the same process sharing a `memory://<name>` URL share one store (`spater.testing.reset_memory()` clears it). Leases, deadlines, and cron schedules stay on real time.

**Low-level API:** `spater.Client` and `spater.Worker(handler)` mirror Go's `Client` and `Server` directly when you do not want the `Asynq` app layer.

## Postgres backend

Pass a `postgres://` or `postgresql://` URL anywhere a Redis URL is accepted (`Asynq`, `Client`, `Worker`, `Inspector`, `Scheduler`, `Semaphore`):

```python
app = Asynq("postgresql://app@localhost/app")
```

- Tasks live in `pyasynq_tasks` storing the same protobuf payload as Redis alongside indexed state columns.
- Workers claim tasks with `UPDATE ... FOR UPDATE SKIP LOCKED` and wake immediately via `LISTEN`/`NOTIFY` (with polling as fallback).
- Schema creation runs on first connect under an advisory lock, or manage it yourself with `spater.postgres.schema_sql()`.

The primary advantage is **transactional enqueue** — commit or roll back tasks alongside application data:

```python
stmt = send_email.enqueue_statement(Email("a@b.c", "hi"))  # paramstyle="numeric" for asyncpg
with conn.transaction():                                    # psycopg
    conn.execute("INSERT INTO users (email) VALUES (%s)", ("a@b.c",))
    conn.execute(stmt.sql, stmt.params)
```

Trade-offs:

- Go asynq, `asynqmon`, and the `asynq` CLI only understand the Redis layout.
- Unique tasks cannot be enqueued with `enqueue_statement` (use the client).
- Throughput is lower than Redis (~7k no-op tasks/s per worker vs ~20k/s on Redis; multiple `enqueue_statement` calls in one transaction share a commit).

## Differences from Go asynq

The Redis layout and protobuf format match asynq v0.26, with a few edge cases fixed:

- Non-failure retries (`RateLimited`, or `is_failure` returning `False`) do not consume `max_retry`.
- `timeout` and `retention` under one second raise `ValueError` instead of silently truncating to `0`.
- Delayed grouped tasks (`process_in` + `group`) join their group when due, and failed aggregations return to their group.
- `delete_queue` accounts for completed, grouped, and aggregating tasks (`force=True` removes them).
- Task-ID conflicts do not leave behind a uniqueness lock, and deleted queues reappear when enqueued into again.

## Development

See [docs/development.md](docs/development.md) for local setup, test suites, the workflow operations center, and release procedures.

## License

MIT. See [LICENSE](LICENSE).
