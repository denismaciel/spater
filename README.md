# Spater

A distributed task queue for Python, backed by Redis or Postgres, with the engine written in Rust.

Spater is a port of Go's [asynq](https://github.com/hibiken/asynq). On Redis it is **wire-compatible** with asynq: Python and Go services can enqueue tasks for each other, run workers on the same queues, and use the same tools ([asynqmon](https://github.com/hibiken/asynqmon), the `asynq` CLI). On [Postgres](#postgres-backend) your tasks live in your application database and can be enqueued inside your own transactions.

Rust (pyo3 + tokio) owns everything that doesn't need to be Python: dequeuing, leases, heartbeats, retries, recovery, scheduling and the storage layer (Redis Lua scripts or Postgres queries). Your handlers are plain Python, either `async def` on the event loop or `def` in a thread pool.

## Install

Install a published release from PyPI with uv:

```sh
uv add spater
```

The distribution, Python import, and CLI are `spater`. Old module and command
aliases are removed. Existing PostgreSQL storage identifiers stay stable so
upgrading does not lose queued tasks.

Prebuilt wheels are also available on [GitHub Releases](https://github.com/denismaciel/spater/releases). To select the matching wheel from a specific GitHub release:

```sh
uv pip install --no-index --find-links 'https://github.com/denismaciel/spater/releases/download/v0.6.0/index.html' 'spater==0.6.0'
```

Each release includes `SHA256SUMS` and an `index.html` flat package index with hashed download links for automatic platform selection. Pin the versioned asset URL and its SHA-256 in your dependency lockfile. Installing a wheel needs neither Git nor a Rust compiler.

Wheels are built for Linux (x86_64, aarch64, musl) and macOS (x86_64, arm64), for Python 3.10 and newer, plus free-threaded CPython 3.14t and 3.15t, where importing spater keeps the GIL off and sync handlers run in parallel. You need Redis, or Postgres (the test suite runs on Postgres 17).

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
info = send_email.enqueue(Email("a@b.c", "hi"))                  # sync
info = await send_email.aenqueue(Email("a@b.c", "hi"), process_in=60)  # async, delayed
result = await send_email.wait_result(info, timeout=10)          # needs retention=...
```

Run workers and the scheduler:

```sh
spater worker tasks:app -q critical=6 -q default=3 -c 20
spater worker tasks:app --processes 4     # one worker server per process
spater scheduler tasks:app                # enqueues @app.periodic tasks
```

## DAG runner

The development checkout includes `spater.flow` for defining, checking, drawing,
and executing typed DAGs. Business functions stay plain Python with zero Spater
imports. A wiring module connects them using `@Job.define(...)`, `Builder`, and
explicit `Ref[T]` handles:

```python
from datetime import timedelta as td
from spater.flow import Builder, Job, Ref, Retry, Runner, Succeeded

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

Constructing a `Job` checks all `Ref` scopes, parameter and return types,
exhaustive `branch` cases, providers (`PerWorker` / `PerStep`), retries, and
serializability immediately, and captures a read-only definition snapshot for
`job.to_json()`, `job.version`, and `job.graph()`. Independent steps run
concurrently up to `concurrency`; pass `after=` when ordering is needed without a
data dependency. Run a job directly with `Runner` against `memory://<name>` or a
Postgres DSN (install the `flow-postgres` extra for Postgres).

Pass a deterministic `run_id=` (and `on_duplicate="fail"` or `"use_existing"`)
to `runner.run(...)` or `queue.enqueue(...)` for idempotent starts, and
`await runner.cancel(run_id)` / `await queue.cancel(run_id)` to cancel a live run.
Steps can carry JSON-serializable `meta={...}` hints, and `Runner` / `FlowQueue`
accept `executor=` (implementing `StepExecutor`, defaulting to `LocalExecutor`),
`middleware=` / `@runner.middleware`, `on_error=` / `@runner.on_error`, and
`pool_size=` for pooled Postgres connections.

For background execution, register `queue = FlowQueue(app, [ARCHIVED_REPORT])`
on the producer and worker apps, importing `FlowQueue` from `spater.flow`.
`await queue.enqueue(ARCHIVED_REPORT, day)` returns the run ID. The run record
is available after the first worker claim. Call `await queue.aclose()` at
shutdown to release worker-scoped providers.

Each broker delivery defaults to a 24-hour timeout and three retries. Set
`delivery_timeout` and `delivery_max_retry` on `FlowQueue` to change them.
Delayed continuations inherit the producer's settings, and time spent parked
doesn't consume the next delivery's timeout. If the delivery needed to continue
an admitted run is archived or disappears from its queue, worker maintenance
or `runner.inspect` records a failure at `$delivery`. An earlier committed
step failure keeps its original cause. Step retries (`Retry(on=..., max=..., after=...)`)
support both raised exceptions and returned domain outcomes.

The runner commits each step's typed output with its completion. On redelivery,
it skips committed work and repeats interrupted steps with the same idempotency
key. A timed step keeps its original deadline across worker loss. If the deadline
has passed, redelivery records a timeout before invoking the step. External
services can use the idempotency key to deduplicate side effects. A compatible
worker can resume a Postgres run after process death. A named memory store lives
within one process. Inspector previews default to 16 KiB per attempt. Use
`await runner.graph(run_id)` for the saved graph or
`await runner.rerun(current_job, run_id)` to start a fresh linked run.

[Example jobs](examples/flows/jobs.py) · [Generated graphs](examples/flows/GRAPHS.md)

### Workflow operations center

Start the local console from the repository root with Node 22.12 or newer,
Rust, and uv. The demo runs real example jobs in a named in-memory store.

```sh
uv sync --locked
npm ci --prefix ops/web
node ops/dev.mjs --demo
```

Open [Runs](http://127.0.0.1:3410/runs) to inspect root runs, attempt timing,
saved graphs, output previews, and mapped children. Open **Workflows** to inspect
a registered definition or launch it with JSON input. A manual rerun starts all
steps with the current registered definition. Keep or edit the saved input.
The source run lists its reruns, and each rerun shows what changed in the input,
definition, failure, and recorded step outcomes.

To use your own jobs, set `SPATER_OPS_JOBS` to an importable `module:attribute`
containing a sequence of `Job` definitions and run `node ops/dev.mjs`.
Set `SPATER_OPS_STORE` to the same PostgreSQL URL as your runner to see runs
from other processes. The default `memory://spater-ops` store lives in the
console's Python process. Console launches go to the `spater-ops` queue handled
by its worker. A queued launch returns its run ID immediately.

The console is a trusted local tool. The web server listens on loopback port
3410 and the Rust API on loopback port 8410. For remote use, forward the web
port through SSH. Ctrl-C stops the local services.

Rust handler DTOs define the API contract. Axum serves the utoipa specification
at [openapi.json](http://127.0.0.1:8410/openapi.json). In `ops/web`, run
`npm run gen:openapi` to regenerate the checked-in specification and TypeScript
types, or `npm run check:openapi` to detect drift. The TanStack Start app uses
those types with `openapi-fetch` and React Query. Its SSR loaders call the Rust
API directly and browser requests use the same-origin Start route.

For a production build of the local console, run `npm run build` in `ops/web`,
start `cargo run --locked --manifest-path ops/api/Cargo.toml -- serve` from
the repository root, and `npm start` from `ops/web`. To run the browser suite
against that build, use `npx playwright install chromium` and `npm run test:e2e`
in `ops/web`. The tests start their own API, worker, and web listener.

## Typing

The package ships type information, including stubs for the Rust extension. Each task's payload and result types come from the handler's signature, so type checkers catch mistakes at the call site:

```python
send_email                                   # TaskDef[Email, dict]
info = send_email.enqueue(Email("a@b.c", "hi"))   # TaskInfo[dict]
await send_email.wait_result(info)           # dict
send_email.enqueue("a@b.c")                  # error: expected Email
send_email.enqueue(email, proces_in=60)      # error: unknown option
```

The payload is the handler's first positional parameter. A parameter annotated `ctx: TaskContext` receives the context. A task without a payload is `TaskDef[None, R]` and is enqueued with no arguments.

## Features

**Task options:** `queue`, `max_retry`, `timeout`, `deadline`, `unique` (dedupe window), `retention` (keep results), `group` (batching), `process_in` / `process_at`, custom `headers`.

**Errors control retries:**

| raise | effect |
|---|---|
| any exception | retry with exponential backoff, then archive |
| `SkipRetry` | archive immediately |
| `RevokeTask` | drop the task, no retry or archive |
| `Retry("msg", after=30)` | retry after 30s |
| `RateLimited(after=5)` | retry later, not counted as a failure |

**Middleware, error hooks and lifespan:**

```python
@app.middleware
async def timing(ctx, call_next):
    return await call_next(ctx)

@app.on_error
def report(ctx, exc): ...

@app.lifespan
async def resources(app):
    db = await connect(); yield; await db.close()
```

**Cancellation is cooperative.** Timeouts, deadlines and `inspector.cancel_processing(id)` cancel async handlers. Sync handlers should check `ctx.cancelled` or call `ctx.raise_if_cancelled()`.

**Periodic tasks:**

```python
app.periodic("@every 30s", payload=None)(cleanup)
app.periodic("CRON_TZ=Europe/Berlin 0 9 * * *", Report(kind="daily"))(report)
```

**Groups (batching):** tasks enqueued with `group="key"` are combined into one task once the grace period, max delay or max size is reached:

```python
@app.group_aggregator
def merge(group: str, tasks: list[Task]) -> Task: ...
```

**Rate limiting across workers** (uses the same Redis key as Go's `x/rate`):

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
i = Inspector("redis://localhost:6379/0")
i.get_queue_info("default"); i.list_tasks("default", "retry"); i.pause_queue("default")
```

```sh
spater inspect queues
spater inspect tasks default retry
spater inspect cancel <task-id>
```

**Observability:**

- `spater.metrics.QueueMetricsCollector` is a Prometheus collector that uses asynq's metric names. Install with `uv add "spater[metrics]"`.
- `spater.otel.instrument(app)` propagates OpenTelemetry trace context through task headers. Install with `uv add "spater[otel]"`.
- Logs go to Python `logging` under `spater.*`.

**Testing:**

```python
from spater.testing import run_task, drain, memory_url
assert await run_task(send_email, Email("a@b.c", "hi")) == {"sent": True}  # no broker
await drain(app)  # real worker until queues are empty
```

For tests that go through a real worker without Redis or Postgres, use an in-process broker: `Asynq(memory_url())` (or any `memory://<name>` URL). Every client, worker, inspector and scheduler in the process that opens the same URL shares one store, and `spater.testing.reset_memory()` empties it. It runs the same conformance suite as the other backends. It doesn't persist anything and can't be shared between processes.

Its clock can be moved forward, so retries, scheduled tasks, retention and uniqueness TTLs don't need real waits:

```python
from spater.testing import advance_time
info = await report.aenqueue(process_in=timedelta(hours=1))
advance_time(app, timedelta(hours=1))   # the task is pending now
await drain(app)
```

Leases, task deadlines and cron schedules stay on real time, so advancing the clock never makes a running task look abandoned.

**Low-level API:** `spater.Client` and `spater.Worker(handler)` mirror Go's `Client` and `Server` when you don't want the `Asynq` app layer.

## Postgres backend

Pass a `postgres://` URL anywhere a Redis URL goes (`Asynq`, `Client`, `Worker(redis_url=...)`, `Inspector`, `Scheduler`, `Semaphore`). The whole API works the same; the test suite runs one conformance suite against every backend.

```python
app = Asynq("postgresql://app@localhost/app")
```

The design follows [procrastinate](https://github.com/procrastinate-org/procrastinate), with the queries issued from Rust:

- Every task is a row in `pyasynq_tasks`, holding the same asynq protobuf message the Redis backend stores, plus indexed columns for its state.
- Workers claim tasks with `UPDATE ... FOR UPDATE SKIP LOCKED`, so any number of workers can share a queue without blocking each other. Every state transition is a single statement.
- A trigger runs `NOTIFY` when a task becomes pending. Idle workers `LISTEN` and pick the task up right away, and the poll interval stays as a fallback. `Inspector.cancel_processing` also travels over `NOTIFY`.
- The schema is created on first connect, guarded by an advisory lock and a version table. To manage it with your own migration tool, apply `spater.postgres.schema_sql()` instead.

The main reason to use it is **transactional enqueue**. The task is committed or rolled back together with your data:

```python
stmt = send_email.enqueue_statement(Email("a@b.c", "hi"))   # paramstyle="numeric" for asyncpg
with conn.transaction():                                     # psycopg
    conn.execute("INSERT INTO users (email) VALUES (%s)", ("a@b.c",))
    conn.execute(stmt.sql, stmt.params)
```

Trade-offs:

- Go asynq, asynqmon and the `asynq` CLI only understand the Redis layout.
- Unique tasks can't be enqueued with `enqueue_statement` (use the client).
- Postgres is slower than Redis. On a laptop with durable commits, one worker processed about 7k no-op tasks/s on Postgres (it claims up to 32 tasks per statement) and about 20k/s on Redis. Each `Client.enqueue` is its own commit, so one client enqueued about 2k tasks/s on Postgres, against 25k/s on Redis. Several `enqueue_statement`s in one transaction share a commit.

## Differences from Go asynq

The data format and Redis layout are the same as asynq v0.26. Spater fixes a few of asynq's edge cases:

- Errors that don't count as failures (`RateLimited`, or `is_failure` returning False) are retried without using up `max_retry`.
- `timeout` and `retention` under one second raise `ValueError`. Go silently truncates them to 0, which means "unset".
- Delayed tasks with a `group` (`process_in` combined with `group`) join their group when due, and failed aggregations are put back into their group instead of being stranded.
- `delete_queue` also counts completed, grouped and aggregating tasks. `force=True` removes them.
- A task-ID conflict doesn't leave a uniqueness lock behind, and a deleted queue reappears when a client enqueues into it again.

## Development

Everything you need (Rust, Python, uv, Redis, Postgres, Go) comes from the Nix dev shell:

```sh
nix develop
make dev    # build the Rust extension into .venv
make test   # cargo + pytest (spawns its own redis and postgres; Go interop tests need go)
make test-ft  # pytest on free-threaded Python 3.14t in .venv-ft; fails if the GIL turns on
make check
make build
make release-check
```

Layout: `src/` holds the Rust engine and pyo3 bindings. `src/rdb/` has the storage layer: the Redis Lua scripts in `src/rdb/lua/` and the Postgres backend and schema in `src/rdb/pg/`, and the in-process backend in `src/rdb/memory.rs`. `python/spater/` holds the Python API, and `interop/go/` holds a small Go program the tests use to check compatibility against real asynq.

## License

MIT. See [LICENSE](LICENSE).

### Packaging and releases

Use uv 0.12.4 (`uv sync --locked`) for development and builds. The Nix shell
bootstraps that exact uv release through its packaged uv (first use needs network).
CI, release builders and the Nix shell use Rust 1.100.0 from `rust-toolchain.toml`. The in-tree
`build_backend.py` delegates Python metadata, source archives and wheel layout
to `uv_build==0.12.4`. Since uv_build itself only supports pure Python, the
adapter compiles the Rust extension with locked Cargo dependencies, inserts it
into the wheel, and updates platform/ABI tags and RECORD hashes. Editable sync
compiles into the source package; rerun `uv sync --reinstall-package spater`
after Rust edits. No Maturin or Hatch backend is involved.

Keep `project.version` and `Cargo.toml` version identical. `make release-check
TAG=vX.Y.Z` checks their agreement, builds and installs a wheel outside the
checkout, verifies its RECORD, and rebuilds the sdist. The release workflow
runs full CI on the tagged commit before publication. Manual dispatch only
builds. Standard CPython wheels retain abi3 (Python 3.10+); free-threaded
3.14 and 3.15 have separate ABI wheels. cibuildwheel repairs Linux wheels
inside manylinux/musllinux containers and smoke-tests every wheel on a matching
native architecture. GitHub releases include immutable artifacts, SHA256SUMS
and the hashed flat index. Local Linux wheels carry a local Linux tag until
repaired; they do not claim manylinux portability.


Tagged releases also publish the tested wheels and sdist to PyPI. Before the
first publication, configure the `spater` project's PyPI Trusted Publisher for owner `denismaciel`,
repository `spater`, workflow `release.yml`, and GitHub environment `pypi`.
Protect that environment with required review and restrict release tag creation
and modification through repository rules. No long-lived PyPI token is needed.

Only `vX.Y.Z` tag pushes in the canonical repository can publish. Validation
requires the tag's commit to be reachable from the current default branch and
versions to match; full CI and every platform build must pass. Manual dispatch
never publishes, including dispatch on a tag. GitHub assets are published first;
the separate `pypi` job then downloads only this run's wheel and sdist artifacts.
It has only read access to repository contents and OIDC token permission, performs
no checkout or package build/install, and uses the SHA-pinned official PyPA action
with PEP 740 attestations. Build/test jobs have no publishing permissions, retain
no checkout credentials, and disable shared dependency caches.

PyPI files cannot be replaced. If publishing fails after the GitHub release was
created, rerun the failed PyPI job after resolving its configuration; rerunning
successful GitHub publication is intentionally rejected. A partial PyPI upload
requires maintainer investigation: existing files are not silently skipped.
See [PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/using-a-publisher/)
and [attestations](https://docs.pypi.org/attestations/producing-attestations/).
