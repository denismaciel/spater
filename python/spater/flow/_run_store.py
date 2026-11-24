"""Transactional flow records shared by direct runs and queued deliveries."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import sqlite3
import time
import weakref
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from threading import BoundedSemaphore, Lock
from typing import TYPE_CHECKING, Any, TypeVar

from ._nodes import RunExistsError
from ._run_tasks import settle

if TYPE_CHECKING:
    import psycopg
    from typing_extensions import LiteralString

Record = dict[str, Any]
Owner = tuple[str, int]
T = TypeVar("T")
_TERMINAL = frozenset(("succeeded", "failed", "cancelled"))
_IDENTITY = (
    "id",
    "job",
    "definition",
    "input",
    "root_id",
    "parent_id",
    "parent_node",
    "parent_attempt",
    "item_index",
    "rerun_of",
)
_ATTEMPT_KEY = ("run_id", "node_id", "attempt")
_PAYLOAD = ("output", "manifest")

# Keep JSON as text: typed values and the display order of preview keys must survive
# every round trip, including updates. Payloads can be released without rewriting previews.
_LIVE_ROOTS_INDEX = (
    "CREATE INDEX IF NOT EXISTS flow_live_roots ON flow_runs(id) WHERE id = root_id AND status = 'running'"
)
_SCHEMA: tuple[LiteralString, ...] = (
    """CREATE TABLE IF NOT EXISTS flow_definitions (
       digest text PRIMARY KEY, definition_json text NOT NULL, graph_json text)""",
    """CREATE TABLE IF NOT EXISTS flow_runs (
       id text PRIMARY KEY, root_id text NOT NULL REFERENCES flow_runs(id),
       definition text NOT NULL REFERENCES flow_definitions(digest), parent_id text REFERENCES flow_runs(id),
       parent_node text, parent_attempt integer, item_index integer,
       generation bigint NOT NULL DEFAULT 0, status text NOT NULL, record text NOT NULL,
       workflow_key text, started_at double precision, rerun_source text,
       UNIQUE(parent_id, parent_node, parent_attempt, item_index))""",
    "CREATE INDEX IF NOT EXISTS flow_runs_root_status ON flow_runs(root_id, status)",
    _LIVE_ROOTS_INDEX,
    """CREATE TABLE IF NOT EXISTS flow_node_runs (
       run_id text NOT NULL REFERENCES flow_runs(id), node_id text NOT NULL, attempt integer NOT NULL,
       record text NOT NULL, payload text, PRIMARY KEY(run_id, node_id, attempt))""",
)

_HISTORY_INDEXES: tuple[LiteralString, ...] = (
    "CREATE INDEX IF NOT EXISTS flow_runs_started ON flow_runs(started_at DESC, id DESC) WHERE id = root_id",
    "CREATE INDEX IF NOT EXISTS flow_workflow_started ON flow_runs(workflow_key, started_at DESC, id DESC)",
    "CREATE INDEX IF NOT EXISTS flow_runs_reruns ON flow_runs(rerun_source, started_at DESC) WHERE rerun_source <> 'null'",
    "CREATE INDEX IF NOT EXISTS flow_runs_lineage_unindexed ON flow_runs(id) WHERE rerun_source IS NULL OR workflow_key IS NULL",
    "CREATE INDEX IF NOT EXISTS flow_runs_unindexed ON flow_runs(id) WHERE workflow_key IS NULL",
)


class ClaimLost(RuntimeError):
    """The current delivery can no longer change this run."""


class InputConflict(ValueError):
    """The same run and lineage exist with a different encoded input."""

    def __init__(self, run_id: str, prior_input: Any) -> None:
        super().__init__(f"run {run_id!r} already exists with different input")
        self.prior_input = prior_input


@dataclass(frozen=True)
class PendingWake:
    """The previous delivery may publish its committed continuation again."""

    due: float


@dataclass(frozen=True)
class ObsoleteDelivery:
    """This broker task has no outstanding work for the run."""


@dataclass(frozen=True)
class DeliveryWatch:
    root_id: str
    generation: int
    expected: str
    previous: str | None
    queue: str | None


def _watch(row: tuple[Any, ...]) -> DeliveryWatch | None:
    root_id, generation, record = row
    root = _read(record)
    delivery = root.get("delivery", {})
    if not delivery.get("expected"):
        return None
    return DeliveryWatch(root_id, generation, delivery["expected"], delivery.get("previous"), root.get("broker_queue"))


class _DB:
    def __init__(self, conn: sqlite3.Connection | psycopg.Connection[Any]):
        self.conn = conn

    def execute(self, sql: LiteralString, params: tuple[Any, ...] = ()) -> sqlite3.Cursor | psycopg.Cursor[Any]:
        if isinstance(self.conn, sqlite3.Connection):
            return self.conn.execute(sql, params)
        return self.conn.execute(sql.replace("?", "%s"), params)

    def index_old_runs(self) -> None:
        while rows := self.execute(
            "SELECT id, record FROM flow_runs WHERE workflow_key IS NULL OR rerun_source IS NULL ORDER BY id LIMIT 128"
        ).fetchall():
            batch = []
            for run_id, text in rows:
                row = _read(text)
                batch.append((_dump(row["job"]), row["started_at"], _dump(row.get("rerun_of")), run_id))
            sql = "UPDATE flow_runs SET workflow_key = ?, started_at = ?, rerun_source = ? WHERE id = ?"
            with contextlib.closing(self.conn.cursor()) as cursor:
                cursor.executemany(sql if isinstance(self.conn, sqlite3.Connection) else sql.replace("?", "%s"), batch)

    def locked_row(self, sql: LiteralString, params: tuple[Any, ...]) -> tuple[Any, ...] | None:
        if not isinstance(self.conn, sqlite3.Connection):
            sql += " FOR UPDATE"
        return self.execute(sql, params).fetchone()

    def root(self, owner: Owner) -> str:
        """Lock the root before reading or modifying any record in its tree."""
        sql = "SELECT generation, status FROM flow_runs WHERE id = ? AND root_id = id"
        row = self.locked_row(sql, (owner[0],))
        if row is None or row[0] != owner[1]:
            raise ClaimLost(f"claim {owner[1]} no longer owns root run {owner[0]!r}")
        return str(row[1])

    def run(self, run_id: str) -> Record:
        row = self.execute("SELECT record FROM flow_runs WHERE id = ?", (run_id,)).fetchone()
        if row is None:
            raise KeyError(run_id)
        return _read(row[0])

    def write_run(self, run: Record) -> None:
        self.execute("UPDATE flow_runs SET status = ?, record = ? WHERE id = ?", (run["status"], _dump(run), run["id"]))

    def attempts(self, run_id: str) -> list[Record]:
        rows = self.execute("SELECT record FROM flow_node_runs WHERE run_id = ? ORDER BY node_id, attempt", (run_id,))
        return [_read(record) for (record,) in rows]

    def terminalize(self, run: Record) -> None:
        """Release full values and close interrupted activations in the same commit."""
        self.write_run(run)
        self.execute("UPDATE flow_node_runs SET payload = NULL WHERE run_id = ? AND payload IS NOT NULL", (run["id"],))
        if run["status"] == "succeeded":
            return
        for attempt in self.attempts(run["id"]):
            if attempt["status"] in _TERMINAL or attempt["status"] in ("retried", "skipped"):
                continue
            failed = (
                run["status"] == "failed"
                and run.get("failure_kind") != "delivery"
                and run.get("node_id") == attempt["node_id"]
            )
            attempt["status"] = "failed" if failed else "cancelled"
            attempt["finished_at"] = run.get("finished_at")
            if failed:
                attempt["error"] = run.get("error")
            self.execute(
                "UPDATE flow_node_runs SET record = ? WHERE run_id = ? AND node_id = ? AND attempt = ?",
                (_dump(attempt), run["id"], attempt["node_id"], attempt["attempt"]),
            )

    def cancel_descendants(self, root_id: str, finished_at: float) -> None:
        active = self.execute(
            """SELECT record FROM flow_runs WHERE root_id = ?
            AND status NOT IN ('succeeded', 'failed', 'cancelled')""",
            (root_id,),
        ).fetchall()
        for (record,) in active:
            descendant = _read(record)
            descendant.update(status="cancelled", finished_at=finished_at)
            self.terminalize(descendant)

    def finish_failure(self, root: Record, finished_at: float) -> Record:
        failure = root.get("pending_failure")
        if failure is None:
            raise ValueError("root has no pending failure")
        current = self.run(failure["run_id"])
        node_id, reason = failure["node_id"], failure["error"]
        while True:
            current.update(status="failed", finished_at=finished_at, node_id=node_id, error=reason)
            current.pop("pending_failure", None)
            self.terminalize(current)
            if current["id"] == root["id"]:
                break
            reason = f"child {current['id']}, {node_id}: {reason}"
            node_id = current["parent_node"]
            current = self.run(current["parent_id"])
        self.cancel_descendants(root["id"], finished_at)
        return current


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _lineage(run: Record) -> str:
    return json.dumps([run.get(k) for k in _IDENTITY if k != "input"], sort_keys=True, allow_nan=False)


def _read(text: str) -> Record:
    data: Record = json.loads(text)
    return data


class _Memory:
    def __init__(self) -> None:
        self.offset = 0.0
        self.lock = Lock()
        self.waiters: dict[asyncio.Event, asyncio.AbstractEventLoop] = {}
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        weakref.finalize(self, self.conn.close)
        self.conn.execute("PRAGMA foreign_keys = ON")
        for stmt in (*_SCHEMA, *_HISTORY_INDEXES):
            self.conn.execute(stmt)

    def clock(self) -> float:
        with self.lock:
            return time.time() + self.offset

    def advance(self, seconds: float) -> None:
        with self.lock:
            self.offset += seconds
            waiters = tuple(self.waiters.items())
        for changed, loop in waiters:
            try:
                loop.call_soon_threadsafe(changed.set)
            except RuntimeError:
                if not loop.is_closed():
                    raise

    async def wait_until(self, due: float) -> None:
        loop = asyncio.get_running_loop()
        while True:
            changed = asyncio.Event()
            with self.lock:
                delay = due - (time.time() + self.offset)
                if delay <= 0:
                    return
                self.waiters[changed] = loop
            timer = loop.call_later(delay, changed.set)
            try:
                await changed.wait()
            finally:
                timer.cancel()
                with self.lock:
                    del self.waiters[changed]

    def transact(self, fn: Callable[[_DB], T]) -> T:
        with self.lock, self.conn:
            return fn(_DB(self.conn))


_registry_lock = Lock()
_memory_stores: dict[str, _Memory] = {}


def _memory(url: str) -> _Memory:
    with _registry_lock:
        if url not in _memory_stores:
            _memory_stores[url] = _Memory()
        return _memory_stores[url]


def reset_memory(url: str | None) -> None:
    with _registry_lock:
        if url is None:
            _memory_stores.clear()
        else:
            _memory_stores.pop(url, None)


def advance_memory(url: str, seconds: float) -> None:
    _memory(url).advance(seconds)


def now(url: str) -> float:
    if url.startswith("memory://"):
        return _memory(url).clock()
    return time.time()


class _PgPool:
    """Small thread-safe LIFO connection pool built directly on psycopg 3."""

    def __init__(self, url: str, max_size: int) -> None:
        if type(max_size) is not int or max_size < 1:
            raise ValueError("pool_size must be a positive integer")
        self.url = url
        self.max_size = max_size
        self._slots = BoundedSemaphore(max_size)
        self._lock = Lock()
        self._idle: deque[psycopg.Connection[Any]] = deque()
        weakref.finalize(self, _close_connections, self._idle, self._lock)

    def _is_idle(self, conn: psycopg.Connection[Any]) -> bool:
        from psycopg.pq import TransactionStatus

        return not conn.closed and conn.info.transaction_status == TransactionStatus.IDLE

    def run(self, fn: Callable[[psycopg.Connection[Any]], T]) -> T:
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError("Postgres flow storage requires psycopg 3") from exc

        self._slots.acquire()
        conn: psycopg.Connection[Any] | None = None
        reused = False
        try:
            while True:
                with self._lock:
                    candidate = self._idle.pop() if self._idle else None
                if candidate is None:
                    break
                if self._is_idle(candidate):
                    conn = candidate
                    reused = True
                    break
                _discard(candidate)
            if conn is None:
                conn = psycopg.connect(self.url)
            try:
                return fn(conn)
            except psycopg.OperationalError:
                if not reused:
                    raise
                # A pooled idle connection can be severed by a server restart or idle timeout.
                _discard(conn)
                conn = psycopg.connect(self.url)
                return fn(conn)
        finally:
            if conn is not None:
                if self._is_idle(conn):
                    with self._lock:
                        self._idle.append(conn)
                else:
                    _discard(conn)
            self._slots.release()

    def close(self) -> None:
        _close_connections(self._idle, self._lock)


def _discard(conn: Any) -> None:
    import psycopg

    with contextlib.suppress(OSError, psycopg.Error):
        conn.close()


def _close_connections(idle: deque[Any], lock: Lock) -> None:
    with lock:
        conns = list(idle)
        idle.clear()
    for conn in conns:
        _discard(conn)


class RunStore:
    def __init__(self, url: str, *, pool_size: int = 4) -> None:
        self.url = url
        self._memory = _memory(url) if url.startswith("memory://") else None
        self._pool = None if self._memory is not None else _PgPool(url, pool_size)
        self._schema_ready = False
        self._schema_lock = Lock()

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()

    async def aclose(self) -> None:
        if self._pool is not None:
            await asyncio.to_thread(self._pool.close)

    def clock(self) -> float:
        return self._memory.clock() if self._memory is not None else time.time()

    async def wait_until(self, due: float) -> None:
        if self._memory is None:
            await asyncio.sleep(max(0.0, due - self.clock()))
        else:
            await self._memory.wait_until(due)

    def _postgres(self, fn: Callable[[_DB], T]) -> T:
        assert self._pool is not None

        def run(conn: psycopg.Connection[Any]) -> T:
            db = _DB(conn)
            if not self._schema_ready:
                with self._schema_lock:
                    if not self._schema_ready:
                        with conn.transaction():
                            if db.execute("SELECT to_regclass('flow_runs_lineage_unindexed')").fetchone() == (None,):
                                db.execute("SELECT pg_advisory_xact_lock(hashtext('spater_flow_schema'))")
                                if db.execute("SELECT to_regclass('flow_runs_lineage_unindexed')").fetchone() == (
                                    None,
                                ):
                                    for stmt in _SCHEMA:
                                        db.execute(stmt)
                                    db.execute("""ALTER TABLE flow_runs ADD COLUMN IF NOT EXISTS workflow_key text,
                                               ADD COLUMN IF NOT EXISTS rerun_source text, ADD COLUMN IF NOT EXISTS started_at double precision""")
                                    for stmt in _HISTORY_INDEXES:
                                        db.execute(stmt)
                                    db.index_old_runs()
                        self._schema_ready = True
            with conn.transaction():
                return fn(db)

        return self._pool.run(run)

    async def _transaction(self, fn: Callable[[_DB], T]) -> T:
        if self._memory is not None:
            return self._memory.transact(fn)
        return await settle(asyncio.to_thread(lambda: self._postgres(fn)))

    async def create(
        self,
        run: Record,
        definition_json: str,
        *,
        owner: Owner | None = None,
        graph: Record | None = None,
        fail_if_exists: bool = False,
    ) -> bool:
        if run["definition"] != hashlib.sha256(definition_json.encode()).hexdigest():
            raise ValueError("run definition digest does not match its JSON")
        child = run.get("parent_id") is not None
        if child != (owner is not None) or (not child and run["id"] != run["root_id"]):
            raise ValueError("a root owns itself and a child requires its parent's claim")
        if child and any(run.get(key) is None for key in ("parent_node", "parent_attempt", "item_index")):
            raise ValueError("a mapped child requires its parent node, attempt and item index")

        def create(db: _DB) -> bool:
            if owner is not None:
                if db.root(owner) != "running" or run["root_id"] != owner[0]:
                    raise ClaimLost("cannot admit a child under an inactive or different root")
                parent = db.run(run["parent_id"])
                if parent["root_id"] != owner[0] or parent["status"] != "running":
                    raise ClaimLost("parent is inactive or belongs to a different root")
            db.execute(
                """INSERT INTO flow_definitions (digest, definition_json, graph_json) VALUES (?, ?, ?)
                          ON CONFLICT (digest) DO NOTHING""",
                (run["definition"], definition_json, None if graph is None else _dump(graph)),
            )
            if graph is not None:
                db.execute(
                    "UPDATE flow_definitions SET graph_json = ? WHERE digest = ? AND graph_json IS NULL",
                    (_dump(graph), run["definition"]),
                )
            inserted = db.execute(
                """INSERT INTO flow_runs
                (id, root_id, definition, parent_id, parent_node, parent_attempt, item_index, status, record, workflow_key, started_at, rerun_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING""",
                (
                    run["id"],
                    run["root_id"],
                    run["definition"],
                    run.get("parent_id"),
                    run.get("parent_node"),
                    run.get("parent_attempt"),
                    run.get("item_index"),
                    run["status"],
                    _dump(run),
                    _dump(run["job"]),
                    run["started_at"],
                    _dump(run.get("rerun_of")),
                ),
            )
            if inserted.rowcount:
                return True
            if fail_if_exists:
                raise RunExistsError(f"run {run['id']!r} already exists")
            try:
                prior = db.run(run["id"])
            except KeyError:
                raise ValueError("mapped item already belongs to a different child run") from None
            if _lineage(prior) != _lineage(run):
                raise ValueError(f"run {run['id']!r} already exists with different input, definition or lineage")
            if json.dumps(prior.get("input"), sort_keys=True) != json.dumps(run.get("input"), sort_keys=True):
                raise InputConflict(run["id"], prior.get("input"))
            return False

        return await self._transaction(create)

    async def get(self, run_id: str) -> Record:
        return await self._transaction(lambda db: db.run(run_id))

    async def cancel(self, run_id: str) -> Record:
        """Atomically cancel a root run, fencing any active worker."""
        finished = self.clock()

        def cancel(db: _DB) -> Record:
            row = db.locked_row("SELECT generation, record FROM flow_runs WHERE id = ? AND root_id = id", (run_id,))
            if row is None:
                try:
                    existing = db.run(run_id)
                except KeyError:
                    raise KeyError(run_id) from None
                raise ValueError(f"cancel the root run {existing['root_id']!r} instead of mapped child {run_id!r}")
            generation, record = row
            root = _read(record)
            if root["status"] in _TERMINAL:
                return root
            if root.get("pending_failure"):
                db.execute("UPDATE flow_runs SET generation = ? WHERE id = ?", (int(generation) + 1, run_id))
                return db.finish_failure(root, finished)
            if "delivery" in root:
                root["delivery"] = {"expected": None}
            root.update(status="cancelled", finished_at=finished)
            db.execute("UPDATE flow_runs SET generation = ? WHERE id = ?", (int(generation) + 1, run_id))
            db.terminalize(root)
            db.cancel_descendants(run_id, finished)
            return root

        return await self._transaction(cancel)

    async def run_page(
        self,
        *,
        offset: int,
        limit: int,
        q: str = "",
        status: str | None = None,
        workflow: str | None = None,
        since: float | None = None,
        until: float | None = None,
        parent: str | None = None,
        rerun_of: str | None = None,
        node: str | None = None,
    ) -> tuple[list[Record], int]:
        def page(db: _DB) -> tuple[list[Record], int]:
            db.index_old_runs()
            where: list[LiteralString] = []
            args: list[Any] = []
            if parent is not None:
                where.append("parent_id = ?")
                args.append(parent)
                order: LiteralString = "parent_node, parent_attempt, item_index, id"
            else:
                if q:
                    where.append("(id = root_id OR id = ?)")
                    args.append(q if "\x00" not in q else None)
                else:
                    where.append("id = root_id")
                order = "started_at DESC, id DESC"
            if status is not None:
                where.append("status = ?")
                args.append(status)
            if rerun_of is not None:
                where.append("rerun_source <> 'null' AND rerun_source = ?")
                args.append(_dump(rerun_of))
            if node is not None:
                where.append("parent_node = ?")
                args.append(node)
            if workflow is not None:
                where.append("workflow_key = ?")
                args.append(_dump(workflow))
            if since is not None:
                where.append("started_at >= ?")
                args.append(since)
            if until is not None:
                where.append("started_at <= ?")
                args.append(until)
            if q:
                needle = "%" + _dump(q.lower())[1:-1].replace("!", "!!").replace("%", "!%").replace("_", "!_") + "%"
                where.append("(lower(id) LIKE ? ESCAPE '!' OR lower(workflow_key) LIKE ? ESCAPE '!')")
                args.extend((needle, needle))
            clause = " FROM flow_runs WHERE " + " AND ".join(where)
            params = tuple(args)
            count = db.execute("SELECT count(*)" + clause, params).fetchone()
            assert count is not None
            rows = db.execute(
                "SELECT record" + clause + " ORDER BY " + order + " LIMIT ? OFFSET ?", (*args, limit, offset)
            )
            return [_read(record) for (record,) in rows], int(count[0])

        return await self._transaction(page)

    async def latest_runs(self, workflow: str | None = None) -> list[Record]:
        def load(db: _DB) -> list[Record]:
            db.index_old_runs()
            if workflow is not None:
                rows = db.execute(
                    """SELECT record FROM flow_runs WHERE workflow_key = ?
                    ORDER BY started_at DESC, id DESC LIMIT 1""",
                    (_dump(workflow),),
                )
            else:
                rows = db.execute("""SELECT record FROM flow_runs WHERE id IN (
                    SELECT id FROM (SELECT id, row_number() OVER (PARTITION BY workflow_key
                        ORDER BY started_at DESC, id DESC) AS position FROM flow_runs) ranked WHERE position = 1)""")
            return [_read(record) for (record,) in rows]

        return await self._transaction(load)

    async def definition(self, digest: str) -> str:
        def load(db: _DB) -> str:
            row = db.execute("SELECT definition_json FROM flow_definitions WHERE digest = ?", (digest,)).fetchone()
            if row is None:
                raise KeyError(digest)
            return str(row[0])

        return await self._transaction(load)

    async def graph(self, digest: str) -> Record:
        def load(db: _DB) -> Record:
            row = db.execute("SELECT graph_json FROM flow_definitions WHERE digest = ?", (digest,)).fetchone()
            if row is None or row[0] is None:
                raise KeyError(digest)
            return _read(row[0])

        return await self._transaction(load)

    async def claim(self, root_id: str) -> int:
        def claim(db: _DB) -> int:
            row = db.locked_row("SELECT generation, record FROM flow_runs WHERE id = ? AND root_id = id", (root_id,))
            if row is None:
                raise KeyError(root_id)
            generation, record = row
            root = _read(record)
            if "delivery" in root:
                root["delivery"] = {"expected": None}
            db.execute(
                "UPDATE flow_runs SET generation = generation + 1, record = ? WHERE id = ?", (_dump(root), root_id)
            )
            return int(generation) + 1

        return await self._transaction(claim)

    async def claim_delivery(self, root_id: str, task_id: str) -> int | PendingWake | ObsoleteDelivery:
        """Fence broker claims by the expected delivery token, including genuine redeliveries."""

        def claim(db: _DB) -> int | PendingWake | ObsoleteDelivery:
            sql = "SELECT generation, record FROM flow_runs WHERE id = ? AND root_id = id"
            row = db.locked_row(sql, (root_id,))
            if row is None:
                raise KeyError(root_id)
            generation, record = row
            root = _read(record)
            if root["status"] in _TERMINAL:
                return ObsoleteDelivery()
            delivery = root.get("delivery", {})
            if task_id != delivery.get("expected", root_id):
                if task_id == delivery.get("previous"):
                    return PendingWake(delivery["due"])
                return ObsoleteDelivery()
            root["delivery"] = {"expected": task_id}
            db.execute(
                "UPDATE flow_runs SET generation = generation + 1, record = ? WHERE id = ?", (_dump(root), root_id)
            )
            return int(generation) + 1

        return await self._transaction(claim)

    async def park_delivery(self, root_id: str, task_id: str, next_id: str, due: float, owner: Owner) -> None:
        """Commit a continuation before publishing it. The previous token can only republish that wake."""
        if next_id == task_id:
            raise ValueError("continuation must have a new task ID")

        def park(db: _DB) -> None:
            if owner[0] != root_id or db.root(owner) != "running":
                raise ClaimLost("run is inactive or belongs to a different root")
            root = db.run(root_id)
            if root.get("delivery", {}).get("expected", root_id) != task_id:
                raise ClaimLost("another delivery owns this run")
            root["delivery"] = {"expected": next_id, "previous": task_id, "due": due}
            db.write_run(root)

        await self._transaction(park)

    async def delivery_watches(self, after: str) -> tuple[str, list[DeliveryWatch]]:
        def page(db: _DB) -> tuple[str, list[DeliveryWatch]]:
            rows = db.execute(
                """SELECT id, generation, record FROM flow_runs
                WHERE id = root_id AND status = 'running' AND id > ? ORDER BY id LIMIT 64""",
                (after,),
            ).fetchall()
            return (rows[-1][0] if len(rows) == 64 else ""), [watch for row in rows if (watch := _watch(row))]

        return await self._transaction(page)

    async def delivery_watch(self, root_id: str) -> DeliveryWatch | None:
        def load(db: _DB) -> DeliveryWatch | None:
            row = db.execute(
                "SELECT id, generation, record FROM flow_runs WHERE id = ? AND id = root_id AND status = 'running'",
                (root_id,),
            ).fetchone()
            return _watch(row) if row else None

        return await self._transaction(load)

    async def fail_delivery(self, watch: DeliveryWatch, error: str) -> None:
        finished = self.clock()

        def fail(db: _DB) -> None:
            row = db.locked_row(
                "SELECT id, generation, record FROM flow_runs WHERE id = ? AND id = root_id AND status = 'running'",
                (watch.root_id,),
            )
            if row is None or _watch(row) != watch:
                return
            root = _read(row[2])
            if root.get("pending_failure"):
                db.finish_failure(root, finished)
            else:
                root.update(
                    status="failed", finished_at=finished, node_id="$delivery", failure_kind="delivery", error=error
                )
                db.terminalize(root)
                db.cancel_descendants(watch.root_id, finished)

        await self._transaction(fail)

    async def update(self, run_id: str, fields: Record, owner: Owner) -> None:
        if any(key in fields for key in _IDENTITY):
            raise ValueError("cannot update run identity fields")

        def update(db: _DB) -> None:
            root_status = db.root(owner)
            run = db.run(run_id)
            if run["root_id"] != owner[0]:
                raise ClaimLost("run belongs to a different root")
            if run["status"] in _TERMINAL and all(run.get(k) == v for k, v in fields.items()):
                return
            if root_status in _TERMINAL or run["status"] in _TERMINAL:
                raise ClaimLost("run is already terminal")
            status = fields.get("status", run["status"])
            if root_status == "cancelling" and status not in ("cancelling", "cancelled"):
                raise ClaimLost("root cancellation has begun")
            if run_id == owner[0] and run.get("pending_failure") is not None:
                if status == "failed":
                    db.finish_failure(run, fields["finished_at"])
                    return
                if status in ("cancelling", "cancelled"):
                    return
                if status == "succeeded":
                    raise ClaimLost("root already has a committed failure")
            run.update(fields)
            if status not in _TERMINAL:
                db.write_run(run)
                return
            db.terminalize(run)
            if run_id == owner[0] and status in ("failed", "cancelled"):
                db.cancel_descendants(run_id, run["finished_at"])

        await self._transaction(update)

    async def finish_failure(self, root_id: str, owner: Owner, finished_at: float) -> Record:
        def finish(db: _DB) -> Record:
            status = db.root(owner)
            if owner[0] != root_id:
                raise ClaimLost("run belongs to a different root")
            root = db.run(root_id)
            if status == "failed":
                return root
            if status in ("succeeded", "cancelled"):
                raise ClaimLost("run is already terminal")
            return db.finish_failure(root, finished_at)

        return await self._transaction(finish)

    async def attempt(self, run_id: str, node_id: str, attempt: int) -> Record | None:
        def get(db: _DB) -> Record | None:
            row = db.execute(
                "SELECT record, payload FROM flow_node_runs WHERE run_id = ? AND node_id = ? AND attempt = ?",
                (run_id, node_id, attempt),
            ).fetchone()
            if row is None:
                return None
            return _read(row[0]) | (_read(row[1]) if row[1] is not None else {})

        return await self._transaction(get)

    async def put_attempt(self, run_id: str, node_id: str, attempt: int, fields: Record, owner: Owner) -> None:
        if any(key in fields for key in _ATTEMPT_KEY):
            raise ValueError("cannot update attempt identity fields")

        def put(db: _DB) -> None:
            if db.root(owner) != "running":
                raise ClaimLost("root is closed to new work")
            run = db.run(run_id)
            if run["root_id"] != owner[0] or run["status"] != "running":
                raise ClaimLost("run is inactive or belongs to a different root")
            key = run_id, node_id, attempt
            row = db.execute(
                "SELECT record, payload FROM flow_node_runs WHERE run_id = ? AND node_id = ? AND attempt = ?", key
            ).fetchone()
            record = dict(zip(_ATTEMPT_KEY, key)) if row is None else _read(row[0])
            payload = {} if row is None or row[1] is None else _read(row[1])
            for name, value in fields.items():
                (payload if name in _PAYLOAD else record)[name] = value
            db.execute(
                """INSERT INTO flow_node_runs (run_id, node_id, attempt, record, payload) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (run_id, node_id, attempt) DO UPDATE SET record = excluded.record, payload = excluded.payload""",
                (*key, _dump(record), _dump(payload) if payload else None),
            )

            if fields.get("status") == "failed":
                root = run if run_id == owner[0] else db.run(owner[0])
                if root.get("pending_failure") is None:
                    root["pending_failure"] = {"run_id": run_id, "node_id": node_id, "error": fields["error"]}
                    db.write_run(root)

        await self._transaction(put)

    async def attempts(self, run_id: str) -> list[Record]:
        return await self._transaction(lambda db: db.attempts(run_id))

    async def children(self, run_id: str) -> list[Record]:
        def get(db: _DB) -> list[Record]:
            rows = db.execute(
                "SELECT record FROM flow_runs WHERE parent_id = ? ORDER BY parent_node, parent_attempt, item_index",
                (run_id,),
            )
            return [_read(row[0]) for row in rows]

        return await self._transaction(get)


def open_store(url: str, *, pool_size: int = 4) -> RunStore:
    if not url.startswith(("memory://", "postgres://", "postgresql://")):
        raise ValueError("flow runs require a memory:// or Postgres broker URL")
    return RunStore(url, pool_size=pool_size)
