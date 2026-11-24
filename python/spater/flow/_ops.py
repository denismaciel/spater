"""Persistent JSON-lines application adapter for the local operations console."""

from __future__ import annotations

import asyncio
import dataclasses
import enum
import json
import logging
import math
import os
import sys
import typing
from collections.abc import Iterator, Sequence
from dataclasses import asdict
from itertools import chain, islice
from typing import Any, TextIO

from spater.app import Asynq
from spater.errors import QueueNotFoundError, TaskNotFoundError

from ._nodes import FlowError, Job
from ._run_queue import FlowQueue, _Delivery
from ._run_store import Record
from ._run_values import RunValueError, _Codec, decode, preview
from ._types import ListOf, Ty, normalize, resolve

if typing.TYPE_CHECKING:
    from typing_extensions import Self

log = logging.getLogger(__name__)

DEFAULT_STORE = "memory://spater-ops"
DEFAULT_JOBS = "flows.jobs:ALL"
QUEUE = "spater-ops"
_SAVED_INPUT = object()
_ABSENT_FIELD = object()
_STATUSES = {"running", "cancelling", "cancelled", "failed", "succeeded"}
_ATTEMPT_FIELDS = ("node_id", "attempt", "status", "started_at", "finished_at", "due", "deadline", "error", "arm")


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


class _Input(_Codec):
    """Resolve raw JSON against declared alternatives, including nested unions."""

    def __init__(self) -> None:
        super().__init__(reading=True)

    def value(self, value: Any, ty: Ty, path: str) -> Any:
        if len(ty.alts) == 1:
            return self.atom(value, next(iter(ty.alts)), path)
        if type(value) in ty.alts:
            return self.atom(value, type(value), path)
        matches = []
        for atom in ty.alts:
            try:
                matches.append(self.atom(value, atom, path))
            except (TypeError, ValueError):
                continue
        if len(matches) != 1:
            raise RunValueError(f"{path or '/'}: input must match exactly one variant of {ty}")
        return matches[0]

    def atom(self, value: Any, atom: Any, path: str) -> Any:
        if atom in (str, int, float, bool) and not isinstance(value, atom):
            raise RunValueError(f"{path.lstrip('/') or 'Input'}: expected {_input_kind(Ty.of(atom))}")
        if hasattr(atom, "model_validate_json"):
            from pydantic import ValidationError

            try:
                return super().atom(value, atom, path)
            except ValidationError as exc:
                errors = []
                for field in exc.errors(include_url=False, include_input=False):
                    location = ".".join(filter(None, [path.lstrip("/"), *(str(x) for x in field["loc"])]))
                    errors.append(f"{location or 'Input'}: {field['msg']}")
                raise RunValueError("; ".join(errors)) from exc
        return super().atom(value, atom, path)

    def record(self, value: Any, cls: Any, path: str) -> Any:
        if not isinstance(value, dict):
            raise RunValueError(f"{path or '/'}: expected an object for {cls.__name__}")
        fields = {f.name: f for f in dataclasses.fields(cls) if f.init}
        for name in value.keys() - fields.keys():
            raise RunValueError(f"Unknown field: {(path + '/' + name).lstrip('/')}")
        for f in fields.values():
            if f.name not in value and f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING:
                raise RunValueError(f"Missing required field: {(path + '/' + f.name).lstrip('/')}")
        hints = typing.get_type_hints(cls)
        return cls(**{k: self.value(v, normalize(hints[k]), f"{path}/{k}") for k, v in value.items()})


def _example(ty: Ty, parents: tuple[Any, ...] = ()) -> Any:
    for atom in sorted(ty.alts, key=str):
        if atom in parents:
            continue
        if isinstance(atom, typing.NewType):
            return _example(normalize(atom.__supertype__), (*parents, atom))
        if isinstance(atom, ListOf) or atom in (list,):
            return []
        origin = typing.get_origin(atom)
        args = typing.get_args(atom)
        if origin is dict:
            return {} if args[0] is str else []
        if origin is tuple and args[-1:] != (Ellipsis,):
            return [_example(normalize(item), parents) for item in args]
        if origin in (tuple, set, frozenset):
            return []
        if atom in (dict, str, int, float, bool):
            return atom()
        if isinstance(atom, type) and issubclass(atom, enum.Enum):
            return 0 if issubclass(atom, enum.Flag) else next(iter(atom.__members__), None)
        if dataclasses.is_dataclass(atom) and isinstance(atom, type):
            hints = typing.get_type_hints(atom)
            return {
                f.name: _example(normalize(hints[f.name]), (*parents, atom)) for f in dataclasses.fields(atom) if f.init
            }
        if hasattr(atom, "model_json_schema"):
            schema = atom.model_json_schema()
            return schema.get(
                "example", {k: v.get("default", v.get("example")) for k, v in schema.get("properties", {}).items()}
            )
        return None
    return None


def _summary(row: Record) -> Record:
    result: Record = {
        k: row[k]
        for k in (
            "id",
            "status",
            "started_at",
            "finished_at",
            "parent_id",
            "parent_node",
            "item_index",
            "rerun_of",
            "node_id",
            "error",
        )
        if k in row
    }
    result.update(workflow=row["job"], version=row["definition"][:12])
    if row.get("parent_id") and "input" in row:
        sample = preview(row["input"], 120)
        value = sample["value"]
        result["item_summary"] = json.dumps(value, ensure_ascii=False) + ("…" if sample["truncated"] else "")
    if row["status"] == "running" and (due := row.get("delivery", {}).get("due")) is not None:
        result["wake_at"] = due
    return result


def _graph(graph: Record) -> Record:
    return {"nodes": graph["nodes"], "edges": graph["edges"]}


def _attempt(row: Record) -> Record:
    result = {key: row[key] for key in _ATTEMPT_FIELDS if key in row}
    for key in ("preview", "input_preview"):
        if sample := row.get(key):
            result[key] = {**sample, "omitted_root": sample.get("omitted_keys", 0)}
            result[key].pop("omitted_keys", None)
    return result


def _boundary(value: Any) -> str:
    if isinstance(value, dict):
        return " | ".join(_boundary(t) for t in value["union"])
    return value.rsplit(":", 1)[-1] if value and value != "none" else "None"


def _duration(value: Any) -> str:
    if isinstance(value, dict):
        if "fn" in value:
            return str(value["fn"])
        return str(value["microseconds"]) + " μs"
    return f"{value} s"


def _node_details(scope: Record) -> Iterator[Record]:
    for node in scope.get("nodes", []):
        kind = node.get("kind")
        if kind == "fail":
            continue
        info: Record = {"id": node["id"]}
        if kind == "step":
            info["callable"] = node["fn"]
            if node.get("timeout") is not None:
                info["timeout"] = _duration(node["timeout"])
            if retry := node.get("retry"):
                on = retry["on"]
                targets = " | ".join(_boundary(t) for t in (on if isinstance(on, list) else [on]))
                info["retry"] = (
                    f"{retry['max']} attempts on {targets}, after {_duration(retry['after'])}, backoff ×{retry['backoff']}"
                )
            yield info
        elif kind == "sleep":
            dur = node.get("duration")
            if isinstance(dur, dict) and "fn" in dur:
                info["callable"] = dur["fn"]
            yield info
        elif kind == "map":
            info["callable"] = node["job"]["name"]
            yield info
        elif kind == "branch":
            yield info
            for arm in node.get("cases", []):
                yield from _node_details(arm["scope"])


def _input_kind(ty: Ty) -> str:
    names = {str: "string", bool: "boolean", int: "integer", float: "number", type(None): "null"}
    return " or ".join(
        sorted(
            "array of " + _input_kind(atom.item) if isinstance(atom, ListOf) else names.get(atom, Ty.of(atom).text())
            for atom in ty.alts
        )
    )


def _field_guide(ty: Ty, prefix: str = "", seen: tuple[Any, ...] = ()) -> Iterator[Record]:
    for atom in sorted(ty.alts, key=str):
        if atom in seen:
            continue
        if isinstance(atom, ListOf):
            yield from _field_guide(atom.item, prefix + "[]", (*seen, atom))
        elif dataclasses.is_dataclass(atom) and isinstance(atom, type):
            hints = typing.get_type_hints(atom)
            for field in dataclasses.fields(atom):
                if not field.init:
                    continue
                name = f"{prefix}.{field.name}" if prefix else field.name
                field_ty = normalize(hints[field.name])
                yield {
                    "name": name,
                    "kind": _input_kind(field_ty),
                    "required": field.default is dataclasses.MISSING and field.default_factory is dataclasses.MISSING,
                }
                yield from _field_guide(field_ty, name, (*seen, atom))
        elif hasattr(atom, "model_json_schema"):
            schema = atom.model_json_schema()
            for name, info in schema.get("properties", {}).items():
                yield {
                    "name": f"{prefix}.{name}" if prefix else name,
                    "kind": info.get("type", info.get("title", "value")),
                    "required": name in schema.get("required", []),
                }


def _input_changes(before: Any, after: Any, path: str = "") -> Iterator[Record]:
    missing = _ABSENT_FIELD
    if isinstance(before, dict) and isinstance(after, dict):
        for key in chain(before, (k for k in after if k not in before)):
            escaped = key.replace("~", "~0").replace("/", "~1")
            yield from _input_changes(before.get(key, missing), after.get(key, missing), f"{path}/{escaped}")
    elif isinstance(before, list) and isinstance(after, list):
        for i in range(max(len(before), len(after))):
            yield from _input_changes(
                before[i] if i < len(before) else missing, after[i] if i < len(after) else missing, f"{path}/{i}"
            )
    elif type(before) is not type(after) or before != after:

        def display(value: Any) -> str | None:
            if value is missing:
                return None
            sample = preview({"value": value}, 350)
            return json.dumps(sample["value"], ensure_ascii=False) + ("…" if sample["truncated"] else "")

        yield {"path": path or "/", "before": display(before), "after": display(after)}


def _comparison(source: Record, current: Record, before: list[Record], after: list[Record]) -> Record:
    def outcomes(rows: list[Record]) -> dict[str, Record]:
        return {a["node_id"]: a for a in sorted(rows, key=lambda a: a["attempt"])}

    def signature(attempt: Record | None) -> str:
        fields = ("attempt", "status", "arm", "error", "preview")
        return json.dumps({k: attempt.get(k) for k in fields}, sort_keys=True) if attempt else ""

    old, new = outcomes(before), outcomes(after)
    input_changed = json.dumps(source["input"], sort_keys=True) != json.dumps(current["input"], sort_keys=True)
    changes = (
        list(islice(_input_changes(source["input"]["value"], current["input"]["value"]), 21)) if input_changed else []
    )
    return {
        "input_changes": changes[:20],
        "more_input_changes": len(changes) > 20,
        "source_status": source["status"],
        "source_failure": source.get("error"),
        "source_node": source.get("node_id"),
        "definition_changed": source["definition"] != current["definition"],
        "input_changed": input_changed,
        "steps": [
            {"node": name, "before": old.get(name), "after": new.get(name)}
            for name in sorted(old.keys() | new.keys())
            if signature(old.get(name)) != signature(new.get(name))
        ],
    }


class Operations:
    def __init__(self, url: str, jobs: Sequence[Job[Any, Any]]):
        self.jobs = {job.name: job for job in jobs}
        if len(self.jobs) != len(jobs):
            raise ValueError("workflow registry contains duplicate names")
        self.app = Asynq(url)
        self.queue = FlowQueue(self.app, jobs)
        self.store = self.queue.runner.store
        self.worker = self.app.worker(
            queues=[QUEUE], install_signal_handlers=False, task_check_interval=0.1, delayed_task_check_interval=0.25
        )
        self.worker_task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> Self:
        self.worker_task = asyncio.create_task(self.worker.run())
        while self.worker.status != "active":
            if self.worker_task.done():
                self.worker_task.result()
                raise RuntimeError("operations worker stopped during startup")
            await asyncio.sleep(0.01)
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.worker.shutdown()
        try:
            if self.worker_task:
                await self.worker_task
        finally:
            await self.queue.aclose()

    async def run(self, id: str) -> Record:
        try:
            row = await self.store.get(id)
        except KeyError:
            pending = await self._pending(id)
            if pending is not None:
                return pending
            try:
                row = await self.store.get(id)
            except KeyError:
                raise ApiError(404, f"Run {id!r} was not found") from None
        row = await self.queue.runner._inspect_row(row)
        data: Record = {
            "run": _summary(row),
            "graph": _graph(await self.store.graph(row["definition"])),
            "attempts": [_attempt(a) for a in await self.store.attempts(id)],
            "input": row["input"]["value"],
        }
        data["input_json"] = json.dumps(data["input"], ensure_ascii=False, indent=2)
        if source_id := row.get("rerun_of"):
            try:
                source = await self.store.get(source_id)
            except KeyError:
                pass
            else:
                prior = [_attempt(a) for a in await self.store.attempts(source_id)]
                data["comparison"] = _comparison(source, row, prior, data["attempts"])
        if "result" in row:
            data["result"] = row["result"]["value"]
        return data

    async def _pending(self, id: str) -> Record | None:
        try:
            info = await self.app.inspector.aget_task_info(QUEUE, id)
        except (TaskNotFoundError, QueueNotFoundError):
            return None
        if info.type != self.queue._task.name:
            return None
        delivery = self.queue._task.codec.decode(info.payload)
        if not isinstance(delivery, _Delivery) or id != delivery.run_id:
            return None
        job = self.jobs.get(delivery.job)
        if job is None or job.version != delivery.version[:12]:
            raise ApiError(409, "Waiting for a worker with the submitted definition version")
        submitted = delivery.submitted_at
        if submitted is None:
            observed = info.last_failed_at or info.next_process_at
            if observed is None:
                raise ApiError(409, "Waiting for initial admission of a delivery without a submission timestamp")
            submitted = observed.timestamp()
        summary = {
            "id": id,
            "workflow": job.name,
            "version": job.version,
            "started_at": submitted,
            "rerun_of": delivery.rerun_of,
            "status": "failed" if info.state == "archived" else "running",
        }
        if info.state == "archived":
            summary.update(
                error=info.last_err,
                node_id="$delivery",
                finished_at=info.last_failed_at.timestamp() if info.last_failed_at else None,
            )
        return {"run": summary, "graph": _graph(asdict(job.graph())), "attempts": [], "input": delivery.input["value"]}

    async def list_runs(
        self,
        *,
        q: str = "",
        rerun_of: str | None = None,
        status: str | None = None,
        workflow: str | None = None,
        since: float | None = None,
        until: float | None = None,
        offset: int = 0,
        limit: int = 50,
    ) -> Record:
        return await self._page(
            offset, limit, status, q=q, rerun_of=rerun_of, workflow=workflow, since=since, until=until
        )

    async def children(
        self, id: str, *, node: str | None = None, status: str | None = None, offset: int = 0, limit: int = 50
    ) -> Record:
        try:
            await self.store.get(id)
        except KeyError:
            if await self._pending(id) is None:
                raise ApiError(404, f"Run {id!r} was not found") from None
        return await self._page(offset, limit, status, parent=id, node=node)

    async def _page(self, offset: int, limit: int, status: str | None, **params: Any) -> Record:
        if type(offset) is not int or not 0 <= offset <= 2**63 - 1 or type(limit) is not int or not 1 <= limit <= 200:
            raise ApiError(400, "offset must be nonnegative and limit must be between 1 and 200")
        if status is not None and status not in _STATUSES:
            raise ApiError(400, "Unknown run status")
        for key in ("since", "until"):
            if (n := params.get(key)) is not None and (type(n) not in (float, int) or not math.isfinite(n)):
                raise ApiError(400, f"{key} must be a finite Unix timestamp")
        rows, total = await self.store.run_page(offset=offset, limit=limit, status=status, **params)
        return {"items": [_summary(r) for r in rows], "total": total}

    def _job(self, name: str) -> Job[Any, Any]:
        if name not in self.jobs:
            raise ApiError(404, f"Workflow {name!r} is not registered")
        return self.jobs[name]

    async def _workflow(self, name: str, latest: Record | None, *, detail: bool) -> Record:
        job = self.jobs.get(name)
        reason = None
        if job is None:
            if latest is None:
                raise ApiError(404, f"Workflow {name!r} was not found")
            definition = json.loads(await self.store.definition(latest["definition"]))
            reason = "This historical workflow is no longer registered."
            launchable, version = False, latest["definition"][:12]
        else:
            definition, version = job.to_dict(), job.version
            launchable = True
        summary: Record = {
            "name": name,
            "version": version,
            "input_type": _boundary(definition["input"]),
            "output_type": _boundary(definition["output"]),
            "launchable": launchable,
            "launch_reason": reason,
            "latest": _summary(latest) if latest else None,
        }
        if not detail:
            return summary
        graph = asdict(job.graph()) if job else await self.store.graph(typing.cast(Record, latest)["definition"])
        example = (
            latest["input"]["value"]
            if latest and latest["definition"][:12] == version
            else (_example(normalize(job.input)) if job else None)
        )
        return {
            "workflow": summary,
            "definition": definition,
            "graph": _graph(graph),
            "example_input": example,
            "input_fields": list(_field_guide(normalize(job.input))) if job else [],
            "example_note": (
                f"Example input from a {latest['status']} run"
                + (f": {latest['error']}" if latest.get("error") else ".")
            )
            if latest and latest["definition"][:12] == version
            else "Generated from the input type.",
            "nodes": list(_node_details(definition["scope"])),
        }

    async def workflows(self) -> Record:
        latest = {row["job"]: row for row in await self.store.latest_runs()}
        return {
            "items": [
                await self._workflow(name, latest.get(name), detail=False)
                for name in sorted(self.jobs.keys() | latest.keys())
            ]
        }

    async def workflow(self, name: str) -> Record:
        latest = await self.store.latest_runs(name)
        return await self._workflow(name, latest[0] if latest else None, detail=True)

    def _validate_input(self, job: Job[Any, Any], input: Any) -> Any:
        try:
            return _Input().value(input, normalize(job.input), "")
        except (TypeError, ValueError, OverflowError) as exc:
            raise ApiError(422, f"Invalid input for {job.name}: {exc}") from exc

    async def launch(self, name: str, input: Any) -> Record:
        job = self._job(name)
        return await self._enqueue(job, self._validate_input(job, input))

    async def rerun(self, id: str, input: Any = _SAVED_INPUT) -> Record:
        try:
            row = await self.store.get(id)
        except KeyError:
            pending = await self._pending(id)
            if pending is None:
                raise ApiError(404, f"Run {id!r} was not found") from None
            raise ApiError(409, "Wait for the initial delivery to be admitted before rerunning") from None
        job = self._job(row["job"])
        if input is not _SAVED_INPUT:
            return await self._enqueue(job, self._validate_input(job, input), rerun_of=id)
        try:
            value = decode(row["input"], normalize(job.input))
        except ValueError as exc:
            raise ApiError(422, f"Stored input is incompatible with the registered definition: {exc}") from exc
        return await self._enqueue(job, value, rerun_of=id)

    async def _enqueue(self, job: Job[Any, Any], value: Any, rerun_of: str | None = None) -> Record:
        try:
            run_id = await self.queue._enqueue(job, input=value, queue=QUEUE, rerun_of=rerun_of)
        except FlowError as exc:
            raise ApiError(409, str(exc)) from exc
        except RunValueError as exc:
            raise ApiError(422, str(exc)) from exc
        return {"run_id": run_id}

    async def dispatch(self, request: Any) -> Record:
        operations = {"list_runs", "run", "children", "workflows", "workflow", "launch", "rerun"}
        try:
            if (
                not isinstance(request, dict)
                or request.get("op") not in operations
                or not isinstance(request.get("args"), dict)
            ):
                raise ApiError(400, "Invalid adapter request")
            payload = await getattr(self, request["op"])(
                **{k: v for k, v in request["args"].items() if v is not None or k == "input"}
            )
            return {"ok": payload}
        except ApiError as exc:
            return {"error": {"status": exc.status, "detail": str(exc)}}
        except Exception:
            log.exception("Operations request failed")
            return {"error": {"status": 500, "detail": "Operations request failed"}}


async def _serve(out: TextIO) -> None:
    url, registry = os.environ.get("SPATER_OPS_STORE", DEFAULT_STORE), os.environ.get("SPATER_OPS_JOBS", DEFAULT_JOBS)
    jobs = list(resolve(registry))
    demo = os.environ.get("SPATER_OPS_DEMO") == "1" and url == DEFAULT_STORE and registry == DEFAULT_JOBS
    if demo:
        from importlib import import_module

        examples = import_module("flows.operations")
        jobs.append(examples.BATCH)
    async with Operations(url, jobs) as ops:
        if demo:
            await examples.seed(ops.queue.runner)
        while line := await asyncio.to_thread(sys.stdin.readline):
            try:
                response = await ops.dispatch(json.loads(line))
            except ValueError:
                response = {"error": {"status": 400, "detail": "Invalid JSON"}}
            print(json.dumps(response, allow_nan=False, separators=(",", ":")), file=out, flush=True)


def main() -> None:
    with os.fdopen(os.dup(1), "w", encoding="utf-8") as protocol:
        os.dup2(2, 1)
        asyncio.run(_serve(protocol))


if __name__ == "__main__":
    main()
