"""Operational reads and mutations over admitted and pending real workflows."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import timedelta

import pytest
from spater.flow import Builder, Job, Ref, Runner
from spater.flow._ops import ApiError, Operations


@dataclass
class Request:
    values: list[int | str]
    mode: str = "normal"


@dataclass
class Item:
    value: str


def items(request: Request) -> list[Item]:
    return [Item(str(value)) for value in request.values]


def describe(item: Item) -> str:
    print(f"describing {item.value}", flush=True)
    return "value " + item.value


@Job.define("ops_item")
def CHILD(f: Builder, item: Ref[Item]) -> Ref[str]:
    return f.step(describe, item)


@Job.define("ops_batch")
def PARENT(f: Builder, request: Ref[Request]) -> None:
    batch = f.step(items, request)
    f.map(CHILD, batch, name="mapped_items")


ALL = [PARENT, CHILD]


@pytest.fixture(params=["memory_url", "pg_url"], ids=["memory", "postgres"])
def ops_url(request):
    return request.getfixturevalue(request.param)


async def completed(ops, run_id):
    for _ in range(200):
        detail = await ops.run(run_id)
        if detail["run"]["status"] in ("succeeded", "failed"):
            return detail
        await asyncio.sleep(0.02)
    pytest.fail("workflow did not finish")


async def test_launch_pending_detail_pagination_rerun_and_retired_snapshot(ops_url, monkeypatch):
    ops = Operations(ops_url, ALL)
    first = (await ops.launch(PARENT.name, {"values": list(range(9)) + ["A"]}))["run_id"]
    pending = await ops.run(first)
    assert pending["run"]["status"] == "running" and pending["input"]["mode"] == "normal"
    assert pending["attempts"] == [] and await ops.children(first) == {"items": [], "total": 0}
    assert ops.app.inspector.get_queue_info("spater-ops").size == 1

    async with ops:
        row = await completed(ops, first)
        assert row["run"]["status"] == "succeeded"
        by_node = {a["node_id"]: a for a in row["attempts"]}
        assert by_node["mapped_items"]["preview"]["value"][-1] == "value A"
        assert by_node["mapped_items"]["preview"]["omitted_root"] == 0
        assert "type" not in row["input"]
        children = await ops.children(first, node="mapped_items", offset=2, limit=3, status="succeeded")
        assert children["total"] == 10
        assert [item["item_index"] for item in children["items"]] == [2, 3, 4]
        chosen = children["items"][0]["id"]
        assert (await ops.run(chosen))["result"] == "value 2"
        assert (await ops.list_runs(q=chosen))["items"][0]["parent_id"] == first
        assert (await ops.list_runs(q="batch", status="succeeded", since=row["run"]["started_at"]))["total"] == 1
        assert (await ops.list_runs(q="%", until=row["run"]["finished_at"]))["total"] == 0
        assert (await ops.list_runs(workflow=PARENT.name, offset=10)) == {"items": [], "total": 1}
        assert (await ops.list_runs())["total"] == 1
        again = (await ops.rerun(first))["run_id"]
        assert again != first
        rerun = await completed(ops, again)
        assert rerun["run"]["rerun_of"] == first and rerun["input"] == row["input"]
        assert (await ops.children(first, node="invalid", status="failed"))["total"] == 0

        assert rerun["comparison"]["input_changed"] is False
        assert rerun["comparison"]["steps"] == []
        assert "2" in children["items"][0]["item_summary"]
        fields = (await ops.workflow(PARENT.name))["input_fields"]
        assert [(f["name"], f["required"]) for f in fields] == [("values", True), ("mode", False)]
        with pytest.raises(ApiError, match="Missing required field: values"):
            await ops.rerun(first, {})
        with pytest.raises(ApiError):
            await ops.rerun(first, None)
        edited_id = (await ops.rerun(first, {"values": ["changed"]}))["run_id"]
        edited = await completed(ops, edited_id)
        assert edited["run"]["rerun_of"] == first
        assert edited["comparison"]["input_changed"] and not edited["comparison"]["definition_changed"]
        assert edited["comparison"]["input_changes"][0] == {"path": "/values/0", "before": "0", "after": '"changed"'}
        assert {step["node"] for step in edited["comparison"]["steps"]} == {"items", "mapped_items"}
        edited_by_node = {a["node_id"]: a for a in edited["attempts"]}
        assert edited_by_node["items"]["input_preview"]["value"]["request"]["values"] == ["changed"]
        lineage = await ops.list_runs(rerun_of=first, limit=1)
        assert lineage["total"] == 2 and lineage["items"][0]["id"] == edited_id
        assert (await ops.list_runs(rerun_of=edited_id))["total"] == 0

    async with Operations(ops_url, []) as retired:

        async def forbid_children(*args):
            pytest.fail("GET /runs/id loaded all child runs")

        monkeypatch.setattr(retired.store, "children", forbid_children)
        assert (await retired.run(first))["graph"] == row["graph"]
        catalogue = await retired.workflow(PARENT.name)
        assert catalogue["graph"] == row["graph"]
        assert catalogue["workflow"]["launchable"] is False
        assert catalogue["nodes"] == [
            {"id": "items", "callable": f"{__name__}:items"},
            {"id": "mapped_items", "callable": "ops_item"},
        ]
        assert {it["name"] for it in (await retired.workflows())["items"]} == {CHILD.name, PARENT.name}
        with pytest.raises(ApiError) as unregistered:
            await retired.rerun(first)
        assert unregistered.value.status == 404


async def test_history_from_external_runner_and_input_errors(ops_url):
    async with Runner(ops_url, preview_bytes=6) as runner:
        outcome = await runner.run(CHILD, Item("abcdef\0ghijklmnopqrstuvwxyz"))
    async with Operations(ops_url, ALL) as ops:
        detail = await ops.run(outcome.run_id)
        sample = detail["attempts"][0]["preview"]
        assert detail["attempts"][0]["input_preview"]["truncated"]
        assert sample["truncated"] and len(sample["value"]) < 10
        assert detail["result"] == "value abcdef\0ghijklmnopqrstuvwxyz"
        assert (await ops.workflow(CHILD.name))["workflow"]["latest"]["id"] == outcome.run_id
        for op, args, expected in [
            ("launch", {"name": PARENT.name, "input": {"values": {}}}, 422),
            ("launch", {"name": PARENT.name, "input": {"values": [None]}}, 422),
            ("launch", {"name": "absent", "input": None}, 404),
            ("run", {"id": "absent"}, 404),
            ("children", {"id": "absent"}, 404),
            ("list_runs", {"limit": -1}, 400),
        ]:
            response = await ops.dispatch({"op": op, "args": args})
            assert response["error"]["status"] == expected, response
        assert (await ops.list_runs(status="running"))["total"] == 0


async def test_subprocess_reserves_stdout_for_frames(tmp_path):
    (tmp_path / "print_jobs.py").write_text(
        "import os\nfrom spater.flow import Builder, Job, Ref\n"
        "os.write(1, b'importing print_jobs\\n')\n"
        "def echo(text: str) -> str:\n"
        "    print('ordinary output', flush=True)\n"
        "    os.write(1, b'native output\\n')\n    return text.upper()\n"
        "@Job.define('echo')\n"
        "def ECHO(f: Builder, text: Ref[str]) -> None:\n"
        "    f.step(echo, text)\n"
        "ALL = [ECHO]\n"
    )
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join((str(tmp_path), os.environ.get("PYTHONPATH", ""))),
        "SPATER_OPS_JOBS": "print_jobs:ALL",
        "SPATER_OPS_STORE": "memory://spater-ops",
        "SPATER_OPS_DEMO": "1",
    }
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "spater.flow._ops",
        env=env,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdin, stdout, stderr = child.stdin, child.stdout, child.stderr
    assert stdin is not None and stdout is not None and stderr is not None

    async def call(op, **args):
        stdin.write((json.dumps({"op": op, "args": args}) + "\n").encode())
        await stdin.drain()
        line = await asyncio.wait_for(stdout.readline(), 5)
        assert line
        return json.loads(line)["ok"]

    try:
        assert await call("list_runs") == {"items": [], "total": 0}
        assert [j["name"] for j in (await call("workflows"))["items"]] == ["echo"]
        launched = await call("launch", name="echo", input="hello")
        for _ in range(200):
            record = await call("run", id=launched["run_id"])
            if record["run"]["status"] == "succeeded":
                break
            await asyncio.sleep(0.02)
        assert record["attempts"][0]["preview"]["value"] == "HELLO"
    finally:
        stdin.close()
        await asyncio.wait_for(child.wait(), 10)
    assert "native output" in (await stderr.read()).decode()


async def test_concurrent_postgres_upgrade_and_older_writer(pg_url):
    import psycopg
    from spater.flow._run_store import open_store

    async with Runner(pg_url) as runner:
        prior = await runner.run(CHILD, Item("preserve\0this"))
        row = await runner.store.get(prior.run_id)

    insert_old = """INSERT INTO flow_runs(id,root_id,definition,status,record)
                    VALUES (%s,%s,%s,'succeeded',%s)"""

    def old_row(number):
        run_id = f"legacy-{number}"
        data = {**row, "id": run_id, "root_id": run_id, "started_at": number, "rerun_of": prior.run_id}
        return run_id, run_id, row["definition"], json.dumps(data)

    with psycopg.connect(pg_url) as conn:
        conn.execute("ALTER TABLE flow_runs DROP COLUMN workflow_key CASCADE, DROP COLUMN started_at CASCADE")
        with conn.cursor() as cur:
            cur.executemany(insert_old, [old_row(i) for i in range(139)])

    stores = [open_store(pg_url) for _ in range(4)]
    pages = await asyncio.gather(*(db.run_page(offset=0, limit=3, workflow=CHILD.name) for db in stores))
    assert all(total == 140 and page[0] == row for page, total in pages)
    with psycopg.connect(pg_url) as conn:
        assert conn.execute("SELECT count(*) FROM flow_runs WHERE workflow_key IS NULL").fetchone() == (0,)
        conn.execute(insert_old, old_row(144))
    page, total = await stores[0].run_page(offset=0, limit=5, workflow=CHILD.name)
    assert total == 141 and page[1]["id"] == "legacy-144"
    assert (await stores[1].latest_runs(CHILD.name))[0]["input"] == row["input"]
    lineage, total = await stores[0].run_page(offset=0, limit=1, rerun_of=prior.run_id)
    assert total == 140 and lineage[0]["id"] == "legacy-144"


async def test_queued_launch_resumes_in_restarted_operations_worker(ops_url):
    @Job.define("deferred_report")
    def job(f: Builder, item: Ref[Item]) -> Ref[str]:
        waited = f.sleep(timedelta(seconds=1))
        return f.step(describe, item, after=waited)

    async with Operations(ops_url, [job]) as first:
        run_id = (await first.launch(job.name, {"value": "retained"}))["run_id"]
        for _ in range(80):
            if first.app.inspector.list_scheduled_tasks("spater-ops"):
                break
            await asyncio.sleep(0.015)
        live = await first.run(run_id)
        assert live["run"]["status"] == "running" and live["run"]["wake_at"] > live["run"]["started_at"]
    async with Operations(ops_url, [job]) as replacement:
        result = await completed(replacement, run_id)
        assert result["run"]["id"] == run_id and result["result"] == "value retained"
        assert [a["node_id"] for a in result["attempts"]] == ["describe", "sleep"]
        assert (await replacement.list_runs())["total"] == 1
