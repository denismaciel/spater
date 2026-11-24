from __future__ import annotations

import dataclasses
import enum
import json
from typing import Any, Literal, NewType, cast

import pytest
from pydantic import BaseModel, ConfigDict, Field
from spater.flow._run_values import RunValueError, decode, encode, preview, selected_type
from spater.flow._types import normalize


class Status(enum.IntEnum):
    SENT = 1
    OPEN = 2


Reference = NewType("Reference", str)


@dataclasses.dataclass(frozen=True, slots=True)
class Recipient:
    address: str


@dataclasses.dataclass
class Carrier:
    address: str


@dataclasses.dataclass(frozen=True)
class Packet:
    contacts: list[Recipient | Carrier]
    groups: dict[str, tuple[Status, Reference]]
    scores: dict[int | str, set[int]]
    marks: frozenset[str]
    optional: Recipient | None
    sequence: tuple[int, ...]
    signature: str = dataclasses.field(init=False, default="recorded")


class Number(BaseModel):
    model_config = ConfigDict(extra="forbid")
    value: int = Field(alias="number")


class LabeledNumber(Number):
    label: str


def wire(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False))


def bytes_used(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())


def test_recovered_values_keep_nested_variants_and_typed_container_members() -> None:
    payload = Packet(
        [Recipient("same"), Carrier("same")],
        {"a~/": (Status.SENT, Reference("42"))},
        {1: {1, 2}, "1": {3}},
        frozenset({"x"}),
        None,
        (3, 4),
    )
    ty = normalize(Packet)
    stored = encode(payload, ty)
    value = decode(wire(stored), ty)
    assert value == payload
    assert isinstance(value.contacts[0], Recipient) and isinstance(value.contacts[1], Carrier)
    assert isinstance(value.groups["a~/"][0], Status)
    assert type(value.marks) is frozenset
    assert value.signature == "recorded"


def test_decoding_resolves_variants_only_from_declared_types() -> None:
    ty = normalize(list[Carrier | Recipient])
    stored = encode([Carrier("hi")], ty)
    stored["types"]["/0"] = "external_package:load"
    with pytest.raises(RunValueError, match="outside"):
        decode(stored, ty)


def test_typed_null_and_empty_lists_keep_their_declared_variant() -> None:
    ty = normalize(list[Carrier] | None)
    assert selected_type([], ty) == normalize(list[Carrier])
    assert selected_type(None, ty) == normalize(None)
    assert decode(wire(encode([], ty)), ty) == []
    assert decode(wire(encode(None, ty)), ty) is None


@pytest.mark.parametrize(
    ("value", "annotation"),
    [
        (1, bool),
        (["oops"], list[int]),
        ({3: "no string key"}, dict),
        ({"x": float("nan")}, dict),
        ({"x": object()}, dict),
        ({"x": "\udc00"}, dict),
        (Carrier(cast(Any, 4)), Carrier),
    ],
)
def test_invalid_inputs_fail_before_persistence(value: Any, annotation: Any) -> None:
    with pytest.raises(ValueError):
        encode(value, normalize(annotation))


def test_model_alias_and_invalid_constructed_model() -> None:
    ty = normalize(Number)
    assert decode(wire(encode(Number(number=2), ty)), ty) == Number(number=2)
    assert decode(wire(encode(LabeledNumber(number=2, label="two"), ty)), ty) == Number(number=2)
    with pytest.raises(ValueError):
        encode(Number.model_construct(value="wrong"), ty)


def test_preview_reserves_root_and_sibling_keys_before_expanding_values() -> None:
    value = {"body": "é🌵" * 100, "sibling": {"x": "x" * 400, "y": {"z": "z" * 400}}, "array": ["a" * 100] * 20}
    stored = encode(value, normalize(dict))
    info = preview(stored, 160)
    assert info["original_bytes"] == bytes_used(value)
    assert info["truncated"] and info["omitted_keys"] == 0
    shown = info["value"]
    assert list(shown) == list(value)
    assert list(shown["sibling"]) == ["x", "y"] and list(shown["sibling"]["y"]) == ["z"]
    assert 0 < len(shown["body"]) < len(value["body"])
    assert len(shown["array"]) > 0
    assert bytes_used(shown) <= 160
    assert decode(stored, normalize(dict)) == value


def test_large_root_key_is_skipped_so_later_root_keys_can_fit() -> None:
    value = {"x" * 40: None, "b": 1, "q" * 50: True, "a": 2}
    info = preview(encode(value, normalize(dict)), 13)
    assert info["value"] == {"b": 1, "a": 2}
    assert list(info["value"]) == ["b", "a"]
    assert info["omitted_keys"] == 2
    assert bytes_used(info["value"]) == 13


def test_preview_fits_each_utf8_boundary_including_escapes() -> None:
    value = {"🌵": ['"\\\n' * 15, "é" * 20], "\n": {"🙂": "☺️" * 30}, "boolean": False}
    data = encode(value, normalize(dict))
    for cap in range(1, bytes_used(value) + 1):
        info = preview(data, cap)
        assert bytes_used(info["value"]) <= cap
        assert info["truncated"] is (cap < bytes_used(value))
    assert preview(data, bytes_used(value))["value"] == value


@dataclasses.dataclass(frozen=True)
class Address:
    street: str


@dataclasses.dataclass(frozen=True)
class DeliveryAddress(Address):
    room: int


def test_declared_base_and_its_subclass_are_supported_as_outputs() -> None:
    from spater.flow import Builder, Job, Ref

    @Job.define("ship")
    def job(f: Builder, street: Ref[str]) -> Ref[Address]:
        return f.step(deliver, street)

    result = deliver("home")
    concrete = normalize(DeliveryAddress)
    assert decode(encode(result, concrete), concrete) == result
    assert decode(encode(result, normalize(job.output)), normalize(job.output)) == Address("home")
    assert decode(encode([result], normalize(list[Address])), normalize(list[Address])) == [Address("home")]
    assert decode(encode(True, normalize(int)), normalize(int)) is True


def deliver(street: str) -> DeliveryAddress:
    return DeliveryAddress(street, 2)


class Color(str, enum.Enum):
    RED = "red"


def test_str_enum_widening_preserves_underlying_string_and_declared_enum() -> None:
    assert decode(encode(Color.RED, normalize(str)), normalize(str)) == "red"
    assert decode(encode(Color.RED, normalize(Color)), normalize(Color)) is Color.RED
    ty = normalize(dict[str, list[str]])
    assert decode(encode({Color.RED: [Color.RED]}, ty), ty) == {"red": ["red"]}


@dataclasses.dataclass(frozen=True)
class Region:
    address: str


@dataclasses.dataclass(frozen=True)
class Destination:
    value: Recipient | Region
    labels: frozenset[str]


class ModelSets(BaseModel):
    members: list[set[str]]
    ordered: list[str]


@dataclasses.dataclass
class DeliveryBatch:
    groups: set[Destination]
    model: ModelSets


def unordered_sample() -> DeliveryBatch:
    labels = frozenset({"blue", "green", "red"})
    return DeliveryBatch(
        {
            Destination(Recipient("same"), labels),
            Destination(Region("same"), labels),
            Destination(Region("other"), labels),
        },
        ModelSets(members=[set(labels), {"b", "a"}], ordered=["z", "a"]),
    )


def test_typed_sets_and_variant_paths_are_stable_across_hash_seeds() -> None:
    import os
    import subprocess
    import sys
    from pathlib import Path

    script = (
        "from test_flow_run_values import unordered_sample, DeliveryBatch\n"
        "from spater.flow._run_values import encode\n"
        "from spater.flow._types import normalize\n"
        "import json\n"
        "print(json.dumps(encode(unordered_sample(), normalize(DeliveryBatch))))"
    )
    encodings = [
        subprocess.check_output(
            [sys.executable, "-c", script],
            env=os.environ
            | {
                "PYTHONHASHSEED": str(seed),
                "PYTHONPATH": os.pathsep.join([str(Path(__file__).resolve().parent), os.getenv("PYTHONPATH", "")]),
            },
            text=True,
        )
        for seed in (1, 2, 3)
    ]
    assert len(set(encodings)) == 1
    result = json.loads(encodings[0])
    assert result["value"]["model"] == {"members": [["blue", "green", "red"], ["a", "b"]], "ordered": ["z", "a"]}
    assert decode(result, normalize(DeliveryBatch)) == unordered_sample()


def legacy_batch() -> dict[str, Any]:
    data = encode(unordered_sample(), normalize(DeliveryBatch))
    # An older worker indexed the union tags in its set iteration order.
    groups = data["value"]["groups"]
    groups.reverse()
    tags = data["types"].copy()
    for i, group in enumerate(groups):
        data["types"][f"/groups/{i}/value"] = tags[f"/groups/{len(groups) - i - 1}/value"]
        group["labels"].reverse()
    for items in data["value"]["model"]["members"]:
        items.reverse()
    return data


def dispatch(batch: DeliveryBatch) -> list[DeliveryBatch]:
    return [batch]


def destination_count(batch: DeliveryBatch) -> int:
    return len(batch.groups)


@pytest.mark.parametrize("backend", ["memory_url", "pg_url"])
async def test_legacy_set_order_recovers_root_and_existing_mapped_child(
    backend: str, request: pytest.FixtureRequest
) -> None:
    from uuid import NAMESPACE_URL, uuid5

    from spater.flow import Builder, Job, Ref, Runner, Succeeded

    batch = unordered_sample()
    legacy = legacy_batch()
    assert decode(legacy, normalize(DeliveryBatch)) == batch
    assert encode(batch, normalize(DeliveryBatch)) != legacy

    @Job.define("count")
    def child(f: Builder, b: Ref[DeliveryBatch]) -> Ref[int]:
        return f.step(destination_count, b)

    @Job.define("dispatch")
    def parent(f: Builder, b: Ref[DeliveryBatch]) -> None:
        items_ref = f.step(dispatch, b)
        f.map(child, items_ref, name="send")

    root = "legacy-delivery"
    child_id = str(uuid5(NAMESPACE_URL, json.dumps([root, "send", 1, 0])))
    async with Runner(request.getfixturevalue(backend)) as runner:
        await runner._create(parent, root, legacy, root)
        owner = (root, await runner.store.claim(root))
        manifest = encode([batch], normalize(list[DeliveryBatch]))
        manifest["value"][0] = legacy["value"]
        manifest["types"] = {"/0" + key: tag for key, tag in legacy["types"].items()}
        await runner.store.put_attempt(root, "dispatch", 1, {"status": "succeeded", "output": manifest}, owner)
        await runner.store.put_attempt(root, "send", 1, {"status": "running", "manifest": manifest}, owner)
        await runner._create(child, child_id, legacy, root, owner=owner, parent=root, node="send", index=0)

        assert await runner.prepare(parent, input=batch, run_id=root) == root
        reordered = ModelSets(members=batch.model.members, ordered=list(reversed(batch.model.ordered)))
        with pytest.raises(ValueError, match="different input"):
            await runner.prepare(parent, input=dataclasses.replace(batch, model=reordered), run_id=root)
        outcome = await runner.deliver(parent, root)
        assert isinstance(outcome, Succeeded)
        row = await runner.store.get(child_id)
        assert row["status"] == "succeeded" and row["input"] == legacy
        assert decode(row["result"], normalize(int)) == len(batch.groups)


class IncrementOnRead(BaseModel):
    value: int
    labels: set[str]

    def model_post_init(self, context: Any) -> None:
        self.value += 1


def count_incremented(model: IncrementOnRead) -> int:
    return model.value


async def test_legacy_matching_preserves_raw_scalar_even_if_validation_changes_it(memory_url: str) -> None:
    from spater.flow import Builder, Job, Ref, Runner
    from spater.flow._run_values import canonical_input

    ty = normalize(IncrementOnRead)

    @Job.define("non_idempotent")
    def job(f: Builder, m: Ref[IncrementOnRead]) -> Ref[int]:
        return f.step(count_incremented, m)

    old_model = IncrementOnRead(value=1, labels={"x", "y"})
    legacy = encode(old_model, ty)
    legacy["value"]["labels"].reverse()
    assert decode(legacy, ty).value != legacy["value"]["value"]
    assert canonical_input(legacy, ty) == encode(old_model, ty)
    async with Runner(memory_url) as runner:
        await runner._create(job, "old", legacy, "old")
        assert await runner.prepare(job, input=old_model, run_id="old") == "old"
        with pytest.raises(ValueError, match="different input"):
            await runner.prepare(job, input=IncrementOnRead(value=2, labels=old_model.labels), run_id="old")


class NestedSetMember(BaseModel):
    model_config = ConfigDict(frozen=True)
    name: str
    tags: frozenset[str]
    ordered: tuple[str, ...]


class NestedModelSets(BaseModel):
    group: set[NestedSetMember]


def test_reorder_nested_pydantic_sets_keeps_original_member_associations() -> None:
    from spater.flow._run_values import canonical_input

    data = NestedModelSets(
        group={
            NestedSetMember(name="b", tags=frozenset({"b1", "b2"}), ordered=("z", "a")),
            NestedSetMember(name="a", tags=frozenset({"a1", "a2"}), ordered=("c", "b", "a")),
        }
    )
    ty = normalize(NestedModelSets)
    expected = encode(data, ty)
    old = wire(expected)
    old["value"]["group"].reverse()
    for raw in old["value"]["group"]:
        raw["tags"].reverse()
    assert canonical_input(old, ty) == expected
    old["value"]["group"][0]["ordered"].reverse()
    assert canonical_input(old, ty) != expected


@dataclasses.dataclass(frozen=True)
class WithLiteral:
    level: Literal["high", "medium", "low"] | None = None
    flag: Literal[True, 1] = 1


def test_literal_roundtrip_and_validation() -> None:
    from spater.flow._types import assignable, serializable

    ty = normalize(WithLiteral)
    assert serializable(ty)
    assert assignable(normalize(Literal["high"]), normalize(Literal["high", "low"]))
    assert assignable(normalize(Literal["high"]), normalize(str))
    assert not assignable(normalize(str), normalize(Literal["high"]))

    sample = WithLiteral(level="high", flag=True)
    raw = encode(sample, ty)
    assert decode(wire(raw), ty) == sample

    empty = WithLiteral(level=None, flag=1)
    assert decode(wire(encode(empty, ty)), ty) == empty

    with pytest.raises(RunValueError):
        encode(WithLiteral(level="nope", flag=1), ty)  # type: ignore[arg-type]
    with pytest.raises(RunValueError):
        decode({"type": raw["type"], "value": {"level": "nope", "flag": 1}, "types": raw["types"]}, ty)
