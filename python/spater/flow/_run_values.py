"""Persist values using the job's annotations as the only source of Python types."""

from __future__ import annotations

import copy
import dataclasses
import enum
import inspect
import json
import math
import re
import types
import typing
from collections import deque
from typing import Any

from ._types import ListOf, NoneType, Ty, normalize

_ENCODER = json.JSONEncoder(ensure_ascii=False, separators=(",", ":"), allow_nan=False)
_SORTED_ENCODER = json.JSONEncoder(ensure_ascii=False, separators=(",", ":"), allow_nan=False, sort_keys=True)
_PLAIN_ASCII_RE = re.compile(r"\A[\x20-\x21\x23-\x5b\x5d-\x7e]*\Z")
_DC_FIELDS: dict[type, dict[str, Ty]] = {}


class RunValueError(ValueError):
    """A run value does not meet its declared type or cannot round-trip through JSON."""


def _json(value: Any) -> str:
    return _ENCODER.encode(value)


def _order_key(value: Any) -> str:
    return _SORTED_ENCODER.encode(value)


def _str_size(value: str) -> int:
    if _PLAIN_ASCII_RE.match(value):
        return len(value) + 2
    return len(_ENCODER.encode(value).encode("utf-8"))


def _size(value: Any) -> int:
    t = type(value)
    if t is str:
        return _str_size(value)
    if value is None:
        return 4
    if t is bool:
        return 4 if value else 5
    if t is int:
        return len(str(value))
    return len(_ENCODER.encode(value).encode("utf-8"))


def _child(path: str, key: str | int) -> str:
    return f"{path}/{str(key).replace('~', '~0').replace('/', '~1')}"


def _require(condition: bool, path: str, message: str) -> None:
    if not condition:
        raise RunValueError(f"{path or '/'}: {message}")


def _runtime_type(atom: Any) -> Any:
    if isinstance(atom, typing.NewType):
        return _runtime_type(atom.__supertype__)
    if isinstance(atom, ListOf):
        return list
    origin = typing.get_origin(atom)
    if origin in (types.UnionType, typing.Union):
        return tuple(_runtime_type(member) for member in typing.get_args(atom))
    return origin or atom


def _select(value: Any, ty: Ty, path: str) -> Any:
    if len(ty.alts) == 1:
        return next(iter(ty.alts))
    exact = [a for a in ty.alts if _runtime_type(a) is type(value)]
    candidates = exact or [a for a in ty.alts if isinstance(value, _runtime_type(a))]
    if len(candidates) == 1:
        return candidates[0]
    # Two container branches may have identical Python types but distinct element types.
    matches = []
    for atom in candidates:
        try:
            _Codec(reading=False).atom(value, atom, path)
        except RunValueError:
            continue
        matches.append(atom)
    _require(len(matches) == 1, path, f"expected exactly one variant of {ty}, got {type(value).__name__}")
    return matches[0]


def _tag(atom: Any) -> str:
    return Ty.of(atom).text(full=True)


def _declared(tag: Any, ty: Ty, path: str) -> Any:
    for atom in ty.alts:
        if _tag(atom) == tag:
            return atom
    raise RunValueError(f"{path or '/'}: stored type {tag!r} is outside {ty}")


def selected_type(value: Any, ty: Ty) -> Ty:
    """Choose the declared variant, retaining element types even for empty lists."""
    atom = _select(value, ty, "")
    _require(isinstance(value, _runtime_type(atom)), "", f"expected {ty}, got {type(value).__name__}")
    return Ty.of(atom)


def encode(value: Any, ty: Ty) -> dict[str, Any]:
    try:
        codec = _Codec(reading=False)
        atom = _select(value, ty, "")
        raw = codec.atom(value, atom, "")
        # Reject invalid Unicode, non-finite numbers and out-of-range JSON integers before admission.
        _ENCODER.encode(raw).encode("utf-8")
        result = {"type": _tag(atom), "value": raw}
        if codec.tags:
            result["types"] = codec.tags
        return result
    except RunValueError:
        raise
    except (ValueError, TypeError, RecursionError, OverflowError, AttributeError) as exc:
        raise RunValueError(f"cannot encode {ty}: {exc}") from exc


def decode(data: Any, ty: Ty) -> Any:
    return _read(data, ty, order_sets=False)


def canonical_input(data: Any, ty: Ty) -> Any:
    """Order only the raw set arrays from a pre-canonical envelope. Keep all other wire values intact."""
    data = copy.deepcopy(data)
    _read(data, ty, order_sets=True)
    return data


def _read(data: Any, ty: Ty, *, order_sets: bool) -> Any:
    try:
        _require(
            isinstance(data, dict) and set(data) in ({"type", "value"}, {"type", "value", "types"}),
            "",
            "invalid value envelope",
        )
        tags = data.get("types", {})
        _require(isinstance(tags, dict), "", "invalid variant map")
        codec = _Codec(reading=True, tags=dict(tags), legacy_tags=tags if order_sets else None)
        value = codec.atom(data["value"], _declared(data["type"], ty, ""), "")
        _require(not codec.tags, "", "unexpected nested variant tags")
        return value
    except RunValueError:
        raise
    except (ValueError, TypeError, RecursionError, OverflowError, AttributeError) as exc:
        raise RunValueError(f"cannot decode {ty}: {exc}") from exc


class _Codec:
    def __init__(
        self, *, reading: bool, tags: dict[str, str] | None = None, legacy_tags: dict[str, str] | None = None
    ) -> None:
        self.reading = reading
        self.tags = {} if tags is None else tags
        self.legacy_tags = legacy_tags
        self.fields = _DC_FIELDS

    def value(self, value: Any, ty: Ty, path: str) -> Any:
        if len(ty.alts) == 1:
            atom = next(iter(ty.alts))
        elif self.reading:
            atom = _declared(self.tags.pop(path, None), ty, path)
        else:
            atom = _select(value, ty, path)
            self.tags[path] = _tag(atom)
        return self.atom(value, atom, path)

    def atom(self, value: Any, atom: Any, path: str) -> Any:
        if atom is str:
            t = type(value)
            if t is str:
                return value
            _require(isinstance(value, str), path, f"expected str, got {t.__name__}")
            return str.__str__(value)
        if atom is int:
            t = type(value)
            if t is int or t is bool:
                return value
            _require(isinstance(value, int), path, f"expected int, got {t.__name__}")
            return int(value)
        if atom is bool:
            if type(value) is bool:
                return value
            raise RunValueError(f"{path or '/'}: expected bool, got {type(value).__name__}")
        if atom is float:
            t = type(value)
            if t is float and math.isfinite(value):
                return value
            _require(isinstance(value, float), path, f"expected float, got {t.__name__}")
            return _plain(float(value), path)
        if atom is NoneType:
            if value is None:
                return None
            raise RunValueError(f"{path or '/'}: expected None, got {type(value).__name__}")
        if isinstance(atom, ListOf):
            _require(isinstance(value, list), path, "expected a list")
            return [self.value(item, atom.item, _child(path, i)) for i, item in enumerate(value)]
        if isinstance(atom, typing.NewType):
            return self.value(value, normalize(atom.__supertype__), _child(path, "@supertype"))
        if type(atom) is not type:
            origin = typing.get_origin(atom)
            if origin is dict:
                return self.mapping(value, typing.get_args(atom), path)
            if origin in (tuple, set, frozenset):
                return self.sequence(value, origin, typing.get_args(atom), path)
        if dataclasses.is_dataclass(atom):
            return self.record(value, atom, path)
        if issubclass(atom, enum.Flag):
            if self.reading:
                _require(type(value) is int, path, f"expected {atom.__name__} bits")
                return atom(value)
            _require(isinstance(value, atom), path, f"expected {atom.__name__}")
            return value.value
        if issubclass(atom, enum.Enum):
            if self.reading:
                _require(isinstance(value, str) and value in atom.__members__, path, f"unknown {atom.__name__} member")
                return atom[value]
            _require(isinstance(value, atom), path, f"expected {atom.__name__}")
            return value.name
        if hasattr(atom, "model_validate_json"):
            if self.reading:
                model = _read_model(atom, value)
                if self.legacy_tags is not None:
                    ordered = _order_model_sets(model, value, by_alias=not _validates_by_name(atom), stored=True)
                    if isinstance(value, dict):
                        value.update(ordered)
                    elif isinstance(value, list):
                        value[:] = ordered
                return model
            _require(isinstance(value, atom), path, f"expected {atom.__name__}")
            by_alias = not _validates_by_name(atom)
            options = {
                "mode": "json",
                "round_trip": True,
                "by_alias": by_alias,
                "warnings": "error",
            }
            if type(value) is atom:
                raw = value.model_dump(**options)
            else:
                from pydantic import TypeAdapter

                raw = TypeAdapter[Any](atom).dump_python(
                    value, mode="json", round_trip=True, by_alias=by_alias, warnings="error"
                )
            raw = _order_model_sets(value, raw, by_alias=by_alias)
            _read_model(atom, raw)
            return raw
        _require(isinstance(value, atom), path, f"expected {_tag(atom)}, got {type(value).__name__}")
        if type(value) is not atom:
            value = atom(value)
        return _plain(value, path)

    def record(self, value: Any, cls: Any, path: str) -> Any:
        if cls not in self.fields:
            hints = typing.get_type_hints(cls)
            self.fields[cls] = {f.name: normalize(hints[f.name]) for f in dataclasses.fields(cls)}
        fields = self.fields[cls]
        if self.reading:
            _require(
                isinstance(value, dict) and value.keys() == fields.keys(), path, f"fields do not match {cls.__name__}"
            )
        else:
            _require(isinstance(value, cls), path, f"expected {cls.__name__}")
        result = {
            name: self.value(value[name] if self.reading else getattr(value, name), ty, _child(path, name))
            for name, ty in fields.items()
        }
        if not self.reading:
            return result
        # Restore the recorded fields, including init=False fields on frozen/slotted records.
        record = object.__new__(cls)
        for name, field_value in result.items():
            object.__setattr__(record, name, field_value)
        return record

    def sequence(self, value: Any, origin: type, args: tuple[Any, ...], path: str) -> Any:
        _require(isinstance(value, list if self.reading else origin), path, f"expected {origin.__name__}")
        if not self.reading and origin in (set, frozenset):
            return self.unordered(value, normalize(args[0]), path)
        fixed_tuple = origin is tuple and (not args or args[-1] is not Ellipsis)
        if fixed_tuple:
            _require(len(args) == len(value), path, "wrong tuple length")
        result = [
            self.value(item, normalize(args[i] if fixed_tuple else args[0]), _child(path, i))
            for i, item in enumerate(value)
        ]
        if self.reading:
            restored = origin(result)
            _require(len(restored) == len(result), path, "duplicate set member")
            if self.legacy_tags is not None and origin in (set, frozenset):
                # Nested sets were ordered while reading each element. Move the
                # original member-local tags with it, without reserializing its contents.
                entries: list[tuple[Any, dict[str, str]]] = [(raw, {}) for raw in value]
                prefix = path + "/"
                for key in tuple(self.legacy_tags):
                    if key.startswith(prefix):
                        index, sep, suffix = key[len(prefix) :].partition("/")
                        if not index.isdecimal() or int(index) >= len(entries):
                            raise RunValueError(f"{path}: invalid set member tag {key!r}")
                        entries[int(index)][1][sep + suffix] = self.legacy_tags.pop(key)
                value[:] = _ordered_members(entries, self.legacy_tags, path)
            return restored
        return result

    def unordered(self, value: Any, item_type: Ty, path: str) -> list[Any]:
        # First encode each member in isolation. Union tags are relative to the
        # member until sorting assigns its stable index in the stored array.
        member = _Codec(reading=False)
        member.fields = self.fields
        entries = []
        for item in value:
            member.tags = {}
            raw = member.value(item, item_type, "")
            entries.append((raw, member.tags))
        return _ordered_members(entries, self.tags, path)

    def mapping(self, value: Any, args: tuple[Any, ...], path: str) -> Any:
        key_type, item_type = args
        if key_type is str:
            _require(isinstance(value, dict) and all(isinstance(k, str) for k in value), path, "expected string keys")
            result = {}
            for k, v in value.items():
                key = str.__str__(k)
                result[key] = self.value(v, normalize(item_type), _child(path, key))
            return result
        # Pairs preserve keys such as 42 versus "42" across a JSON round-trip.
        _require(isinstance(value, list if self.reading else dict), path, "expected key/value pairs")
        pairs = []
        for i, pair in enumerate(value if self.reading else value.items()):
            at = _child(path, i)
            _require(isinstance(pair, (list, tuple)) and len(pair) == 2, at, "expected [key, value]")
            pairs.append(
                [
                    self.value(pair[0], normalize(key_type), _child(at, 0)),
                    self.value(pair[1], normalize(item_type), _child(at, 1)),
                ]
            )
        if self.reading:
            mapping = dict(pairs)
            _require(len(mapping) == len(pairs), path, "duplicate mapping key")
            return mapping
        return pairs


def _ordered_members(entries: list[tuple[Any, dict[str, str]]], tags: dict[str, str], path: str) -> list[Any]:
    entries.sort(key=_order_key)
    result = []
    for i, (raw, relative_tags) in enumerate(entries):
        result.append(raw)
        prefix = _child(path, i)
        tags.update({prefix + suffix: tag for suffix, tag in relative_tags.items()})
    return result


def _order_model_sets(source: Any, raw: Any, *, by_alias: bool, stored: bool = False, annotation: Any = None) -> Any:
    """Pydantic serializes both sets and ordered sequences as arrays. Only reorder the former."""

    def visit(value: Any, item: Any, ty: Any = None) -> Any:
        return _order_model_sets(value, item, by_alias=by_alias, stored=stored, annotation=ty)

    cls = type(source)
    serializers = getattr(cls, "__pydantic_decorators__", None)
    if serializers is not None and serializers.model_serializers:
        # A custom serializer owns its wire shape, including intentional array order.
        return raw
    fields = getattr(cls, "model_fields", {})
    if getattr(cls, "__pydantic_root_model__", False):
        return visit(source.root, raw, fields["root"].annotation)
    args = typing.get_args(annotation)
    if typing.get_origin(annotation) in (types.UnionType, typing.Union):
        variants = [v for v in args if typing.get_origin(v) is cls or v is cls]
        annotation = variants[0] if len(variants) == 1 else None
        args = typing.get_args(annotation)
    if isinstance(raw, list) and isinstance(source, (list, tuple, set, frozenset)) and len(raw) == len(source):
        unordered = isinstance(source, (set, frozenset))
        values = source
        if stored and unordered and any(isinstance(v, (dict, list)) for v in raw):
            # Decoding a set lost the old index-to-object association. Recover each
            # member from its own position before examining its nested collections.
            if not args:
                return sorted(raw, key=_order_key)
            from pydantic import TypeAdapter

            adapter: TypeAdapter[Any] = TypeAdapter(args[0])
            kwargs: dict[str, Any] = (
                {"by_name": True} if "by_name" in inspect.signature(adapter.validate_json).parameters else {}
            )
            values = [adapter.validate_json(_json(item), strict=True, **kwargs) for item in raw]
        fixed = isinstance(source, tuple) and args and args[-1] is not Ellipsis
        children = [
            visit(v, item, args[i] if fixed and i < len(args) else args[0] if args else None)
            for i, (v, item) in enumerate(zip(values, raw))
        ]
        return sorted(children, key=_order_key) if unordered else children
    if isinstance(raw, dict):
        if isinstance(source, dict) and len(raw) == len(source):
            # Pydantic's JSON mode stringifies keys, retaining insertion order.
            return {k: visit(v, raw[k], args[-1] if args else None) for k, v in zip(raw, source.values())}
        if serializers is not None:
            raw = raw.copy()
            custom = {name for serializer in serializers.field_serializers.values() for name in serializer.info.fields}
            for name, info in fields.items():
                key = (info.serialization_alias or info.alias or name) if by_alias else name
                if key in raw and name not in custom and "*" not in custom:
                    raw[key] = visit(getattr(source, name), raw[key], info.annotation)
            for key, extra in (getattr(source, "model_extra", None) or {}).items():
                if key in raw:
                    raw[key] = visit(extra, raw[key])
        elif dataclasses.is_dataclass(source):
            raw = raw.copy()
            hints = typing.get_type_hints(cls)
            for field in dataclasses.fields(source):
                if field.name in raw:
                    raw[field.name] = visit(getattr(source, field.name), raw[field.name], hints.get(field.name))
    return raw


def _validates_by_name(cls: Any) -> bool:
    # Pydantic 2.11 added by_name. Earlier releases validate aliases by default.
    return "by_name" in inspect.signature(cls.model_validate_json).parameters


def _read_model(cls: Any, raw: Any) -> Any:
    options = {"by_name": True} if _validates_by_name(cls) else {}
    return cls.model_validate_json(_json(raw), strict=True, **options)


def _plain(value: Any, path: str) -> Any:
    if value is None or type(value) in (bool, int, str):
        return value
    if type(value) is float and math.isfinite(value):
        return value
    if type(value) is list:
        return [_plain(item, _child(path, i)) for i, item in enumerate(value)]
    if type(value) is dict and all(type(k) is str for k in value):
        return {k: _plain(v, _child(path, k)) for k, v in value.items()}
    raise RunValueError(f"{path or '/'}: expected a JSON value")


def _placeholder(value: Any) -> tuple[Any, int]:
    t = type(value)
    if t is dict:
        return {}, 2
    if t is list:
        return [], 2
    if t is str:
        return "", 2
    if value is None:
        return None, 4
    if t is bool:
        return value, (4 if value else 5)
    size = _size(value)
    return (value, size) if size <= 4 else (None, 4)


def _shares(needs: list[int], available: int) -> list[int]:
    """Give short siblings their full allocation, then split the rest fairly."""
    grants = [0] * len(needs)
    ordered = sorted(range(len(needs)), key=needs.__getitem__)
    for offset, i in enumerate(ordered):
        count = len(ordered) - offset
        share, extra = divmod(available, count)
        if needs[i] > share:
            for j, sibling in enumerate(sorted(ordered[offset:])):
                grants[sibling] = share + int(j < extra)
            break
        grants[i] = needs[i]
        available -= needs[i]
    return grants


def _key_budget(value: Any) -> int:
    """Additional bytes needed to show an object's immediate keys."""
    if type(value) is not dict:
        return 0
    return sum(_str_size(k) + 1 + _placeholder(v)[1] for k, v in value.items()) + max(0, len(value) - 1)


def _prefix(value: str, budget: int) -> str:
    if budget <= 2:
        return ""
    if _PLAIN_ASCII_RE.match(value):
        return value[: budget - 2]
    low, high = 0, min(len(value), budget - 2)
    while low < high:
        middle = (low + high + 1) // 2
        if _str_size(value[:middle]) <= budget:
            low = middle
        else:
            high = middle - 1
    return value[:low]


def _compute_sizes(root: Any) -> dict[int, int]:
    """Memoize exact compact UTF-8 JSON byte sizes for every container/string subtree once."""
    memo: dict[int, int] = {}

    def measure(node: Any) -> int:
        t = type(node)
        if t is str:
            key = id(node)
            if key not in memo:
                memo[key] = _str_size(node)
            return memo[key]
        if node is None:
            return 4
        if t is bool:
            return 4 if node else 5
        if t is int:
            return len(str(node))
        if t is list:
            key = id(node)
            if key not in memo:
                memo[key] = 2 + sum(measure(item) for item in node) + max(0, len(node) - 1)
            return memo[key]
        if t is dict:
            key = id(node)
            if key not in memo:
                memo[key] = 2 + sum(_str_size(k) + 1 + measure(v) for k, v in node.items()) + max(0, len(node) - 1)
            return memo[key]
        return len(_ENCODER.encode(node).encode("utf-8"))

    measure(root)
    return memo


def preview(data: Any, limit: int = 16384) -> dict[str, Any]:
    """Inspect an encoded value, capping only the compact UTF-8 JSON of its plain value."""
    if type(limit) is not int or limit < 1:
        raise ValueError("the preview budget must be a positive integer")
    value = data["value"]
    sizes = _compute_sizes(value)

    def size_of(node: Any) -> int:
        cached = sizes.get(id(node))
        if cached is not None:
            return cached
        return _size(node)

    total = size_of(value)
    result = {"value": value, "truncated": total > limit, "original_bytes": total, "omitted_keys": 0}
    if total <= limit:
        return result

    # A queue grows every sibling's visible shape before descending another level.
    pending: deque[tuple[Any, Any, Any, int]] = deque([(value, result, "value", limit)])
    while pending:
        source, parent, index, budget = pending.popleft()
        src_size = size_of(source)
        if src_size <= budget:
            parent[index] = source
        elif type(source) is str and budget >= 2:
            parent[index] = _prefix(source, budget)
        elif type(source) in (list, dict) and budget >= 2:
            obj = type(source) is dict
            target: Any = {} if obj else []
            parent[index] = target
            remaining = budget - 2
            children = []
            for key, child in source.items() if obj else enumerate(source):
                placeholder, base = _placeholder(child)
                cost = base + int(bool(target)) + (_str_size(key) + 1 if isinstance(key, str) else 0)
                if cost > remaining:
                    if obj:
                        if parent is result:
                            result["omitted_keys"] += 1
                        continue
                    break  # Lists retain a prefix with the original indexes.
                if obj:
                    target[key] = placeholder
                else:
                    target.append(placeholder)
                remaining -= cost
                need = size_of(child) - base
                if need > 0:
                    children.append((child, key, base, need))
            floors = _shares([_key_budget(c) for c, _, _, _ in children], remaining)
            extra = _shares([need - floor for (_, _, _, need), floor in zip(children, floors)], remaining - sum(floors))
            for (child, key, base, _), floor, bonus in zip(children, floors, extra):
                if floor + bonus:
                    pending.append((child, target, key, base + floor + bonus))
        else:
            parent[index] = None if budget >= 4 else 0
            if parent is result and isinstance(source, dict):
                result["omitted_keys"] = len(source)
    return result
