from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class ContractChange:
    kind: str
    field: str
    detail: str


@dataclass(frozen=True)
class ContractDiff:
    breaking: tuple[ContractChange, ...]
    additive: tuple[ContractChange, ...]
    fingerprint: str


def fingerprint(contract: Mapping[str, Mapping[str, object]]) -> str:
    normalized = {
        field: dict(sorted(spec.items())) for field, spec in sorted(contract.items())
    }
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compare_contracts(
    previous: Mapping[str, Mapping[str, object]], current: Mapping[str, Mapping[str, object]]
) -> ContractDiff:
    breaking: list[ContractChange] = []
    additive: list[ContractChange] = []
    for field, old in previous.items():
        if field not in current:
            if bool(old.get("required", False)):
                breaking.append(ContractChange("removed_required", field, "required field removed"))
            else:
                breaking.append(ContractChange("removed", field, "field removed"))
            continue
        new = current[field]
        if old.get("type") != new.get("type"):
            breaking.append(
                ContractChange("type_change", field, f"{old.get('type')} -> {new.get('type')}")
            )
        if bool(old.get("nullable", True)) and not bool(new.get("nullable", True)):
            breaking.append(ContractChange("nullability_tightened", field, "nullable -> non-nullable"))
    for field, spec in current.items():
        if field not in previous:
            additive.append(ContractChange("added", field, "field added"))
        elif not bool(previous[field].get("required", False)) and bool(spec.get("required", False)):
            breaking.append(ContractChange("required_tightened", field, "optional -> required"))
    return ContractDiff(tuple(breaking), tuple(additive), fingerprint(current))
