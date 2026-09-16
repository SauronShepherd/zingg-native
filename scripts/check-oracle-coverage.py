#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
ARCHITECTURE = ROOT / "core/src/main/scala/ai/zingg/native/RewriteArchitecture.scala"
CONTRACT = ROOT / "core/src/test/resources/oracle-coverage.json"
# Build Plan v5 Z1.10: the reviewed oracle contract currently has 66 pending
# rules. Any change to that count must update the baseline in the same review so
# reductions are recorded and cannot silently regress later.
PENDING_RULE_BASELINE = 66


def _extract_registry_names(source: str, name: str, next_name: str) -> list[str]:
    pattern = re.compile(
        rf"private\s+val\s+{re.escape(name)}\s*=\s*Seq\((.*?)\)\s*\n\s*(?:private\s+)?val\s+{re.escape(next_name)}",
        re.DOTALL,
    )
    match = pattern.search(source)
    if not match:
        raise SystemExit(f"could not locate {name} in {ARCHITECTURE}")
    return re.findall(r'"([A-Za-z0-9]+)"', match.group(1))


def _pending_rule_count(contract: dict[str, Any]) -> int:
    pending = contract.get("pending", [])
    if not isinstance(pending, list):
        raise ValueError("oracle coverage contract must contain list 'pending'")
    return len(set(pending))


def _ratchet_errors(contract: dict[str, Any]) -> list[str]:
    try:
        pending_count = _pending_rule_count(contract)
    except ValueError as exc:
        return [str(exc)]
    if pending_count > PENDING_RULE_BASELINE:
        return [
            "pending oracle rule count grew from the ratchet baseline "
            f"{PENDING_RULE_BASELINE} to {pending_count}"
        ]
    if pending_count < PENDING_RULE_BASELINE:
        return [
            "pending oracle ratchet baseline is stale: lower it from "
            f"{PENDING_RULE_BASELINE} to {pending_count}"
        ]
    return []


def main() -> int:
    source = ARCHITECTURE.read_text()
    contract = json.loads(CONTRACT.read_text())

    similarities = _extract_registry_names(source, "similarityNames", "hashNames")
    hashes = _extract_registry_names(source, "hashNames", "preprocessNames")
    live = {f"similarity.{name}" for name in similarities} | {f"blocking.{name}" for name in hashes}

    covered = contract.get("covered", {})
    pending = contract.get("pending", [])
    if not isinstance(covered, dict) or not isinstance(pending, list):
        raise SystemExit("oracle coverage contract must contain object 'covered' and list 'pending'")

    covered_ids = set(covered)
    pending_ids = set(pending)
    errors: list[str] = _ratchet_errors(contract)

    if len(pending_ids) != len(pending):
        errors.append("pending coverage entries contain duplicates")
    overlap = covered_ids & pending_ids
    if overlap:
        errors.append(f"rules cannot be both covered and pending: {sorted(overlap)}")

    classified = covered_ids | pending_ids
    missing = live - classified
    stale = classified - live
    if missing:
        errors.append(f"live rules missing from oracle contract: {sorted(missing)}")
    if stale:
        errors.append(f"oracle contract contains stale rules: {sorted(stale)}")

    for operation_id, evidence in sorted(covered.items()):
        if not isinstance(evidence, dict):
            errors.append(f"covered rule {operation_id} must map to an evidence object")
            continue
        vectors = evidence.get("vectors")
        property_name = evidence.get("property")
        test_path = evidence.get("test")
        oracle = evidence.get("oracle")
        if not isinstance(vectors, int) or vectors < 5:
            errors.append(f"covered rule {operation_id} must declare at least 5 vectors")
        if not isinstance(property_name, str) or not property_name.strip():
            errors.append(f"covered rule {operation_id} must declare a property")
        if not isinstance(oracle, str) or not oracle.strip():
            errors.append(f"covered rule {operation_id} must declare its oracle")
        if not isinstance(test_path, str) or not test_path.strip() or not (ROOT / test_path).is_file():
            errors.append(f"covered rule {operation_id} must point to an existing test file")

    if pending != sorted(pending):
        errors.append("pending coverage entries must stay sorted for reviewable diffs")

    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    print(
        "oracle coverage contract: "
        f"live={len(live)} covered={len(covered_ids)} pending={len(pending_ids)} "
        f"baseline={PENDING_RULE_BASELINE}"
    )
    if pending_ids:
        print("Z1.8 remains partial until pending reaches zero.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
