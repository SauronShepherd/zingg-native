from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check-oracle-coverage.py"
CONTRACT = ROOT / "core/src/test/resources/oracle-coverage.json"

spec = importlib.util.spec_from_file_location("check_oracle_coverage", SCRIPT)
assert spec is not None and spec.loader is not None
validator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validator)


def _contract() -> dict[str, object]:
    return json.loads(CONTRACT.read_text())


def test_pending_oracle_rule_count_is_ratchet() -> None:
    contract = _contract()
    current = validator._pending_rule_count(contract)
    assert validator.PENDING_RULE_BASELINE == current

    broken = copy.deepcopy(contract)
    pending = broken["pending"]
    assert isinstance(pending, list)
    pending.append("similarity.NewUncoveredRule")

    assert validator._ratchet_errors(broken) == [
        "pending oracle rule count grew from the ratchet baseline "
        f"{validator.PENDING_RULE_BASELINE} to {current + 1}"
    ]


def test_pending_oracle_rule_ratchet_allows_backlog_reduction() -> None:
    contract = _contract()
    pending = contract["pending"]
    assert isinstance(pending, list)
    contract["pending"] = pending[:-1]

    assert validator._ratchet_errors(contract) == []
