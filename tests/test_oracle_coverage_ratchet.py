from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


def _checker():
    path = Path(__file__).parents[1] / "scripts" / "check-oracle-coverage.py"
    spec = spec_from_file_location("check_oracle_coverage", path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_pending_oracle_count_is_a_downward_only_ratchet() -> None:
    checker = _checker()

    assert checker._pending_ratchet_error(checker.PENDING_ORACLE_BASELINE) is None
    assert checker._pending_ratchet_error(checker.PENDING_ORACLE_BASELINE - 1) is None
    assert checker._pending_ratchet_error(checker.PENDING_ORACLE_BASELINE + 1) == (
        "pending oracle coverage grew from ratchet baseline 66 to 67"
    )
