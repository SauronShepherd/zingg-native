import pytest

from zingg_native.backend.base import PrototypeExpressionBackend, resolve_backend
from zingg_native.errors import BackendUnavailableError
from zingg_native.errors import UnsupportedOperationError
from zingg_native import Zingg


class FakeSpark:
    class _JVM:
        pass
    _jvm = _JVM()


def test_prototype_requires_explicit_opt_in():
    assert isinstance(resolve_backend(FakeSpark(), "expressions"), PrototypeExpressionBackend)


def test_classic_requires_loaded_core():
    with pytest.raises(BackendUnavailableError):
        resolve_backend(FakeSpark(), "classic")


def test_unverified_phases_are_not_exposed_by_safe_transport():
    z = object.__new__(Zingg)
    z.backend = type("ClassicBackend", (), {})()
    with pytest.raises(UnsupportedOperationError):
        z.exact_match(None, ["key"])


def test_candidate_phase_dispatches_only_to_shared_classic_backend():
    z = object.__new__(Zingg)
    z.backend = type("ConnectBackend", (), {})()
    with pytest.raises(UnsupportedOperationError, match="shared-core phase"):
        z.find_training_data(None, ["key"], "id")


def test_classic_phase_rejects_unimplemented_persistence_options():
    z = object.__new__(Zingg)
    z.backend = type("ClassicBackend", (), {"find_training_data": lambda *_: None})()
    with pytest.raises(UnsupportedOperationError, match="output_path"):
        z.find_training_data(None, ["key"], "id", output_path="dbfs:/pairs")
    with pytest.raises(UnsupportedOperationError, match="include_all_pairs"):
        z.find_training_data(None, ["key"], "id", include_all_pairs=True)
