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
