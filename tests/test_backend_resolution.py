import pytest

from zingg_native.backend.base import PrototypeExpressionBackend, resolve_backend
from zingg_native.errors import BackendUnavailableError


class FakeSpark:
    class _JVM:
        pass
    _jvm = _JVM()


def test_prototype_requires_explicit_opt_in():
    assert isinstance(resolve_backend(FakeSpark(), "expressions"), PrototypeExpressionBackend)


def test_classic_requires_loaded_core():
    with pytest.raises(BackendUnavailableError):
        resolve_backend(FakeSpark(), "classic")
