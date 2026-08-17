import pytest
from zingg_native import Zingg
from zingg_native.backend.base import PrototypeExpressionBackend, resolve_backend
from zingg_native.errors import BackendUnavailableError, UnsupportedOperationError


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


@pytest.mark.parametrize(
    "method,args",
    [
        ("exact_match", (None, ["key"])),
        ("score_features", (None,)),
        ("train", (None, ["key"])),
        ("match_pairs", (None, {})),
        ("link_pairs", (None, {})),
        ("cluster_pairs", (None,)),
        ("fuzzy_match", (None, {})),
        ("link_sources", (None, None, {})),
        ("match", (None, {})),
        ("link", (None, {})),
        ("generate_docs", ({},)),
        ("execute", ()),
    ],
)
def test_all_unverified_phases_are_guarded(method, args):
    z = object.__new__(Zingg)
    z.backend = type("ClassicBackend", (), {})()
    with pytest.raises(UnsupportedOperationError, match="not certified"):
        getattr(z, method)(*args)


def test_candidate_phase_dispatches_only_to_shared_classic_backend():
    z = object.__new__(Zingg)
    z.backend = type("ConnectBackend", (), {})()
    with pytest.raises(UnsupportedOperationError, match="shared-core phase"):
        z.find_training_data(None, ["key"], "id")


def test_training_evidence_dispatches_only_to_shared_classic_backend():
    z = object.__new__(Zingg)
    z.backend = type("ConnectBackend", (), {})()
    with pytest.raises(UnsupportedOperationError, match="training evidence"):
        z.inspect_training_evidence(None)


def test_classic_phase_supports_persistence_but_rejects_all_pairs_shortcut():
    z = object.__new__(Zingg)
    calls = []
    z.backend = type("ClassicBackend", (), {
        "find_training_data": lambda self, *args: calls.append(args) or "persisted"
    })()
    assert z.find_training_data(None, ["key"], "id", output_path="dbfs:/pairs") == "persisted"
    assert calls[-1][-1] == "dbfs:/pairs"
    with pytest.raises(UnsupportedOperationError, match="include_all_pairs"):
        z.find_training_data(None, ["key"], "id", include_all_pairs=True)
