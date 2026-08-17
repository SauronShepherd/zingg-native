import pytest
from zingg_native import Zingg
from zingg_native.backend.base import PrototypeExpressionBackend, resolve_backend
from zingg_native.errors import BackendUnavailableError, UnsupportedOperationError


class FakeSpark:
    class _JVM:
        pass
    _jvm = _JVM()


def test_status_reports_runtime_and_transport_metadata():
    z = object.__new__(Zingg)
    z.config = type("Config", (), {"protocol_version": "1"})()
    z.backend = type("ClassicBackend", (), {"name": "classic-py4j"})()
    z.runtime = type("Runtime", (), {
        "spark_version": "4.1.0",
        "api_mode": "classic",
        "engine": "spark",
        "native_execution": False,
    })()
    result = z.status()
    assert result == {
        "library_version": "0.2.0-SNAPSHOT",
        "protocol_version": "1",
        "backend": "classic-py4j",
        "spark_version": "4.1.0",
        "api_mode": "classic",
        "engine": "spark",
        "native_execution_observed": False,
    }


def test_prototype_requires_explicit_opt_in():
    assert isinstance(resolve_backend(FakeSpark(), "expressions"), PrototypeExpressionBackend)


def test_classic_requires_loaded_core():
    with pytest.raises(BackendUnavailableError):
        resolve_backend(FakeSpark(), "classic")


def test_classic_boundary_rejects_unknown_operations_before_gateway_call():
    from zingg_native.backend.classic import ClassicBackend

    backend = object.__new__(ClassicBackend)
    with pytest.raises(NotImplementedError, match="operation NOPE"):
        backend.transform(None, "NOPE")
    with pytest.raises(NotImplementedError, match="preprocessing operation NOPE"):
        backend.preprocess(None, "NOPE", ["name"])


def test_classic_handshake_rejects_incompatible_library():
    from zingg_native.backend.classic import ClassicBackend

    class Gateway:
        def protocolVersion(self):
            return "1"

        def libraryVersion(self):
            return "0.1.0"

    class JVM:
        class ai:
            class zingg:
                class native:
                    class gateway:
                        ClassicGateway = Gateway

    class Spark:
        _jvm = JVM()
        version = "4.1.0"

    with pytest.raises(BackendUnavailableError, match="library version"):
        ClassicBackend(Spark())


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
