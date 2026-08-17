from pathlib import Path


ROOT = Path(__file__).parents[1]


def test_preprocessing_is_declared_as_shared_classic_operation():
    core = (ROOT / "core/src/main/scala/ai/zingg/native/Core.scala").read_text()
    gateway = (ROOT / "core/src/main/scala/ai/zingg/native/gateway/ClassicGateway.scala").read_text()
    facade = (ROOT / "python/src/zingg_native/zingg.py").read_text()
    assert 'case "TRIM"' in core
    assert 'case "CASE_NORMALIZE"' in core
    assert "def preprocess" in gateway
    assert "def preprocess" in facade
