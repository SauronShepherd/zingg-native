from pathlib import Path


def test_experimental_trainer_is_explicitly_outside_safe_phase_contract():
    root = Path(__file__).parents[1]
    gateway = (root / "core/src/main/scala/ai/zingg/native/gateway/ClassicGateway.scala").read_text()
    facade = (root / "python/src/zingg_native/zingg.py").read_text()
    assert "fitExperimentalModel" in gateway
    assert "fit_experimental_model" in facade
    assert "non-SAFE" in facade
