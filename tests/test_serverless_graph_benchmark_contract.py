from __future__ import annotations

from pathlib import Path


def test_serverless_graph_benchmark_is_real_and_bounded() -> None:
    root = Path(__file__).resolve().parents[1]
    source = (
        root
        / "serverless-launcher"
        / "src"
        / "main"
        / "scala"
        / "ai"
        / "zingg"
        / "native"
        / "launch"
        / "UnavailableServerlessDiagnostics.scala"
    ).read_text(encoding="utf-8")

    assert "--native-graph-benchmark is not available" not in source
    assert "NATIVE_GRAPH_BENCHMARK_CASE" in source
    assert "NATIVE_GRAPH_BENCHMARK_PASS" in source
    assert "NATIVE_GRAPH_BENCHMARK_JSON" in source
    assert '\\"schemaVersion\\":1' in source
    assert '\\"kind\\":\\"case\\"' in source
    assert '\\"kind\\":\\"summary\\"' in source
    assert '\\"thresholds\\":null' in source
    assert 'Scenario("chain"' in source
    assert 'Scenario("star"' in source
    assert "Seq(16, 64)" in source
    assert "thresholds=none" in source
