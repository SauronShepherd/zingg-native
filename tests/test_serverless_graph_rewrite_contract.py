from pathlib import Path


def test_serverless_graph_probe_qualifies_rewrite_non_convergence() -> None:
    source = (
        Path(__file__).resolve().parents[1]
        / "serverless-launcher"
        / "src"
        / "main"
        / "scala"
        / "ai"
        / "zingg"
        / "native"
        / "launch"
        / "ServerlessGraphProbe.scala"
    ).read_text(encoding="utf-8")

    assert 'System.setProperty("zingg.native.mode", "REWRITE")' in source
    assert 'NativeOperationProvider.fromSpark(spark, "graph-probe-rewrite")' in source
    assert 'require(rewriteRejected, "REWRITE graph probe must reject an insufficient iteration bound")' in source
    assert "rewriteIterationFailure=true" in source
    assert 'System.clearProperty("zingg.native.mode")' in source
