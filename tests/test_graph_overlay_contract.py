from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GRAPH_OVERLAY = (
    ROOT
    / "integration/zingg-0.7.0-overlay/spark/core/src/main/java/zingg/spark/core/util/SparkGraphUtil.java"
)


def test_native_graph_overlay_uses_configured_iteration_limit():
    graph = GRAPH_OVERLAY.read_text()
    assert 'Integer.getInteger("zingg.native.graph.maxIterations", 128)' in graph
    assert "ColName.CLUSTER_COLUMN,\n                    maxIterations);" in graph
    assert "ColName.CLUSTER_COLUMN,\n                    128);" not in graph
