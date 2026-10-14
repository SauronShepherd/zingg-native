import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GRAPH = ROOT / "core/src/main/scala/ai/zingg/native/NativeGraph.scala"


def _same_assignments_body() -> str:
    source = GRAPH.read_text(encoding="utf-8")
    match = re.search(
        r"private(?:\[native\])? def sameAssignments\(left: DataFrame, right: DataFrame\): Boolean = \{(?P<body>.*?)\n  \}",
        source,
        flags=re.DOTALL,
    )
    assert match is not None, "NativeGraph must keep an explicit assignment-equivalence check"
    return match.group("body")


def test_graph_convergence_compares_assignments_bidirectionally() -> None:
    body = _same_assignments_body()

    assert ".except(rightAssignments)" in body
    assert "rightAssignments.except(leftAssignments)" in body
    assert ".unionByName(" in body
    assert ".limit(1)" in body
    assert ".isEmpty" in body


def test_graph_convergence_never_uses_an_aggregate_fingerprint() -> None:
    source = GRAPH.read_text(encoding="utf-8")
    body = _same_assignments_body()

    assert "sum(" not in body
    assert "minNbrSum" not in source
    assert "previousSum" not in source
    assert "currentSum" not in source
