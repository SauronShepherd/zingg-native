import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GRAPH = ROOT / "core/src/main/scala/ai/zingg/native/NativeGraph.scala"


def _same_assignments_body() -> str:
    source = GRAPH.read_text(encoding="utf-8")
    match = re.search(
        r"private def sameAssignments\(left: DataFrame, right: DataFrame\): Boolean = \{(?P<body>.*?)\n  \}",
        source,
        flags=re.DOTALL,
    )
    assert match is not None, "NativeGraph must keep an explicit assignment-equivalence check"
    return match.group("body")


def test_graph_convergence_compares_assignments_bidirectionally() -> None:
    body = _same_assignments_body()

    assert "leftAssignments.except(rightAssignments).isEmpty" in body
    assert "rightAssignments.except(leftAssignments).isEmpty" in body


def test_graph_convergence_never_uses_an_aggregate_fingerprint() -> None:
    source = GRAPH.read_text(encoding="utf-8")
    body = _same_assignments_body()

    assert "sum(" not in body
    assert "minNbrSum" not in source
    assert "previousSum" not in source
    assert "currentSum" not in source
