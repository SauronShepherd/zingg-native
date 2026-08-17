from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_connect_transport_has_no_classic_or_python_algorithm_dependencies():
    source = (ROOT / "python/src/zingg_native/backend/connect.py").read_text()
    for forbidden in ("_jvm", "_jdf", "_gateway", "SparkContext", "..similarity"):
        assert forbidden not in source


def test_classic_is_the_only_transport_that_crosses_py4j_boundary():
    source = (ROOT / "python/src/zingg_native/backend/classic.py").read_text()
    assert "spark._jvm" in source
    assert "df._jdf" in source
