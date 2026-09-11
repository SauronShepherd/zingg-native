from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_ci_prepares_pinned_reference_before_reference_dependent_checks():
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert workflow.count("./scripts/prepare-reference.ps1") >= 3
    assert workflow.index("./scripts/prepare-reference.ps1") < workflow.index("./scripts/check-upstream-inventory.ps1")


def test_production_sources_do_not_use_old_unmanaged_native_fallback():
    forbidden = "/Volumes/sda_dev/default/zingg_native_e2e_volume/.native-transient/base"
    roots = [
        ROOT / "core/src/main",
        ROOT / "serverless-launcher/src/main",
        ROOT / "integration/zingg-0.7.0-overlay",
        ROOT / "resources",
    ]
    offenders = []
    for root in roots:
        for path in root.rglob("*"):
            if path.is_file() and path.suffix.lower() in {".scala", ".java", ".py", ".yml", ".yaml", ".json", ".xml"}:
                if forbidden in path.read_text(encoding="utf-8", errors="ignore"):
                    offenders.append(str(path.relative_to(ROOT)))
    assert not offenders, offenders


def test_graph_iteration_and_containment_contracts_are_explicit_in_source():
    graph = (ROOT / "core/src/main/scala/ai/zingg/native/NativeGraph.scala").read_text(encoding="utf-8")
    launcher = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text(encoding="utf-8")
    assert 'require(maxIterations > 0, "maxIterations must be positive")' in graph
    assert "did not converge" in graph
    assert 'sys.props.get("zingg.native.graph.materializePath")' in graph
    assert "refusing an unmanaged fallback" in graph
    assert "dbfs:/tmp/zingg-native-graph" not in graph
    assert "explicit graph materialization requires --zinggDir" in launcher
    assert 'val scopedPrefix = scopedRoot.stripSuffix("/") + "/"' in launcher
    assert "configured == scopedRoot || configured.startsWith(scopedPrefix)" in launcher


def test_fuzzy_probe_uses_only_scoped_run_root_for_materialization():
    probe = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/ServerlessFuzzyActionProbe.scala").read_text(encoding="utf-8")
    assert 'sys.props.get("zingg.native.materialization.runRoot")' in probe
    assert "refusing an unmanaged fallback path" in probe
    assert "/Volumes/sda_dev/default/zingg_native_e2e_volume/probes/" not in probe


def test_closeout_documentation_covers_required_release_contracts():
    doc = (ROOT / "docs/closeout-contracts.md").read_text(encoding="utf-8")
    required = [
        "Databricks Dedicated with Photon",
        "Databricks Serverless",
        "--zinggDir",
        "Symbolic-link roots",
        "Prefix lookalikes",
        "fail-closed",
        "STRICT",
        "REWRITE",
        "pinned Zingg 0.7.0",
        "JaroWinklerFunction",
        "Python 3.10 and 3.12",
        "Java 17",
    ]
    for phrase in required:
        assert phrase in doc
