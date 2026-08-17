import json
from pathlib import Path


def test_serverless_core_job_is_a_reproducible_jar_task():
    root = Path(__file__).parents[1]
    spec = json.loads((root / "databricks-serverless-core-e2e.json").read_text(encoding="utf-8"))
    task = spec["tasks"][0]
    environment = spec["environments"][0]
    assert task["environment_key"] == "serverless"
    assert task["spark_jar_task"]["main_class_name"] == "ai.zingg.serverless.ServerlessCoreE2E"
    assert "job_cluster_key" not in task
    assert environment["spec"]["environment_version"] == "5"
    dependencies = environment["spec"]["java_dependencies"]
    assert any(path.endswith("zingg-native-core_2.13-0.2.0-SNAPSHOT.jar") for path in dependencies)
    assert any(path.endswith("zingg-native-connect_2.13-0.2.0-SNAPSHOT.jar") for path in dependencies)


def test_serverless_evidence_records_success_and_non_claims():
    root = Path(__file__).parents[1]
    evidence = json.loads(
        (root / "docs" / "evidence" / "databricks-serverless.json").read_text(encoding="utf-8")
    )
    assert evidence["sharedCoreJarTask"]["result"] == "SUCCESS"
    assert "JARO_SIMILARITY" in evidence["sharedCoreJarTask"]["verified"]
    assert evidence["managedConnectFeasibility"]["result"] == "FAILED_BEFORE_NATIVE_EXECUTION"
    assert "managed Connect ExpressionPlugin execution" in evidence["notClaimed"]
