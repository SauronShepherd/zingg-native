#!/usr/bin/env python3
"""Reject stale or overclaiming release metadata."""
import json
from pathlib import Path

manifest = json.loads(Path("core/src/main/resources/zingg-native-capabilities.json").read_text())
evidence = json.loads(Path("docs/evidence/databricks-serverless-v5.json").read_text())
serverless = manifest["targets"]["databricksServerless"]
if serverless["latestEvidence"] != "docs/evidence/databricks-serverless-v5.json":
    raise SystemExit("Serverless capability manifest points at the wrong evidence file")
if "full-feature production training" not in serverless["validationScope"]:
    raise SystemExit("Serverless validation scope must identify the completed full-feature production train gate")
if "cross-job persistence" not in serverless["validationScope"]:
    raise SystemExit("Serverless validation scope must identify cross-job persistence evidence")
if evidence["environment"]["environmentVersion"] != "5":
    raise SystemExit("Evidence environment version drifted from the selected release profile")
if evidence["environment"]["zinggVersion"] != "0.7.0":
    raise SystemExit("Evidence Zingg version is not 0.7.0")
print("release metadata consistency passed")
