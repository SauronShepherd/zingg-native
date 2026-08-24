#!/usr/bin/env python3
"""Merge validation evidence into the static capability manifest.

This script does not infer support. It only promotes an operation/target from
"unvalidated" when explicit evidence documents are supplied by later test/E2E
pipelines. The implementation archive intentionally ships with no such evidence.
"""
from __future__ import annotations
import argparse, json
from pathlib import Path


def main() -> None:
    p=argparse.ArgumentParser()
    p.add_argument("--base",type=Path,default=Path("core/src/main/resources/zingg-native-capabilities.json"))
    p.add_argument("--evidence",type=Path,action="append",default=[])
    p.add_argument("--output",type=Path,required=True)
    args=p.parse_args()
    manifest=json.loads(args.base.read_text())
    for path in args.evidence:
        evidence=json.loads(path.read_text())
        operation=evidence.get("operation")
        target=evidence.get("target")
        if not operation or operation not in manifest["operations"]:
            raise SystemExit(f"unknown/missing operation in {path}: {operation!r}")
        if target not in {"dedicatedPhoton","serverless","semanticParity"}:
            raise SystemExit(f"unknown/missing target in {path}: {target!r}")
        if evidence.get("status") != "verified":
            continue
        manifest["operations"][operation][target]="verified"
        if evidence.get("evidenceId"):
            manifest["operations"][operation].setdefault("photonEvidence",[]).append(evidence["evidenceId"])
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(manifest,indent=2)+"\n")

if __name__=="__main__": main()
