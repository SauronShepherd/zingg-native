#!/usr/bin/env python3
"""Static artifact-shape verifier for later build/release pipelines.

It deliberately does not claim semantic correctness or Photon execution.
"""
from __future__ import annotations
import argparse, json, zipfile
from pathlib import Path

CORE_REQUIRED={
 'ai/zingg/native/Core$.class',
 'ai/zingg/native/NativeOperationProvider.class',
 'ai/zingg/nativebridge/NativeOperationProvider.class',
 'ai/zingg/native/PublicRewriteRules$.class',
 'zingg-native-capabilities.json',
}
LAUNCHER_REQUIRED={'ai/zingg/native/launch/DatabricksZinggMain$.class'}

def names(path:Path)->set[str]:
    with zipfile.ZipFile(path) as z: return set(z.namelist())

def require(path:Path,required:set[str])->None:
    present=names(path); missing=required-present
    if missing: raise SystemExit(f'{path}: missing entries: {sorted(missing)}')

def main()->None:
    p=argparse.ArgumentParser()
    p.add_argument('--core',type=Path,required=True)
    p.add_argument('--serverless-launcher',type=Path)
    args=p.parse_args()
    require(args.core,CORE_REQUIRED)
    if args.serverless_launcher: require(args.serverless_launcher,LAUNCHER_REQUIRED)
    with zipfile.ZipFile(args.core) as z:
        manifest=json.loads(z.read('zingg-native-capabilities.json'))
    if manifest.get('serverlessConnectPluginRequired') is not False:
        raise SystemExit('capability manifest incorrectly requires a custom Connect plugin')
    validation = manifest.get('validation', {})
    if validation.get('testsRunForThisImplementation') is True and not validation.get('validationScope'):
        raise SystemExit('artifact manifest claims validation without a bounded validationScope')
    print('artifact shape: valid; runtime/semantic/Photon validation is separate')

if __name__=='__main__': main()
