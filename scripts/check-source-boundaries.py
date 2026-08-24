#!/usr/bin/env python3
"""Static release guard for production source boundaries.

Intended for later CI; this implementation run does not execute it.
"""
from __future__ import annotations
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
PRODUCTION=[ROOT/'core/src/main', ROOT/'serverless-launcher/src/main']
FORBIDDEN={
  'org.apache.spark.sql.catalyst':'Catalyst internals',
  'org.apache.spark.sql.connect.plugin':'custom Connect server plugin',
  'org.apache.spark.SparkContext':'SparkContext',
  'org.apache.spark.api.java.JavaSparkContext':'JavaSparkContext',
  'org.graphframes':'GraphFrames compile dependency',
}
FORBIDDEN_PATTERNS={
  '.cache(':'DataFrame cache API',
  '.persist(':'DataFrame persist API',
  '.checkpoint(':'DataFrame checkpoint API',
}

def main()->None:
    failures=[]
    for base in PRODUCTION:
        if not base.exists():
            failures.append(f'{base.relative_to(ROOT)}: required production source tree is missing')
            continue
        for path in base.rglob('*'):
            if not path.is_file() or path.suffix not in {'.scala','.java','.py'}: continue
            text=path.read_text(errors='replace')
            for token,reason in FORBIDDEN.items():
                if token in text: failures.append(f'{path.relative_to(ROOT)}: {reason}: {token}')
            for token,reason in FORBIDDEN_PATTERNS.items():
                if token in text: failures.append(f'{path.relative_to(ROOT)}: {reason}: {token}')
    if failures:
        print('\n'.join(failures),file=sys.stderr); raise SystemExit(1)
    print('production source boundaries: clean')

if __name__=='__main__': main()
