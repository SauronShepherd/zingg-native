#!/usr/bin/env python3
"""Inspect a built Serverless application JAR for forbidden packaged/runtime dependencies.

This is a later build/release guard, not proof of Photon execution.
"""
from __future__ import annotations
import argparse, struct, sys, zipfile
from pathlib import Path

FORBIDDEN_ENTRIES=(
 'org/apache/spark/sql/catalyst/',
 'org/apache/spark/sql/connect/plugin/',
 'org/apache/spark/SparkContext.class',
 'org/apache/spark/api/java/JavaSparkContext.class',
 'org/graphframes/',
 'org/apache/spark/rdd/',
 'scala-library-2.12',
)
FORBIDDEN_BYTES=(
 b'org/apache/spark/sql/catalyst',
 b'org/apache/spark/sql/connect/plugin',
 b'org/apache/spark/api/java/JavaSparkContext',
 b'org/graphframes/',
 b'org/apache/spark/rdd/',
 b'org/apache/spark/SparkContext',
 b'_2.12',
)
MAX_CLASS_MAJOR = 61  # Java 17, the Serverless environment-5 runtime.

def main()->None:
    p=argparse.ArgumentParser()
    p.add_argument('jar',type=Path, nargs='+', help='one or more certified application jars')
    args=p.parse_args()
    failures=[]
    for jar in args.jar:
        with zipfile.ZipFile(jar) as zf:
            for name in zf.namelist():
                if any(name.startswith(prefix) for prefix in FORBIDDEN_ENTRIES):
                    failures.append(f'{jar.name}: packaged forbidden class/resource: {name}')
                if name.endswith('.class'):
                    data=zf.read(name)
                    if data[:4] == b'\xca\xfe\xba\xbe' and len(data) >= 8:
                        major = struct.unpack('>H', data[6:8])[0]
                        if major > MAX_CLASS_MAJOR:
                            failures.append(f'{jar.name}:{name}: class-file major {major} exceeds Java 17 ({MAX_CLASS_MAJOR})')
                    for token in FORBIDDEN_BYTES:
                        if token in data: failures.append(f'{jar.name}:{name}: references {token.decode()}')
    if failures:
        print('\n'.join(sorted(set(failures))),file=sys.stderr); raise SystemExit(1)
    print('serverless bytecode boundary: clean')

if __name__=='__main__': main()
