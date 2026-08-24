#!/usr/bin/env python3
"""Fail if the separately supplied Zingg assembly embeds the native core.

The Serverless job supplies the native core as its own dependency.  Embedding
the same classes in the Zingg assembly creates class-loader ambiguity and can
silently run a stale implementation.
"""
from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("jar", type=Path)
    args = parser.parse_args()
    if not args.jar.is_file():
        raise SystemExit(f"assembly does not exist: {args.jar}")
    with zipfile.ZipFile(args.jar) as archive:
        embedded = sorted(
            name for name in archive.namelist() if name.startswith("ai/zingg/native/")
        )
    if embedded:
        print(
            f"{args.jar}: embeds {len(embedded)} ai/zingg/native entries; "
            "deploy the native core separately",
            file=sys.stderr,
        )
        raise SystemExit(1)
    print(f"Zingg assembly packaging boundary: clean ({args.jar})")


if __name__ == "__main__":
    main()
